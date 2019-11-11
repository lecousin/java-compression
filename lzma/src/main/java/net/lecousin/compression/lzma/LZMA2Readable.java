package net.lecousin.compression.lzma;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.function.Consumer;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.TaskManager;
import net.lecousin.framework.concurrent.Threading;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.IOUtil;
import net.lecousin.framework.io.buffering.PreBufferedReadable;
import net.lecousin.framework.io.util.DataUtil;
import net.lecousin.framework.memory.ByteArrayCache;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;

public class LZMA2Readable extends ConcurrentCloseable<IOException> implements IO.Readable {

    /**
     * Smallest valid LZMA2 dictionary size.
     * <p>
     * Very tiny dictionaries would be a performance problem, so
     * the minimum is 4 KiB.
     */
    public static final int DICT_SIZE_MIN = 4096;

    /**
     * Largest dictionary size supported by this implementation.
     * <p>
     * The LZMA2 algorithm allows dictionaries up to one byte less than 4 GiB.
     * This implementation supports only 16 bytes less than 2 GiB for raw
     * LZMA2 streams, and for .xz files the maximum is 1.5 GiB. This
     * limitation is due to Java using signed 32-bit integers for array
     * indexing. The limitation shouldn't matter much in practice since so
     * huge dictionaries are not normally used.
     */
    public static final int DICT_SIZE_MAX = Integer.MAX_VALUE & ~15;

    private static final int COMPRESSED_SIZE_MAX = 1 << 16;

    private final ByteArrayCache arrayCache;
    private IO.Readable.Buffered input;

    private LZDecoder lz;
    private RangeDecoderFromBuffer rc;
    private LZMADecoder lzma;

    private int uncompressedSize = 0;
    private boolean isLZMAChunk = false;

    private boolean needDictReset = true;
    private boolean needProps = true;
    private boolean endReached = false;

    /**
     * Gets approximate decompressor memory requirements as kibibytes for
     * the given dictionary size.
     *
     * @param       dictSize    LZMA2 dictionary size as bytes, must be
     *                          in the range [<code>DICT_SIZE_MIN</code>,
     *                          <code>DICT_SIZE_MAX</code>]
     *
     * @return      approximate memory requirements as kibibytes (KiB)
     */
    public static int getMemoryUsage(int dictSize) {
        // The base state is around 30-40 KiB (probabilities etc.),
        // range decoder needs COMPRESSED_SIZE_MAX bytes for buffering,
        // and LZ decoder needs a dictionary buffer.
        return 40 + COMPRESSED_SIZE_MAX / 1024 + getDictSize(dictSize) / 1024;
    }

    private static int getDictSize(int dictSize) {
        if (dictSize < DICT_SIZE_MIN || dictSize > DICT_SIZE_MAX)
            throw new IllegalArgumentException(
                    "Unsupported dictionary size " + dictSize);

        // Round dictionary size upward to a multiple of 16. This way LZMA
        // can use LZDecoder.getPos() for calculating LZMA's posMask.
        // Note that this check is needed only for raw LZMA2 streams; it is
        // redundant with .xz.
        return (dictSize + 15) & ~15;
    }

    /**
     * Creates a new readable that decompresses raw LZMA2 data
     * from <code>input</code>.
     * <p>
     * The caller needs to know the dictionary size used when compressing;
     * the dictionary size isn't stored as part of a raw LZMA2 stream.
     * <p>
     * Specifying a too small dictionary size will prevent decompressing
     * the stream. Specifying a too big dictionary is waste of memory but
     * decompression will work.
     * <p>
     * There is no need to specify a dictionary bigger than
     * the uncompressed size of the data even if a bigger dictionary
     * was used when compressing. If you know the uncompressed size
     * of the data, this might allow saving some memory.
     *
     * @param       input       readable from which LZMA2-compressed
     *                          data is read
     *
     * @param       dictSize    LZMA2 dictionary size as bytes, must be
     *                          in the range [<code>DICT_SIZE_MIN</code>,
     *                          <code>DICT_SIZE_MAX</code>]
     */
    public LZMA2Readable(IO.Readable input, int dictSize) {
        this(input, dictSize, null);
    }

    /**
     * Creates a new LZMA2 decompressor using a preset dictionary.
     * <p>
     * This is like <code>LZMA2Readable(IO.Readable, int)</code> except
     * that the dictionary may be initialized using a preset dictionary.
     * If a preset dictionary was used when compressing the data, the
     * same preset dictionary must be provided when decompressing.
     *
     * @param       input       readable from which LZMA2-compressed
     *                          data is read
     *
     * @param       dictSize    LZMA2 dictionary size as bytes, must be
     *                          in the range [<code>DICT_SIZE_MIN</code>,
     *                          <code>DICT_SIZE_MAX</code>]
     *
     * @param       presetDict  preset dictionary or <code>null</code>
     *                          to use no preset dictionary
     */
    public LZMA2Readable(IO.Readable input, int dictSize, byte[] presetDict) {
        this(input, dictSize, presetDict, null);
    }

    /**
     * Creates a new LZMA2 decompressor using a preset dictionary
     * and array cache.
     * <p>
     * This is like <code>LZMA2Readable(IO.Readable, int, byte[])</code>
     * except that this also takes the <code>arrayCache</code> argument.
     *
     * @param       input       readable from which LZMA2-compressed
     *                          data is read
     *
     * @param       dictSize    LZMA2 dictionary size as bytes, must be
     *                          in the range [<code>DICT_SIZE_MIN</code>,
     *                          <code>DICT_SIZE_MAX</code>]
     *
     * @param       presetDict  preset dictionary or <code>null</code>
     *                          to use no preset dictionary
     *
     * @param       arrayCache  cache to be used for allocating large arrays.
     *                          if not given, the default one is used
     *
     */
    public LZMA2Readable(IO.Readable input, int dictSize, byte[] presetDict, ByteArrayCache arrayCache) {
        // Check for null because otherwise null isn't detect
        // in this constructor.
        if (input == null)
            throw new IllegalArgumentException("input is null");

        if (arrayCache == null) arrayCache = ByteArrayCache.getInstance();
        this.arrayCache = arrayCache;
        if (input instanceof IO.Readable.Buffered)
        	this.input = (IO.Readable.Buffered)input;
        else
        	this.input = new PreBufferedReadable(input, COMPRESSED_SIZE_MAX, input.getPriority(), COMPRESSED_SIZE_MAX, input.getPriority(), 8);
        this.rc = new RangeDecoderFromBuffer(COMPRESSED_SIZE_MAX, arrayCache);
        this.lz = new LZDecoder(getDictSize(dictSize), presetDict, arrayCache);

        if (presetDict != null && presetDict.length > 0)
            needDictReset = false;
    }

    @Override
    public int readSync(ByteBuffer buffer) throws IOException {
    	return readSync(buffer, false);
    }
    
    @Override
    public int readFullySync(ByteBuffer buffer) throws IOException {
    	return readSync(buffer, true);
    }
    
    public int readSync(ByteBuffer buffer, boolean fully) throws IOException {
        if (buffer.remaining() == 0) return 0;
        if (input == null || isClosing()) throw new ClosedChannelException();
        if (endReached) return -1;

        int size = 0;

        while (buffer.hasRemaining()) {
            if (uncompressedSize == 0) {
            	if (!fully && size > 0)
            		return size;
                decodeChunkHeaderSync();
                if (endReached)
                    return size == 0 ? -1 : size;
            }

            int copySizeMax = Math.min(uncompressedSize, buffer.remaining());

            if (!isLZMAChunk) {
                lz.copyUncompressedSync(input, copySizeMax);
            } else {
                lz.setLimit(copySizeMax);
                lzma.decode();
            }

            int copiedSize = lz.flush(buffer);
            size += copiedSize;
            uncompressedSize -= copiedSize;

            if (uncompressedSize == 0 && (!rc.isFinished() || lz.hasPending()))
            	throw new CorruptedInputException();
        }

        return size;
    }

	@Override
	public AsyncSupplier<Integer, IOException> readFullyAsync(ByteBuffer buffer, Consumer<Pair<Integer, IOException>> ondone) {
    	if (!buffer.hasRemaining())
    		return IOUtil.success(Integer.valueOf(0), ondone);
        if (input == null || isClosing())
        	return IOUtil.error(new ClosedChannelException(), ondone);
        if (endReached)
        	return IOUtil.success(Integer.valueOf(-1), ondone);
        
        AsyncSupplier<Integer, IOException> result = new AsyncSupplier<>();
    	TaskUtil.decompressionTask(input, () -> readAsync(buffer, result, 0, true, false, ondone)).start(); 
    	return result;
	}
    
    @Override
    public AsyncSupplier<Integer, IOException> readAsync(ByteBuffer buffer, Consumer<Pair<Integer, IOException>> ondone) {
    	if (!buffer.hasRemaining())
    		return IOUtil.success(Integer.valueOf(0), ondone);
        if (input == null || isClosing())
        	return IOUtil.error(new ClosedChannelException(), ondone);
        if (endReached)
        	return IOUtil.success(Integer.valueOf(-1), ondone);

        AsyncSupplier<Integer, IOException> result = new AsyncSupplier<>();
    	TaskUtil.decompressionTask(input, () -> readAsync(buffer, result, 0, false, false, ondone)).start(); 
    	return result;
    }
    
    private void readAsync(ByteBuffer buffer, AsyncSupplier<Integer, IOException> result, int done, boolean fully, boolean headerDecoded, Consumer<Pair<Integer, IOException>> ondone) {
		if (endReached) {
			IOUtil.success(Integer.valueOf(done == 0 ? -1 : done), result, ondone);
			return;
		}

		if (!headerDecoded && uncompressedSize == 0) {
    		if (done > 0 && !fully) {
    			IOUtil.success(Integer.valueOf(done), result, ondone);
    			return;
    		}
    		IAsync<IOException> header = decodeChunkHeaderAsync();
    		if (!header.isDone()) {
    			header.thenStart(TaskUtil.decompressionTask(input, () -> readAsync(buffer, result, done, fully, true, ondone)), result);
    			return;
    		}
    		if (!header.isSuccessful()) {
    			IOUtil.notSuccess(header, result, ondone);
    			return;
    		}
    		if (endReached) {
    			IOUtil.success(Integer.valueOf(done == 0 ? -1 : done), result, ondone);
    			return;
    		}
    	}
    	
        int copySizeMax = Math.min(uncompressedSize, buffer.remaining());

        if (!isLZMAChunk) {
        	IAsync<IOException> copy = lz.copyUncompressedSyncIfPossible(input, copySizeMax);
    		if (!copy.isDone()) {
    			copy.thenStart(TaskUtil.decompressionTask(input, () -> readAsync2(buffer, result, done, fully, ondone)), result);
    			return;
    		}
    		if (!copy.isSuccessful()) {
    			IOUtil.notSuccess(copy, result, ondone);
    			return;
    		}
        } else {
            lz.setLimit(copySizeMax);
            try { lzma.decode(); }
            catch (IOException e) {
            	IOUtil.error(e, result, ondone);
            	return;
            }
        }
        readAsync2(buffer, result, done, fully, ondone);
    }

    private void readAsync2(ByteBuffer buffer, AsyncSupplier<Integer, IOException> result, int done, boolean fully, Consumer<Pair<Integer, IOException>> ondone) {
    	int copiedSize = lz.flush(buffer);
        uncompressedSize -= copiedSize;

        if (uncompressedSize == 0 && (!rc.isFinished() || lz.hasPending())) {
        	IOUtil.error(new CorruptedInputException(), result, ondone);
        	return;
        }
        
        if (!fully || !buffer.hasRemaining())
        	IOUtil.success(Integer.valueOf(done + copiedSize), result, ondone);
        else
        	readAsync(buffer, result, done + copiedSize, true, false, ondone);
    }

    private void decodeChunkHeaderSync() throws IOException {
        int control = input.read();

        if (control == 0x00) {
            endReached = true;
            putArraysToCache();
            return;
        }

        if (control >= 0xE0 || control == 0x01) {
            needProps = true;
            needDictReset = false;
            lz.reset();
        } else if (needDictReset) {
            throw new CorruptedInputException();
        }

        if (control >= 0x80) {
            isLZMAChunk = true;

            uncompressedSize = (control & 0x1F) << 16;
            uncompressedSize += DataUtil.readUnsignedShortBigEndian(input) + 1;

            int compressedSize = DataUtil.readUnsignedShortBigEndian(input) + 1;

            if (control >= 0xC0) {
                needProps = false;
                decodePropsSync();

            } else if (needProps) {
                throw new CorruptedInputException();

            } else if (control >= 0xA0) {
                lzma.reset();
            }

            rc.prepareInputBufferSync(input, compressedSize);

        } else if (control > 0x02) {
            throw new CorruptedInputException();

        } else {
            isLZMAChunk = false;
            uncompressedSize = DataUtil.readUnsignedShortBigEndian(input) + 1;
        }
    }

    private IAsync<IOException> decodeChunkHeaderAsync() {
        int control;
        try { control = input.readAsync(); }
        catch (IOException e) { return new Async<>(e); }
        if (control == -2)
        	return TaskUtil.continueDecompression(input, input.canStartReading(), this::decodeChunkHeaderAsync);

        if (control == 0x00) {
            endReached = true;
            putArraysToCache();
            return new Async<>(true);
        }

        if (control >= 0xE0 || control == 0x01) {
            needProps = true;
            needDictReset = false;
            lz.reset();
        } else if (needDictReset) {
        	return new Async<>(new CorruptedInputException());
        }

        if (control >= 0x80) {
        	byte[] buf = new byte[4];
        	AsyncSupplier<Integer, IOException> read = input.readFullySyncIfPossible(ByteBuffer.wrap(buf, 0, 4));
        	if (!read.isDone())
        		return TaskUtil.continueDecompression(input, read, () -> decodeChunkHeaderLZMAAsync(control, buf));
        	if (!read.isSuccessful()) return read;
    		if (read.getResult().intValue() != 4)
    			return new Async<>(new EOFException());
    		return decodeChunkHeaderLZMAAsync(control, buf);
        }
        if (control > 0x02)
        	return new Async<>(new CorruptedInputException());
        byte[] buf = new byte[2];
        AsyncSupplier<Integer, IOException> read = input.readFullySyncIfPossible(ByteBuffer.wrap(buf, 0, 2));
    	if (!read.isDone())
    		return TaskUtil.continueDecompression(input, read, () -> decodeChunkHeaderUncompressedAsync(buf));
    	if (!read.isSuccessful()) return read;
		if (read.getResult().intValue() != 2)
			return new Async<>(new EOFException());
		return decodeChunkHeaderUncompressedAsync(buf);
    }
    
    private IAsync<IOException> decodeChunkHeaderLZMAAsync(int control, byte[] bufHeader) {
        isLZMAChunk = true;

        uncompressedSize = (control & 0x1F) << 16;
        uncompressedSize += DataUtil.readUnsignedShortBigEndian(bufHeader, 0) + 1;

        int compressedSize = DataUtil.readUnsignedShortBigEndian(bufHeader, 2) + 1;

        if (control >= 0xC0) {
            needProps = false;
            IAsync<IOException> decode = decodePropsAsync();
            if (!decode.isDone())
            	return TaskUtil.continueDecompression(input, decode, () -> rc.prepareInputBufferAsync(input, compressedSize));
            if (!decode.isSuccessful()) return decode;
        } else if (needProps) {
        	return new Async<>(new CorruptedInputException());

        } else if (control >= 0xA0) {
            lzma.reset();
        }

        return rc.prepareInputBufferAsync(input, compressedSize);
    }
    
    private IAsync<IOException> decodeChunkHeaderUncompressedAsync(byte[] buf) {
        isLZMAChunk = false;
        uncompressedSize = DataUtil.readUnsignedShortBigEndian(buf, 0) + 1;
        return new Async<>(true);
    }

    private void decodePropsSync() throws IOException {
        int props = input.read();

        if (props > (4 * 5 + 4) * 9 + 8)
            throw new CorruptedInputException();

        int pb = props / (9 * 5);
        props -= pb * 9 * 5;
        int lp = props / 9;
        int lc = props - lp * 9;

        if (lc + lp > 4)
            throw new CorruptedInputException();

        lzma = new LZMADecoder(lz, rc, lc, lp, pb);
    }

    private IAsync<IOException> decodePropsAsync() {
        int props;
        try { props = input.readAsync(); }
        catch (IOException e) {
        	return new Async<>(e);
        }
        
        if (props == -2)
        	return TaskUtil.continueDecompression(input, input.canStartReading(), this::decodePropsAsync);

        if (props > (4 * 5 + 4) * 9 + 8)
            return new Async<>(new CorruptedInputException());

        int pb = props / (9 * 5);
        props -= pb * 9 * 5;
        int lp = props / 9;
        int lc = props - lp * 9;

        if (lc + lp > 4)
            return new Async<>(new CorruptedInputException());

        lzma = new LZMADecoder(lz, rc, lc, lp, pb);
        return new Async<>(true);
    }

    private void putArraysToCache() {
        if (lz != null) {
            lz.putArraysToCache(arrayCache);
            lz = null;

            rc.putArraysToCache(arrayCache);
            rc = null;
        }
    }

    
    @Override
    protected IAsync<IOException> closeUnderlyingResources() {
		putArraysToCache();
		return input.closeAsync();
    }
    
    @Override
    protected void closeResources(Async<IOException> ondone) {
    	input = null;
    	// TODO
    	ondone.unblock();
    }

	@Override
	public String getSourceDescription() {
		return input != null ? "LZMAReadable[" + input.getSourceDescription() + "]" : "LZMAReadable";
	}

	@Override
	public IO getWrappedIO() {
		return input;
	}

	@Override
	public void setPriority(byte priority) {
		if (input != null) input.setPriority(priority);
	}

	@Override
	public byte getPriority() {
		return input != null ? input.getPriority() : Task.PRIORITY_NORMAL;
	}

	@Override
	public TaskManager getTaskManager() {
		return Threading.getCPUTaskManager();
	}

	@Override
	public IAsync<IOException> canStartReading() {
		if (uncompressedSize > 0) return new Async<>(true);
		return input.canStartReading();
	}

	@Override
	public long skipSync(long n) throws IOException {
		return IOUtil.skipSyncByReading(this, n);
	}

	@Override
	public AsyncSupplier<Long, IOException> skipAsync(long n, Consumer<Pair<Long, IOException>> ondone) {
		return IOUtil.skipAsyncByReading(this, n, ondone);
	}
	
}
