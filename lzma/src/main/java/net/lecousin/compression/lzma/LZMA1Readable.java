package net.lecousin.compression.lzma;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.concurrent.threads.Task.Priority;
import net.lecousin.framework.concurrent.threads.TaskManager;
import net.lecousin.framework.concurrent.threads.Threading;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.IOUtil;
import net.lecousin.framework.io.buffering.PreBufferedReadable;
import net.lecousin.framework.memory.ByteArrayCache;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;

public class LZMA1Readable extends ConcurrentCloseable<IOException> implements IO.Readable {
    /**
     * Largest dictionary size supported by this implementation.
     * <p>
     * LZMA allows dictionaries up to one byte less than 4 GiB. This
     * implementation supports only 16 bytes less than 2 GiB. This
     * limitation is due to Java using signed 32-bit integers for array
     * indexing. The limitation shouldn't matter much in practice since so
     * huge dictionaries are not normally used.
     */
    public static final int DICT_SIZE_MAX = Integer.MAX_VALUE & ~15;

    private IO.Readable.Buffered input;
    private ByteArrayCache arrayCache;
    private LZDecoder lz;
    private RangeDecoderFromStream rc;
    private LZMADecoder lzma;

    private boolean endReached = false;

    /**
     * Number of uncompressed bytes left to be decompressed, or -1 if
     * the end marker is used.
     */
    private long remainingSize;

    /**
     * Gets approximate decompressor memory requirements as kibibytes for
     * the given dictionary size and LZMA properties byte (lc, lp, and pb).
     *
     * @param       dictSize    LZMA dictionary size as bytes, should be
     *                          in the range [<code>0</code>,
     *                          <code>DICT_SIZE_MAX</code>]
     *
     * @param       propsByte   LZMA properties byte that encodes the values
     *                          of lc, lp, and pb
     *
     * @return      approximate memory requirements as kibibytes (KiB)
     *
     * @throws      UnsupportedOptionsException
     *                          if <code>dictSize</code> is outside
     *                          the range [<code>0</code>,
     *                          <code>DICT_SIZE_MAX</code>]
     *
     * @throws      CorruptedInputException
     *                          if <code>propsByte</code> is invalid
     */
    public static int getMemoryUsage(int dictSize, byte propsByte)
            throws UnsupportedOptionsException, CorruptedInputException {
        if (dictSize < 0 || dictSize > DICT_SIZE_MAX)
            throw new UnsupportedOptionsException(
                    "LZMA dictionary is too big for this implementation");

        int props = propsByte & 0xFF;
        if (props > (4 * 5 + 4) * 9 + 8)
            throw new CorruptedInputException("Invalid LZMA properties byte");

        props %= 9 * 5;
        int lp = props / 9;
        int lc = props - lp * 9;

        return getMemoryUsage(dictSize, lc, lp);
    }

    /**
     * Gets approximate decompressor memory requirements as kibibytes for
     * the given dictionary size, lc, and lp. Note that pb isn't needed.
     *
     * @param       dictSize    LZMA dictionary size as bytes, must be
     *                          in the range [<code>0</code>,
     *                          <code>DICT_SIZE_MAX</code>]
     *
     * @param       lc          number of literal context bits, must be
     *                          in the range [0, 8]
     *
     * @param       lp          number of literal position bits, must be
     *                          in the range [0, 4]
     *
     * @return      approximate memory requirements as kibibytes (KiB)
     */
    public static int getMemoryUsage(int dictSize, int lc, int lp) {
        if (lc < 0 || lc > 8 || lp < 0 || lp > 4)
            throw new IllegalArgumentException("Invalid lc or lp");

        // Probability variables have the type "short". There are
        // 0x300 (768) probability variables in each literal subcoder.
        // The number of literal subcoders is 2^(lc + lp).
        //
        // Roughly 10 KiB for the base state + LZ decoder's dictionary buffer
        // + sizeof(short) * number probability variables per literal subcoder
        //   * number of literal subcoders
        return 10 + getDictSize(dictSize) / 1024
               + ((2 * 0x300) << (lc + lp)) / 1024;
    }

    private static int getDictSize(int dictSize) {
        if (dictSize < 0 || dictSize > DICT_SIZE_MAX)
            throw new IllegalArgumentException(
                    "LZMA dictionary is too big for this implementation");

        // For performance reasons, use a 4 KiB dictionary if something
        // smaller was requested. It's a rare situation and the performance
        // difference isn't huge, and it starts to matter mostly when the
        // dictionary is just a few bytes. But we need to handle the special
        // case of dictSize == 0 anyway, which is an allowed value but in
        // practice means one-byte dictionary.
        //
        // Note that using a dictionary bigger than specified in the headers
        // can hide errors if there is a reference to data beyond the original
        // dictionary size but is still within 4 KiB.
        if (dictSize < 4096)
            dictSize = 4096;

        // Round dictionary size upward to a multiple of 16. This way LZMA
        // can use LZDecoder.getPos() for calculating LZMA's posMask.
        return (dictSize + 15) & ~15;
    }

    /**
     * Creates a new .lzma file format decompressor without
     * a memory usage limit.
     *
     * @param       in          input stream from which .lzma data is read;
     *                          it might be a good idea to wrap it in
     *                          <code>BufferedInputStream</code>, see the
     *                          note at the top of this page
     *
     * @throws      CorruptedInputException
     *                          file is corrupt or perhaps not in
     *                          the .lzma format at all
     *
     * @throws      UnsupportedOptionsException
     *                          dictionary size or uncompressed size is too
     *                          big for this implementation
     *
     * @throws      EOFException
     *                          file is truncated or perhaps not in
     *                          the .lzma format at all
     *
     * @throws      IOException may be thrown by <code>in</code>
     */
    public LZMA1Readable(IO.Readable in) throws IOException {
        this(in, -1);
    }

    /**
     * Creates a new .lzma file format decompressor without
     * a memory usage limit.
     * <p>
     * This is identical to <code>LZMAInputStream(InputStream)</code>
     * except that this also takes the <code>arrayCache</code> argument.
     *
     * @param       in          input stream from which .lzma data is read;
     *                          it might be a good idea to wrap it in
     *                          <code>BufferedInputStream</code>, see the
     *                          note at the top of this page
     *
     *
     * @param       arrayCache  cache to be used for allocating large arrays
     *
     * @throws      CorruptedInputException
     *                          file is corrupt or perhaps not in
     *                          the .lzma format at all
     *
     * @throws      UnsupportedOptionsException
     *                          dictionary size or uncompressed size is too
     *                          big for this implementation
     *
     * @throws      EOFException
     *                          file is truncated or perhaps not in
     *                          the .lzma format at all
     *
     * @throws      IOException may be thrown by <code>in</code>
     *
     * @since 1.7
     */
    public LZMA1Readable(IO.Readable in, ByteArrayCache arrayCache)
            throws IOException {
        this(in, -1, arrayCache);
    }

    /**
     * Creates a new .lzma file format decompressor with an optional
     * memory usage limit.
     *
     * @param       in          input stream from which .lzma data is read;
     *                          it might be a good idea to wrap it in
     *                          <code>BufferedInputStream</code>, see the
     *                          note at the top of this page
     *
     * @param       memoryLimit memory usage limit in kibibytes (KiB)
     *                          or <code>-1</code> to impose no
     *                          memory usage limit
     *
     * @throws      CorruptedInputException
     *                          file is corrupt or perhaps not in
     *                          the .lzma format at all
     *
     * @throws      UnsupportedOptionsException
     *                          dictionary size or uncompressed size is too
     *                          big for this implementation
     *
     * @throws      MemoryLimitException
     *                          memory usage limit was exceeded
     *
     * @throws      EOFException
     *                          file is truncated or perhaps not in
     *                          the .lzma format at all
     *
     * @throws      IOException may be thrown by <code>in</code>
     */
    public LZMA1Readable(IO.Readable in, int memoryLimit) throws IOException {
        this(in, memoryLimit, ByteArrayCache.getInstance());
    }

    /**
     * Creates a new .lzma file format decompressor with an optional
     * memory usage limit.
     * <p>
     * This is identical to <code>LZMAInputStream(InputStream, int)</code>
     * except that this also takes the <code>arrayCache</code> argument.
     *
     * @param       in          input stream from which .lzma data is read;
     *                          it might be a good idea to wrap it in
     *                          <code>BufferedInputStream</code>, see the
     *                          note at the top of this page
     *
     * @param       memoryLimit memory usage limit in kibibytes (KiB)
     *                          or <code>-1</code> to impose no
     *                          memory usage limit
     *
     * @param       arrayCache  cache to be used for allocating large arrays
     *
     * @throws      CorruptedInputException
     *                          file is corrupt or perhaps not in
     *                          the .lzma format at all
     *
     * @throws      UnsupportedOptionsException
     *                          dictionary size or uncompressed size is too
     *                          big for this implementation
     *
     * @throws      MemoryLimitException
     *                          memory usage limit was exceeded
     *
     * @throws      EOFException
     *                          file is truncated or perhaps not in
     *                          the .lzma format at all
     *
     * @throws      IOException may be thrown by <code>in</code>
     *
     * @since 1.7
     */
    public LZMA1Readable(IO.Readable in, int memoryLimit, ByteArrayCache arrayCache) throws IOException {
        if (!(in instanceof IO.Readable.Buffered))
        	in = new PreBufferedReadable(in, 8192, input.getPriority(), 65536, input.getPriority(), 8);

        IO.Readable.Buffered bio = (IO.Readable.Buffered)in;
        
        // Properties byte (lc, lp, and pb)
        byte propsByte = bio.readByte();

        // Dictionary size is an unsigned 32-bit little endian integer.
        int dictSize = 0;
        for (int i = 0; i < 4; ++i)
            dictSize |= (bio.readByte() & 0xFF) << (8 * i);

        // Uncompressed size is an unsigned 64-bit little endian integer.
        // The maximum 64-bit value is a special case (becomes -1 here)
        // which indicates that the end marker is used instead of knowing
        // the uncompressed size beforehand.
        long uncompSize = 0;
        for (int i = 0; i < 8; ++i)
            uncompSize |= (long)(bio.readByte() & 0xFF) << (8 * i);

        // Check the memory usage limit.
        int memoryNeeded = getMemoryUsage(dictSize, propsByte);
        if (memoryLimit != -1 && memoryNeeded > memoryLimit)
            throw new IOException("Need " + memoryNeeded + "KB of memory, limit is " + memoryLimit);

        initialize(bio, uncompSize, propsByte, dictSize, null, arrayCache);
    }

    /**
     * Creates a new input stream that decompresses raw LZMA data (no .lzma
     * header) from <code>in</code>.
     * <p>
     * The caller needs to know if the "end of payload marker (EOPM)" alias
     * "end of stream marker (EOS marker)" alias "end marker" present.
     * If the end marker isn't used, the caller must know the exact
     * uncompressed size of the stream.
     * <p>
     * The caller also needs to provide the LZMA properties byte that encodes
     * the number of literal context bits (lc), literal position bits (lp),
     * and position bits (pb).
     * <p>
     * The dictionary size used when compressing is also needed. Specifying
     * a too small dictionary size will prevent decompressing the stream.
     * Specifying a too big dictionary is waste of memory but decompression
     * will work.
     * <p>
     * There is no need to specify a dictionary bigger than
     * the uncompressed size of the data even if a bigger dictionary
     * was used when compressing. If you know the uncompressed size
     * of the data, this might allow saving some memory.
     *
     * @param       in          input stream from which compressed
     *                          data is read
     *
     * @param       uncompSize  uncompressed size of the LZMA stream or -1
     *                          if the end marker is used in the LZMA stream
     *
     * @param       propsByte   LZMA properties byte that has the encoded
     *                          values for literal context bits (lc), literal
     *                          position bits (lp), and position bits (pb)
     *
     * @param       dictSize    dictionary size as bytes, must be in the range
     *                          [<code>0</code>, <code>DICT_SIZE_MAX</code>]
     *
     * @throws      CorruptedInputException
     *                          if <code>propsByte</code> is invalid or
     *                          the first input byte is not 0x00
     *
     * @throws      UnsupportedOptionsException
     *                          dictionary size or uncompressed size is too
     *                          big for this implementation
     *
     *
     */
    public LZMA1Readable(IO.Readable in, long uncompSize, byte propsByte, int dictSize) throws IOException {
        initialize(in, uncompSize, propsByte, dictSize, null, ByteArrayCache.getInstance());
    }

    /**
     * Creates a new input stream that decompresses raw LZMA data (no .lzma
     * header) from <code>in</code> optionally with a preset dictionary.
     *
     * @param       in          input stream from which LZMA-compressed
     *                          data is read
     *
     * @param       uncompSize  uncompressed size of the LZMA stream or -1
     *                          if the end marker is used in the LZMA stream
     *
     * @param       propsByte   LZMA properties byte that has the encoded
     *                          values for literal context bits (lc), literal
     *                          position bits (lp), and position bits (pb)
     *
     * @param       dictSize    dictionary size as bytes, must be in the range
     *                          [<code>0</code>, <code>DICT_SIZE_MAX</code>]
     *
     * @param       presetDict  preset dictionary or <code>null</code>
     *                          to use no preset dictionary
     *
     * @throws      CorruptedInputException
     *                          if <code>propsByte</code> is invalid or
     *                          the first input byte is not 0x00
     *
     * @throws      UnsupportedOptionsException
     *                          dictionary size or uncompressed size is too
     *                          big for this implementation
     *
     * @throws      EOFException file is truncated or corrupt
     *
     * @throws      IOException may be thrown by <code>in</code>
     */
    public LZMA1Readable(IO.Readable in, long uncompSize, byte propsByte, int dictSize, byte[] presetDict)
    throws IOException {
        initialize(in, uncompSize, propsByte, dictSize, presetDict, ByteArrayCache.getInstance());
    }

    /**
     * Creates a new input stream that decompresses raw LZMA data (no .lzma
     * header) from <code>in</code> optionally with a preset dictionary.
     * <p>
     * This is identical to <code>LZMAInputStream(InputStream, long, byte, int,
     * byte[])</code> except that this also takes the <code>arrayCache</code>
     * argument.
     *
     * @param       in          input stream from which LZMA-compressed
     *                          data is read
     *
     * @param       uncompSize  uncompressed size of the LZMA stream or -1
     *                          if the end marker is used in the LZMA stream
     *
     * @param       propsByte   LZMA properties byte that has the encoded
     *                          values for literal context bits (lc), literal
     *                          position bits (lp), and position bits (pb)
     *
     * @param       dictSize    dictionary size as bytes, must be in the range
     *                          [<code>0</code>, <code>DICT_SIZE_MAX</code>]
     *
     * @param       presetDict  preset dictionary or <code>null</code>
     *                          to use no preset dictionary
     *
     * @param       arrayCache  cache to be used for allocating large arrays
     *
     * @throws      CorruptedInputException
     *                          if <code>propsByte</code> is invalid or
     *                          the first input byte is not 0x00
     *
     * @throws      UnsupportedOptionsException
     *                          dictionary size or uncompressed size is too
     *                          big for this implementation
     *
     * @throws      EOFException file is truncated or corrupt
     *
     * @throws      IOException may be thrown by <code>in</code>
     *
     * @since 1.7
     */
    public LZMA1Readable(
    	IO.Readable in, long uncompSize, byte propsByte, int dictSize, byte[] presetDict, ByteArrayCache arrayCache
    ) throws IOException {
        initialize(in, uncompSize, propsByte, dictSize, presetDict, arrayCache);
    }

    /**
     * Creates a new input stream that decompresses raw LZMA data (no .lzma
     * header) from <code>in</code> optionally with a preset dictionary.
     *
     * @param       in          input stream from which LZMA-compressed
     *                          data is read
     *
     * @param       uncompSize  uncompressed size of the LZMA stream or -1
     *                          if the end marker is used in the LZMA stream
     *
     * @param       lc          number of literal context bits, must be
     *                          in the range [0, 8]
     *
     * @param       lp          number of literal position bits, must be
     *                          in the range [0, 4]
     *
     * @param       pb          number position bits, must be
     *                          in the range [0, 4]
     *
     * @param       dictSize    dictionary size as bytes, must be in the range
     *                          [<code>0</code>, <code>DICT_SIZE_MAX</code>]
     *
     * @param       presetDict  preset dictionary or <code>null</code>
     *                          to use no preset dictionary
     *
     * @throws      CorruptedInputException
     *                          if the first input byte is not 0x00
     *
     * @throws      EOFException file is truncated or corrupt
     *
     * @throws      IOException may be thrown by <code>in</code>
     */
    public LZMA1Readable(
    	IO.Readable in, long uncompSize, int lc, int lp, int pb, int dictSize, byte[] presetDict
    ) throws IOException {
        initialize(in, uncompSize, lc, lp, pb, dictSize, presetDict, ByteArrayCache.getInstance());
    }

    /**
     * Creates a new input stream that decompresses raw LZMA data (no .lzma
     * header) from <code>in</code> optionally with a preset dictionary.
     * <p>
     * This is identical to <code>LZMAInputStream(InputStream, long, int, int,
     * int, int, byte[])</code> except that this also takes the
     * <code>arrayCache</code> argument.
     *
     * @param       in          input stream from which LZMA-compressed
     *                          data is read
     *
     * @param       uncompSize  uncompressed size of the LZMA stream or -1
     *                          if the end marker is used in the LZMA stream
     *
     * @param       lc          number of literal context bits, must be
     *                          in the range [0, 8]
     *
     * @param       lp          number of literal position bits, must be
     *                          in the range [0, 4]
     *
     * @param       pb          number position bits, must be
     *                          in the range [0, 4]
     *
     * @param       dictSize    dictionary size as bytes, must be in the range
     *                          [<code>0</code>, <code>DICT_SIZE_MAX</code>]
     *
     * @param       presetDict  preset dictionary or <code>null</code>
     *                          to use no preset dictionary
     *
     * @param       arrayCache  cache to be used for allocating large arrays
     *
     * @throws      CorruptedInputException
     *                          if the first input byte is not 0x00
     *
     * @throws      EOFException file is truncated or corrupt
     *
     * @throws      IOException may be thrown by <code>in</code>
     */
    public LZMA1Readable(
    	IO.Readable in, long uncompSize, int lc, int lp, int pb,
        int dictSize, byte[] presetDict, ByteArrayCache arrayCache
    ) throws IOException {
        initialize(in, uncompSize, lc, lp, pb, dictSize, presetDict, arrayCache);
    }

    private void initialize(
    	IO.Readable in, long uncompSize, byte propsByte, int dictSize, byte[] presetDict, ByteArrayCache arrayCache
    ) throws IOException {
        // Validate the uncompressed size since the other "initialize" throws
        // IllegalArgumentException if uncompSize < -1.
        if (uncompSize < -1)
            throw new UnsupportedOptionsException(
                    "Uncompressed size is too big");

        // Decode the properties byte. In contrast to LZMA2, there is no
        // limit of lc + lp <= 4.
        int props = propsByte & 0xFF;
        if (props > (4 * 5 + 4) * 9 + 8)
            throw new CorruptedInputException("Invalid LZMA properties byte");

        int pb = props / (9 * 5);
        props -= pb * 9 * 5;
        int lp = props / 9;
        int lc = props - lp * 9;

        // Validate the dictionary size since the other "initialize" throws
        // IllegalArgumentException if dictSize is not supported.
        if (dictSize < 0 || dictSize > DICT_SIZE_MAX)
            throw new UnsupportedOptionsException(
                    "LZMA dictionary is too big for this implementation");

        initialize(in, uncompSize, lc, lp, pb, dictSize, presetDict, arrayCache);
    }

    private void initialize(
    	IO.Readable in, long uncompSize, int lc, int lp, int pb, int dictSize, byte[] presetDict, ByteArrayCache arrayCache
    ) throws IOException {
        // getDictSize validates dictSize and gives a message in
        // the exception too, so skip validating dictSize here.
        if (uncompSize < -1 || lc < 0 || lc > 8 || lp < 0 || lp > 4
                || pb < 0 || pb > 4)
            throw new IllegalArgumentException();

        if (in instanceof IO.Readable.Buffered)
        	this.input = (IO.Readable.Buffered)in;
        else
        	this.input = new PreBufferedReadable(in, 8192, input.getPriority(), 65536, input.getPriority(), 8);
        this.arrayCache = arrayCache;

        // If uncompressed size is known, use it to avoid wasting memory for
        // a uselessly large dictionary buffer.
        dictSize = getDictSize(dictSize);
        if (uncompSize >= 0 && dictSize > uncompSize)
            dictSize = getDictSize((int)uncompSize);

        lz = new LZDecoder(getDictSize(dictSize), presetDict);
        rc = new RangeDecoderFromStream(input);
        lzma = new LZMADecoder(lz, rc, lc, lp, pb);

        remainingSize = uncompSize;
    }

    @Override
    public int readSync(ByteBuffer buffer) throws IOException {
    	return readSync(buffer, false);
    }
    
    @Override
    public int readFullySync(ByteBuffer buffer) throws IOException {
    	return readSync(buffer, true);
    }

    private int readSync(ByteBuffer buffer, boolean fully) throws IOException {
        if (!buffer.hasRemaining())
            return 0;

        if (input == null)
            throw new IOException("Stream closed");

        if (endReached)
            return -1;

        int size = 0;

        while (buffer.hasRemaining()) {
            // If uncompressed size is known and thus no end marker will
            // be present, set the limit so that the uncompressed size
            // won't be exceeded.
            int copySizeMax = buffer.remaining();
            if (remainingSize >= 0 && remainingSize < buffer.remaining())
                copySizeMax = (int)remainingSize;

            lz.setLimit(copySizeMax);

            // Decode into the dictionary buffer.
            try {
                lzma.decode();
            } catch (CorruptedInputException e) {
                // The end marker is encoded with a LZMA symbol that
                // indicates maximum match distance. This is larger
                // than any supported dictionary and thus causes
                // CorruptedInputException from LZDecoder.repeat.
                if (remainingSize != -1 || !lzma.endMarkerDetected())
                    throw e;

                endReached = true;

                // The exception makes lzma.decode() miss the last range
                // decoder normalization, so do it here. This might
                // cause an IOException if it needs to read a byte
                // from the input stream.
                rc.normalize();
            }

            // Copy from the dictionary to buf.
            int copiedSize = lz.flush(buffer);
            size += copiedSize;

            if (remainingSize >= 0) {
                // Update the number of bytes left to be decompressed.
                remainingSize -= copiedSize;
                assert remainingSize >= 0;

                if (remainingSize == 0)
                    endReached = true;
            }

            if (endReached) {
                // Checking these helps a lot when catching corrupt
                // or truncated .lzma files. LZMA Utils doesn't do
                // the first check and thus it accepts many invalid
                // files that this implementation and XZ Utils don't.
                if (!rc.isFinished() || lz.hasPending())
                    throw new CorruptedInputException();

                putArraysToCache();
                return size == 0 ? -1 : size;
            }
            
            if (size > 0 && !fully)
            	break;
        }

        return size;
    }
    
    @Override
    public AsyncSupplier<Integer, IOException> readAsync(ByteBuffer buffer, Consumer<Pair<Integer, IOException>> ondone) {
    	return Task.cpu("Read LZMA1", input.getPriority(), () -> {
    		return Integer.valueOf(readSync(buffer, false));
    	}, ondone).start().getOutput();
    }
    
    @Override
    public AsyncSupplier<Integer, IOException> readFullyAsync(ByteBuffer buffer, Consumer<Pair<Integer, IOException>> ondone) {
    	return Task.cpu("Read LZMA1", input.getPriority(), () -> {
    		return Integer.valueOf(readSync(buffer, true));
    	}, ondone).start().getOutput();
    }

    private void putArraysToCache() {
        if (lz != null) {
            lz.putArraysToCache(arrayCache);
            lz = null;
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
		return input != null ? "LZMA1Readable[" + input.getSourceDescription() + "]" : "LZMA1Readable";
	}

	@Override
	public IO getWrappedIO() {
		return input;
	}

	@Override
	public void setPriority(Priority priority) {
		if (input != null) input.setPriority(priority);
	}

	@Override
	public Priority getPriority() {
		return input != null ? input.getPriority() : Task.Priority.NORMAL;
	}

	@Override
	public TaskManager getTaskManager() {
		return Threading.getCPUTaskManager();
	}

	@Override
	public IAsync<IOException> canStartReading() {
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
