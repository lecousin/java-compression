package net.lecousin.compression.lzma;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.lecousin.compression.lzma.rangecoder.RangeEncoderToBuffer;
import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.TaskManager;
import net.lecousin.framework.concurrent.Threading;
import net.lecousin.framework.concurrent.synch.AsyncWork;
import net.lecousin.framework.concurrent.synch.ISynchronizationPoint;
import net.lecousin.framework.concurrent.synch.SynchronizationPoint;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.IOUtil;
import net.lecousin.framework.io.util.DataUtil;
import net.lecousin.framework.memory.ByteArrayCache;
import net.lecousin.framework.memory.IntArrayCache;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;
import net.lecousin.framework.util.RunnableWithParameter;

public class LZMA2Writable extends ConcurrentCloseable implements IO.Writable {

    static final int COMPRESSED_SIZE_MAX = 64 << 10;

    private final ByteArrayCache arrayCache;

    private IO.Writable.Buffered output;

    private LZEncoder lz;
    private RangeEncoderToBuffer rc;
    private LZMAEncoder lzma;

    private final int props; // Cannot change props on the fly for now.
    private boolean dictResetNeeded = true;
    private boolean stateResetNeeded = true;
    private boolean propsNeeded = true;

    private int pendingSize = 0;
    private boolean finished = false;

    private static int getExtraSizeBefore(int dictSize) {
        return COMPRESSED_SIZE_MAX > dictSize
               ? COMPRESSED_SIZE_MAX - dictSize : 0;
    }

    static int getMemoryUsage(LZMA2Options options) {
        // 64 KiB buffer for the range encoder + a little extra + LZMAEncoder
        int dictSize = options.getDictSize();
        int extraSizeBefore = getExtraSizeBefore(dictSize);
        return 70 + LZMAEncoder.getMemoryUsage(options.getMode(),
                                               dictSize, extraSizeBefore,
                                               options.getMatchFinder());
    }

    public LZMA2Writable(IO.Writable.Buffered output, LZMA2Options options) {
    	this(output, options, ByteArrayCache.getInstance(), IntArrayCache.getInstance());
    }
    
    public LZMA2Writable(IO.Writable.Buffered output, LZMA2Options options, ByteArrayCache byteArrayCache, IntArrayCache intArrayCache) {
        if (output == null)
            throw new NullPointerException();

        this.arrayCache = byteArrayCache;
        this.output = output;
        rc = new RangeEncoderToBuffer(COMPRESSED_SIZE_MAX, arrayCache);

        int dictSize = options.getDictSize();
        int extraSizeBefore = getExtraSizeBefore(dictSize);
        lzma = LZMAEncoder.getInstance(rc,
                options.getLc(), options.getLp(), options.getPb(),
                options.getMode(),
                dictSize, extraSizeBefore, options.getNiceLen(),
                options.getMatchFinder(), options.getDepthLimit(),
                this.arrayCache, intArrayCache);

        lz = lzma.getLZEncoder();

        byte[] presetDict = options.getPresetDict();
        if (presetDict != null && presetDict.length > 0) {
            lz.setPresetDict(dictSize, presetDict);
            dictResetNeeded = false;
        }

        props = (options.getPb() * 5 + options.getLp()) * 9 + options.getLc();
    }

    @Override
    public int writeSync(ByteBuffer buffer) throws IOException {
        if (finished)
            throw new IOException("Stream finished or closed");

        int done = 0;
        while (buffer.hasRemaining()) {
            int used = lz.fillWindow(buffer);
            done += used;
            pendingSize += used;

            if (lzma.encodeForLZMA2())
                writeChunkSync();
        }
        return done;
    }
    
    @Override
    public AsyncWork<Integer, IOException> writeAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer, IOException>> ondone) {
        if (finished)
            return IOUtil.error(new IOException("Stream finished or closed"), ondone);

        AsyncWork<Integer, IOException> result = new AsyncWork<>();
        TaskUtil.compressionTask(output, () -> {
        	writeAsync(buffer, 0, result, ondone);
        }).start();
        return result;
    }
    
    private void writeAsync(ByteBuffer buffer, int done, AsyncWork<Integer, IOException> result, RunnableWithParameter<Pair<Integer, IOException>> ondone) {
        while (buffer.hasRemaining()) {
            int used = lz.fillWindow(buffer);
            done += used;
            pendingSize += used;

            if (lzma.encodeForLZMA2()) {
                ISynchronizationPoint<IOException> write = writeChunkAsync();
                if (!write.isUnblocked()) {
                	int d = done;
                	write.listenAsync(TaskUtil.compressionTask(output, () -> {
                		writeAsync(buffer, d, result, ondone);
                	}), result);
                	return;
                }
                if (!write.isSuccessful()) {
                	IOUtil.notSuccess(write, result, ondone);
                	return;
                }
            }
        }
        IOUtil.success(Integer.valueOf(done), result, ondone);
    }

    private void writeChunkSync() throws IOException {
        int compressedSize = rc.finish();
        int uncompressedSize = lzma.getUncompressedSize();

        assert compressedSize > 0 : compressedSize;
        assert uncompressedSize > 0 : uncompressedSize;

        // +2 because the header of a compressed chunk is 2 bytes
        // bigger than the header of an uncompressed chunk.
        if (compressedSize + 2 < uncompressedSize) {
            writeLZMASync(uncompressedSize, compressedSize);
        } else {
            lzma.reset();
            uncompressedSize = lzma.getUncompressedSize();
            assert uncompressedSize > 0 : uncompressedSize;
            writeUncompressedSync(uncompressedSize);
        }

        pendingSize -= uncompressedSize;
        lzma.resetUncompressedSize();
        rc.reset();
    }

    private ISynchronizationPoint<IOException> writeChunkAsync() {
        int compressedSize = rc.finish();
        int uncompressedSize = lzma.getUncompressedSize();

        assert compressedSize > 0 : compressedSize;
        assert uncompressedSize > 0 : uncompressedSize;

        // +2 because the header of a compressed chunk is 2 bytes
        // bigger than the header of an uncompressed chunk.
        ISynchronizationPoint<IOException> write;
        if (compressedSize + 2 < uncompressedSize) {
            write = writeLZMAAsync(uncompressedSize, compressedSize);
        } else {
            lzma.reset();
            uncompressedSize = lzma.getUncompressedSize();
            assert uncompressedSize > 0 : uncompressedSize;
            write = writeUncompressedAsync(uncompressedSize);
        }

        if (!write.isUnblocked()) {
        	SynchronizationPoint<IOException> sp = new SynchronizationPoint<>();
        	int uc = uncompressedSize;
        	write.listenAsync(TaskUtil.compressionTask(output, () -> {
                endWriteChunk(uc);
                sp.unblock();
        	}), sp);
        	return sp;
        }
        if (!write.isSuccessful())
        	return write;
        endWriteChunk(uncompressedSize);
        return new SynchronizationPoint<>(true);
    }
    
    private void endWriteChunk(int uncompressedSize) {
        pendingSize -= uncompressedSize;
        lzma.resetUncompressedSize();
        rc.reset();
    }

    private void writeLZMASync(int uncompressedSize, int compressedSize)
    throws IOException {
        int control;

        if (propsNeeded) {
            if (dictResetNeeded)
                control = 0x80 + (3 << 5);
            else
                control = 0x80 + (2 << 5);
        } else {
            if (stateResetNeeded)
                control = 0x80 + (1 << 5);
            else
                control = 0x80;
        }

        control |= (uncompressedSize - 1) >>> 16;
        output.write((byte)control);

        DataUtil.writeShortBigEndian(output, (short)(uncompressedSize - 1));
        DataUtil.writeShortBigEndian(output, (short)(compressedSize - 1));

        if (propsNeeded)
        	output.write((byte)props);

        rc.writeSync(output);

        propsNeeded = false;
        stateResetNeeded = false;
        dictResetNeeded = false;
    }

    private ISynchronizationPoint<IOException> writeLZMAAsync(int uncompressedSize, int compressedSize) {
        int control;

        if (propsNeeded) {
            if (dictResetNeeded)
                control = 0x80 + (3 << 5);
            else
                control = 0x80 + (2 << 5);
        } else {
            if (stateResetNeeded)
                control = 0x80 + (1 << 5);
            else
                control = 0x80;
        }

        control |= (uncompressedSize - 1) >>> 16;
        try { // TODO async ?
	        output.write((byte)control);
	
	        DataUtil.writeShortBigEndian(output, (short)(uncompressedSize - 1));
	        DataUtil.writeShortBigEndian(output, (short)(compressedSize - 1));
	
	        if (propsNeeded)
	        	output.write((byte)props);
        } catch (IOException e) {
        	return new SynchronizationPoint<>(e);
        }

        propsNeeded = false;
        stateResetNeeded = false;
        dictResetNeeded = false;

        return rc.writeAsync(output);
    }

    private void writeUncompressedSync(int uncompressedSize) throws IOException {
        while (uncompressedSize > 0) {
            int chunkSize = Math.min(uncompressedSize, COMPRESSED_SIZE_MAX);
            output.write((byte)(dictResetNeeded ? 0x01 : 0x02));
            DataUtil.writeShortBigEndian(output, (short)(chunkSize - 1));
            lz.copyUncompressedSync(output, uncompressedSize, chunkSize);
            uncompressedSize -= chunkSize;
            dictResetNeeded = false;
        }

        stateResetNeeded = true;
    }

    private ISynchronizationPoint<IOException> writeUncompressedAsync(int uncompressedSize) {
        while (uncompressedSize > 0) {
            int chunkSize = Math.min(uncompressedSize, COMPRESSED_SIZE_MAX);
            try { // TODO async ?
	            output.write((byte)(dictResetNeeded ? 0x01 : 0x02));
	            DataUtil.writeShortBigEndian(output, (short)(chunkSize - 1));
            } catch (IOException e) {
            	return new SynchronizationPoint<>(e);
            }
            ISynchronizationPoint<IOException> write = lz.copyUncompressedAsync(output, uncompressedSize, chunkSize);
            uncompressedSize -= chunkSize;
            dictResetNeeded = false;
            if (!write.isUnblocked()) {
            	int remaining = uncompressedSize;
            	return TaskUtil.continueCompression(output, write, () -> { return writeUncompressedAsync(remaining); });
            } if (!write.isSuccessful())
            	return write;
        }

        stateResetNeeded = true;
        return new SynchronizationPoint<>(true);
    }

    public void finishSync() throws IOException {
        assert !finished;

        lz.setFinishing();

        while (pendingSize > 0) {
            lzma.encodeForLZMA2();
            writeChunkSync();
        }

        output.write((byte)0x00);

        finished = true;

        lzma.putArraysToCache(arrayCache);
        lzma = null;
        lz = null;
        rc.putArraysToCache(arrayCache);
        rc = null;
        output.flush().blockException(0);
    }

    public ISynchronizationPoint<IOException> finishAsync() {
        assert !finished;

        lz.setFinishing();
        SynchronizationPoint<IOException> sp = new SynchronizationPoint<>();
        finishing(sp);
        return sp;
    }
    
    private void finishing(SynchronizationPoint<IOException> sp) {
        while (pendingSize > 0) {
            lzma.encodeForLZMA2();
            ISynchronizationPoint<IOException> write = writeChunkAsync();
            if (!write.isUnblocked()) {
            	write.listenAsync(TaskUtil.compressionTask(output, () -> {
            		finishing(sp);
            	}), sp);
            	return;
            }
            if (!write.isSuccessful()) {
            	if (write.hasError()) sp.error(write.getError());
            	else sp.cancel(write.getCancelEvent());
            	return;
            }
        }

        try {
        	output.write((byte)0x00);
        } catch (IOException e) {
        	sp.error(e);
        	return;
        }

        finished = true;

        lzma.putArraysToCache(arrayCache);
        lzma = null;
        lz = null;
        rc.putArraysToCache(arrayCache);
        rc = null;
        output.flush().listenInline(sp);
    }
    
    @Override
    protected ISynchronizationPoint<?> closeUnderlyingResources() {
    	if (output != null) {
    		if (!finished) {
    			SynchronizationPoint<Exception> sp = new SynchronizationPoint<>();
    			finishAsync().listenInlineSP(() -> {
    				output.closeAsync().listenInline(sp);
    			}, sp);
    			return sp;
    		}
    		return output.closeAsync();
    	}
    	return new SynchronizationPoint<>(true);
    }
    
    @Override
    protected void closeResources(SynchronizationPoint<Exception> ondone) {
    	output = null;
    	// TODO
    	ondone.unblock();
    }

	@Override
	public String getSourceDescription() {
		return output != null ? "LZMAWritable[" + output.getSourceDescription() + "]" : "LZMAWritable";
	}

	@Override
	public IO getWrappedIO() {
		return output;
	}

	@Override
	public void setPriority(byte priority) {
		if (output != null) output.setPriority(priority);
	}

	@Override
	public byte getPriority() {
		return output != null ? output.getPriority() : Task.PRIORITY_NORMAL;
	}

	@Override
	public TaskManager getTaskManager() {
		return Threading.getCPUTaskManager();
	}

	@Override
	public ISynchronizationPoint<IOException> canStartWriting() {
		return output.canStartWriting();
	}
	
}