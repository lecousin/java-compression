package net.lecousin.compression.lzma.rangecoder;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.lecousin.framework.concurrent.synch.ISynchronizationPoint;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.memory.ByteArrayCache;

public final class RangeEncoderToBuffer extends RangeEncoder {
    private final byte[] buf;
    private int bufPos;

    public RangeEncoderToBuffer(int bufSize, ByteArrayCache arrayCache) {
        buf = arrayCache.get(bufSize, true);
        reset();
    }

    public void putArraysToCache(ByteArrayCache arrayCache) {
        arrayCache.free(buf);
    }

    @Override
	public void reset() {
        super.reset();
        bufPos = 0;
    }

    @Override
	public int getPendingSize() {
        // With LZMA2 it is known that cacheSize fits into an int.
        return bufPos + (int)cacheSize + 5 - 1;
    }

    @Override
	public int finish() {
        // super.finish() cannot throw an IOException because writeByte()
        // provided in this file cannot throw an IOException.
        try {
            super.finish();
        } catch (IOException e) {
            throw new Error();
        }

        return bufPos;
    }

    public void writeSync(IO.Writable.Buffered output) throws IOException {
        output.write(buf, 0, bufPos);
    }
    
    public ISynchronizationPoint<IOException> writeAsync(IO.Writable.Buffered output) {
    	return output.writeAsync(ByteBuffer.wrap(buf, 0, bufPos));
    }

    @Override
	void writeByte(int b) {
        buf[bufPos++] = (byte)b;
    }
}
