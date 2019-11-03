package net.lecousin.compression.lzma;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.memory.ByteArrayCache;

final class LZDecoder {
    private final byte[] buf;
    private final int bufSize; // To avoid buf.length with an array-cached buf.
    private int start = 0;
    private int pos = 0;
    private int full = 0;
    private int limit = 0;
    private int pendingLen = 0;
    private int pendingDist = 0;

    public LZDecoder(int dictSize, byte[] presetDict, ByteArrayCache arrayCache) {
        bufSize = dictSize;
        buf = arrayCache.get(bufSize, true);

        if (presetDict != null) {
            pos = Math.min(presetDict.length, dictSize);
            full = pos;
            start = pos;
            System.arraycopy(presetDict, presetDict.length - pos, buf, 0, pos);
        }
    }

    public void putArraysToCache(ByteArrayCache arrayCache) {
        arrayCache.free(buf);
    }

    public void reset() {
        start = 0;
        pos = 0;
        full = 0;
        limit = 0;
        buf[bufSize - 1] = 0x00;
    }

    public void setLimit(int outMax) {
        if (bufSize - pos <= outMax)
            limit = bufSize;
        else
            limit = pos + outMax;
    }

    public boolean hasSpace() {
        return pos < limit;
    }

    public boolean hasPending() {
        return pendingLen > 0;
    }

    public int getPos() {
        return pos;
    }

    public int getByte(int dist) {
        int offset = pos - dist - 1;
        if (dist >= pos)
            offset += bufSize;

        return buf[offset] & 0xFF;
    }

    public void putByte(byte b) {
        buf[pos++] = b;

        if (full < pos)
            full = pos;
    }

    public void repeat(int dist, int len) throws IOException {
        if (dist < 0 || dist >= full)
            throw new CorruptedInputException();

        int left = Math.min(limit - pos, len);
        pendingLen = len - left;
        pendingDist = dist;

        int back = pos - dist - 1;
        if (dist >= pos)
            back += bufSize;

        do {
            buf[pos++] = buf[back++];
            if (back == bufSize)
                back = 0;
        } while (--left > 0);

        if (full < pos)
            full = pos;
    }

    public void repeatPending() throws IOException {
        if (pendingLen > 0)
            repeat(pendingDist, pendingLen);
    }

    public IAsync<IOException> copyUncompressedSyncIfPossible(IO.Readable.Buffered input, int len) {
        int copySize = Math.min(bufSize - pos, len);
        return TaskUtil.checkRead(
        	input.readFullySyncIfPossible(ByteBuffer.wrap(buf, pos, copySize), (res) -> {
        		if (res.getValue1() != null) {
        	        pos += copySize;

        	        if (full < pos)
        	            full = pos;
        		}
        	}),
        	copySize
        );
    }

    public void copyUncompressedSync(IO.Readable.Buffered input, int len) throws IOException {
        int copySize = Math.min(bufSize - pos, len);
        int nb = input.readFullySync(ByteBuffer.wrap(buf, pos, copySize));
        if (nb < copySize) throw new EOFException();
        pos += copySize;

        if (full < pos)
            full = pos;
    }

    public int flush(ByteBuffer buffer) {
        int copySize = pos - start;
        if (pos == bufSize)
            pos = 0;

        buffer.put(buf, start, copySize);
        start = pos;

        return copySize;
    }
}
