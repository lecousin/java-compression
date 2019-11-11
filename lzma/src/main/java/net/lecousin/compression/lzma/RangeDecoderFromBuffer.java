package net.lecousin.compression.lzma;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.util.DataUtil;
import net.lecousin.framework.memory.ByteArrayCache;

final class RangeDecoderFromBuffer extends RangeDecoder {
    private static final int INIT_SIZE = 5;

    private final byte[] buf;
    private int pos;

    public RangeDecoderFromBuffer(int inputSizeMax, ByteArrayCache arrayCache) {
        // We will use the *end* of the array so if the cache gives us
        // a bigger-than-requested array, we still want to use buf.length.
        buf = arrayCache.get(inputSizeMax - INIT_SIZE, true);
        pos = buf.length;
    }

    public void putArraysToCache(ByteArrayCache arrayCache) {
        arrayCache.free(buf);
    }

    public void prepareInputBufferSync(IO.Readable.Buffered input, int len) throws IOException {
        if (len < INIT_SIZE) throw new CorruptedInputException();
        
        if (input.read() != 0x00) throw new CorruptedInputException();

        code = DataUtil.readIntegerBigEndian(input);
        range = 0xFFFFFFFF;

        // Read the data to the end of the buffer. If the data is corrupt
        // and the decoder, reading from buf, tries to read past the end of
        // the data, ArrayIndexOutOfBoundsException will be thrown and
        // the problem is detected immediately.
        len -= INIT_SIZE;
        pos = buf.length - len;
        if (input.readFullySync(ByteBuffer.wrap(buf, pos, len)) != len)
        	throw new EOFException();
    }

    public IAsync<IOException> prepareInputBufferAsync(IO.Readable.Buffered input, int len) {
        if (len < INIT_SIZE) return new Async<>(new CorruptedInputException());
        
        try {
	        int i = input.readAsync();
	        if (i == -2)
	        	return TaskUtil.continueDecompression(input, input.canStartReading(), () -> prepareInputBufferAsync(input, len));
	
	        if (i != 0x00) return new Async<>(new CorruptedInputException());
	
	        code = DataUtil.readIntegerBigEndian(input);
        } catch (IOException e) {
        	return new Async<>(e);
        }
        range = 0xFFFFFFFF;

        // Read the data to the end of the buffer. If the data is corrupt
        // and the decoder, reading from buf, tries to read past the end of
        // the data, ArrayIndexOutOfBoundsException will be thrown and
        // the problem is detected immediately.
        pos = buf.length - (len - INIT_SIZE);
        return TaskUtil.checkRead(input.readFullySyncIfPossible(ByteBuffer.wrap(buf, pos, len - INIT_SIZE)), len - INIT_SIZE);
    }

    public boolean isFinished() {
        return pos == buf.length && code == 0;
    }

    @Override
	public void normalize() throws IOException {
        if ((range & TOP_MASK) == 0) {
            try {
                // If the input is corrupt, this might throw
                // ArrayIndexOutOfBoundsException.
                code = (code << SHIFT_BITS) | (buf[pos++] & 0xFF);
                range <<= SHIFT_BITS;
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new CorruptedInputException();
            }
        }
    }
}
