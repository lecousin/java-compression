package net.lecousin.compression.lzma;

import java.io.IOException;

import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.util.DataUtil;

final class RangeDecoderFromStream extends RangeDecoder {
    
	private final IO.Readable.Buffered in;

    public RangeDecoderFromStream(IO.Readable.Buffered in) throws IOException {
    	this.in = in;
    	
        if (in.readByte() != 0x00)
            throw new CorruptedInputException();

        code = DataUtil.Read32.BE.read(in);
        range = 0xFFFFFFFF;
    }

    public boolean isFinished() {
        return code == 0;
    }

    @Override
	public void normalize() throws IOException {
        if ((range & TOP_MASK) == 0) {
            code = (code << SHIFT_BITS) | (in.readByte() & 0xFF);
            range <<= SHIFT_BITS;
        }
    }
}