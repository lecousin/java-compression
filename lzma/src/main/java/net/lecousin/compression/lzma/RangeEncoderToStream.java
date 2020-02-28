package net.lecousin.compression.lzma;

import java.io.IOException;

import net.lecousin.framework.io.IO;

class RangeEncoderToStream extends RangeEncoder {
	private final IO.Writable.Buffered out;

    public RangeEncoderToStream(IO.Writable.Buffered out) {
        this.out = out;
        reset();
    }

    @Override
	void writeByte(int b) throws IOException {
        out.write((byte)b);
    }
}
