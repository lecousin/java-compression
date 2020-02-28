package net.lecousin.compression.lzma;

import net.lecousin.framework.core.test.io.TestIOError;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.buffering.SimpleBufferedWritable;

public class TestLZMA2Errors extends TestIOError {

	@Override
	protected IO.Readable getReadable(IO.Readable io) throws Exception {
		return new LZMA2Readable(io, 4096);
	}

	@Override
	protected IO.Readable.Buffered getReadableBuffered(IO.Readable io) throws Exception {
		return null;
	}

	@Override
	protected IO.Readable.Seekable getReadableSeekable(IO.Readable.Seekable io) throws Exception {
		return null;
	}

	@Override
	protected IO.Writable getWritable(IO.Writable io) throws Exception {
		LZMA2Options options = new LZMA2Options(0);
		options.setDictSize(4096);
		options.setLc(1);
		options.setLp(1);
		options.setMode(LZMA2Options.Mode.FAST);
		return new LZMA2Writable(new SimpleBufferedWritable(io, 2), options);
	}

}
