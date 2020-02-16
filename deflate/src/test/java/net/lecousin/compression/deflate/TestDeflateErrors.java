package net.lecousin.compression.deflate;

import java.util.zip.Deflater;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.core.test.io.TestIOError;
import net.lecousin.framework.io.IO;

public class TestDeflateErrors extends TestIOError {

	@Override
	protected IO.Readable getReadable(IO.Readable io) throws Exception {
		return new DeflateReadable(io, Task.PRIORITY_NORMAL, false);
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
		return new DeflateWritable(io, Task.PRIORITY_NORMAL, Deflater.NO_COMPRESSION, false, 1);
	}

}
