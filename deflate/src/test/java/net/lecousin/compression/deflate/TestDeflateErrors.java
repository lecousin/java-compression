package net.lecousin.compression.deflate;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.core.test.io.TestIOError;
import net.lecousin.framework.io.FileIO;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.buffering.ByteArrayIO;
import net.lecousin.framework.io.util.EmptyReadable;

import org.junit.Test;

public class TestDeflateErrors extends TestIOError {

	@Override
	protected IO.Readable getReadable(IO.Readable io) throws Exception {
		return new DeflateReadable(io, Task.Priority.NORMAL, false, 1024);
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
		return new DeflateWritable(io, Task.Priority.NORMAL, Deflater.NO_COMPRESSION, false, 1);
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testEmptyReadable() throws Exception {
		DeflateReadable in = new DeflateReadable(new EmptyReadable("empty zip", Task.Priority.NORMAL), Task.Priority.NORMAL, false, 1024);
		try {
			in.readSync(ByteBuffer.allocate(16));
			throw new AssertionError();
		} catch (IOException e) {
			// ok
		}
		in.close();

		in = new DeflateReadable(new EmptyReadable("empty zip", Task.Priority.NORMAL), Task.Priority.NORMAL, false, 1024);
		try {
			in.readAsync(ByteBuffer.allocate(16)).blockResult(0);
			throw new AssertionError();
		} catch (IOException e) {
			// ok
		}
		in.close();
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testInvalidZip() throws Exception {
		DeflateReadable in = new DeflateReadable(new ByteArrayIO(new byte[100], "invalid zip"), Task.Priority.NORMAL, false, 1024);
		try {
			in.readSync(ByteBuffer.allocate(16));
			throw new AssertionError();
		} catch (IOException e) {
			// ok
		}
		in.close();

		in = new DeflateReadable(new ByteArrayIO(new byte[100], "invalid zip"), Task.Priority.NORMAL, false, 1024);
		try {
			in.readAsync(ByteBuffer.allocate(16)).blockResult(0);
			throw new AssertionError();
		} catch (IOException e) {
			// ok
		}
		in.close();
	}

	
	@Test
	public void testCompessorReadError() throws Exception {
		File tmp = File.createTempFile("test", "deflate_compressor");
		tmp.deleteOnExit();
		try (FileIO.WriteOnly fout = new FileIO.WriteOnly(tmp, Task.Priority.NORMAL)) {
			DeflateCompressor compressor = new DeflateCompressor(false);
			compressor.compress(new TestIOError.ReadableAlwaysError(), fout, 4096, 10, Task.Priority.NORMAL).blockThrow(0);
			throw new AssertionError();
		} catch (IOException e) {
			// ok
		}
	}
	
	@Test
	public void testCompessorWriteError() throws Exception {
		try {
			DeflateCompressor compressor = new DeflateCompressor(false);
			compressor.compress(new ByteArrayIO(new byte[100], "invalid zip"), new TestIOError.WritableAlwaysError(), 4096, 10, Task.Priority.NORMAL).blockThrow(0);
			throw new AssertionError();
		} catch (IOException e) {
			// ok
		}
	}
	
}
