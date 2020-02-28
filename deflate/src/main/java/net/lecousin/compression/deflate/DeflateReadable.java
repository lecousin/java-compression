package net.lecousin.compression.deflate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import net.lecousin.framework.concurrent.CancelException;
import net.lecousin.framework.concurrent.Executable;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.concurrent.threads.Task.Priority;
import net.lecousin.framework.concurrent.threads.TaskManager;
import net.lecousin.framework.concurrent.threads.Threading;
import net.lecousin.framework.exception.NoException;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.IOUtil;
import net.lecousin.framework.memory.ByteArrayCache;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;

/**
 * Deflate decompression: wrap a Readable to uncompress it.
 * It uses the {@link Inflater} provided by Java.
 */
public class DeflateReadable extends ConcurrentCloseable<IOException> implements IO.Readable {
	
	/** DeflateReadable with a known uncompressed size. */
	public static class SizeKnown extends DeflateReadable implements IO.KnownSize {
		/** Constructor. */
		public SizeKnown(IO.Readable input, Priority priority, long uncompressedSize, boolean nowrap, int bufferSize) {
			super(input, priority, nowrap, bufferSize);
			this.uncompressedSize = uncompressedSize;
		}

		private long uncompressedSize;
		
		@Override
		public AsyncSupplier<Long, IOException> getSizeAsync() {
			return new AsyncSupplier<>(Long.valueOf(uncompressedSize), null);
		}
		
		@Override
		public long getSizeSync() {
			return uncompressedSize;
		}
	}
	
	/** Constructor. */
	public DeflateReadable(IO.Readable input, Priority priority, boolean nowrap, int bufferSize) {
		inflater = new Inflater(nowrap);
		this.input = input;
		this.priority = priority;
		readBuf = ByteBuffer.wrap(ByteArrayCache.getInstance().get(bufferSize, true));
	}
	
	private IO.Readable input;
	private Priority priority;
	private Inflater inflater;
	private ByteBuffer readBuf;
	private boolean reachEOF = false;

	@Override
	public TaskManager getTaskManager() {
		return Threading.getCPUTaskManager();
	}
	
	@Override
	public Priority getPriority() { return priority; }
	
	@Override
	public void setPriority(Priority priority) { this.priority = priority; }

	@Override
	public String getSourceDescription() { return "Deflate stream: " + (input != null ? input.getSourceDescription() : "closed"); }

	@Override
	public IO getWrappedIO() { return null; }

	@Override
	protected IAsync<IOException> closeUnderlyingResources() {
		return input.closeAsync();
	}
	
	@Override
	protected void closeResources(Async<IOException> ondone) {
		input = null;
		ByteArrayCache.getInstance().free(readBuf);
		inflater.end();
		inflater = null;
		ondone.unblock();
	}
	
	@Override
	public IAsync<IOException> canStartReading() {
		return new Async<>(true);
	}
	
	@Override
	@SuppressWarnings("java:S1604")
	public AsyncSupplier<Integer,IOException> readAsync(ByteBuffer buffer, Consumer<Pair<Integer,IOException>> ondone) {
		if (isClosing() || isClosed()) return new AsyncSupplier<>(null, null, new CancelException("Deflate stream closed"));
		if (reachEOF)
			return IOUtil.success(Integer.valueOf(-1), ondone);
		if (!inflater.needsInput()) {
			AsyncSupplier<Integer, IOException> result = new AsyncSupplier<>();
			Task.cpu("Uncompressing zip: " + input.getSourceDescription(), priority, new Executable<Void, NoException>() {
				@Override
				public Void execute() {
					readBufferAsync(buffer, ondone, result);
					return null;
				}
			}).start();
			return operation(result);
		}
		if (inflater.finished()) {
			reachEOF = true;
			return IOUtil.success(Integer.valueOf(-1), ondone);
		}
		AsyncSupplier<Integer, IOException> result = new AsyncSupplier<>();
		fillAsync(buffer, result, ondone);
		return operation(result);
	}
	
	@Override
	public int readSync(ByteBuffer buffer) throws IOException {
		if (isClosing() || isClosed()) throw new IOException("Deflate stream closed");
		return readBufferSync(buffer);
	}
	
	private int readBufferSync(ByteBuffer buffer) throws IOException {
		if (reachEOF) return -1;
		byte[] b;
		int off;
		if (buffer.hasArray()) {
			b = buffer.array();
			off = buffer.arrayOffset() + buffer.position();
		} else {
			b = new byte[buffer.remaining()];
			off = 0;
		}
		try {
			int n;
			while ((n = inflater.inflate(b, off, buffer.remaining())) == 0) {
				if (inflater.finished() || inflater.needsDictionary()) {
					reachEOF = true;
					return -1;
				}
				if (inflater.needsInput()) fillSync();
			}
			if (!buffer.hasArray())
				buffer.put(b, 0, n);
			else
				buffer.position(off + n - buffer.arrayOffset());
			return n;
		} catch (DataFormatException e) {
			throw new IOException("Inflate error after " + inflater.getBytesRead()
				+ " compressed bytes read, and " + inflater.getBytesWritten() + " uncompressed bytes written", e);
		}
	}
	
	private void readBufferAsync(
		ByteBuffer buffer, Consumer<Pair<Integer,IOException>> ondone, AsyncSupplier<Integer, IOException> result
	) {
		byte[] b;
		int off;
		if (buffer.hasArray()) {
			b = buffer.array();
			off = buffer.arrayOffset() + buffer.position();
		} else {
			b = new byte[buffer.remaining()];
			off = 0;
		}
		try {
			int n;
			int total = 0;
			do {
				while ((n = inflater.inflate(b, off + total, buffer.remaining() - total)) == 0) {
					if (total > 0) break;
					if (inflater.finished() || inflater.needsDictionary()) {
						reachEOF = true;
						IOUtil.success(Integer.valueOf(-1), result, ondone);
						return;
					}
					if (isClosing() || isClosed()) {
						IOUtil.error(new IOException("Deflate stream closed"), result, ondone);
						return;
					}
					if (inflater.needsInput()) {
						fillAsync(buffer, result, ondone);
						return;
					}
				}
				total += n;
			} while (n > 0 && total < buffer.remaining() && !inflater.needsInput());
			if (!buffer.hasArray())
				buffer.put(b, 0, total);
			else
				buffer.position(off + total - buffer.arrayOffset());
			IOUtil.success(Integer.valueOf(total), result, ondone);
		} catch (DataFormatException e) {
			IOUtil.error(new IOException("Inflate error after " + inflater.getBytesRead() + " compressed bytes read, and "
				+ inflater.getBytesWritten() + " uncompressed bytes written", e), result, ondone);
		}
	}
	
	private void fillSync() throws IOException {
		if (isClosing() || isClosed()) throw new IOException("Deflate stream closed");
		readBuf.clear();
		int len = input.readSync(readBuf);
		if (len <= 0)
			throw new IOException("Unexpected end of zip input");
		inflater.setInput(readBuf.array(), 0, len);
	}
	
	@SuppressWarnings("java:S1604")
	private void fillAsync(ByteBuffer buffer, AsyncSupplier<Integer, IOException> result, Consumer<Pair<Integer,IOException>> ondone) {
		readBuf.clear();
		AsyncSupplier<Integer, IOException> read = input.readAsync(readBuf);
		Task.cpu("Uncompressing zip: " + input.getSourceDescription(), priority, new Executable<Void, NoException>() {
			@Override
			public Void execute() {
				if (!read.isSuccessful()) {
					IOUtil.notSuccess(read, result, ondone);
					return null;
				}
				int len = read.getResult().intValue();
				if (len <= 0) {
					if (isClosing() || isClosed()) result.cancel(new CancelException("Deflate stream closed"));
					else IOUtil.error(new IOException("Unexpected end of zip input"), result, ondone);
					return null;
				}
				inflater.setInput(readBuf.array(), 0, len);
				readBufferAsync(buffer, ondone, result);
				return null;
			}
		}).startOn(read, true);
	}

	@Override
	public int readFullySync(ByteBuffer buffer) throws IOException {
		if (reachEOF) return -1;
		return IOUtil.readFully(this, buffer);
	}

	@Override
	public AsyncSupplier<Integer,IOException> readFullyAsync(ByteBuffer buffer, Consumer<Pair<Integer,IOException>> ondone) {
		return operation(IOUtil.readFullyAsync(this, buffer, ondone));
	}

	@Override
	public long skipSync(long n) throws IOException {
		return IOUtil.skipSyncByReading(this, n);
	}

	@Override
	public AsyncSupplier<Long,IOException> skipAsync(long n, Consumer<Pair<Long,IOException>> ondone) {
		return operation(IOUtil.skipAsyncByReading(this, n, ondone));
	}
	
}
