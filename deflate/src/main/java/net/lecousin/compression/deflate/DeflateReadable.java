package net.lecousin.compression.deflate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import net.lecousin.framework.application.LCCore;
import net.lecousin.framework.concurrent.CancelException;
import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.TaskManager;
import net.lecousin.framework.concurrent.Threading;
import net.lecousin.framework.concurrent.synch.AsyncWork;
import net.lecousin.framework.concurrent.synch.ISynchronizationPoint;
import net.lecousin.framework.concurrent.synch.SynchronizationPoint;
import net.lecousin.framework.exception.NoException;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.IOUtil;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;
import net.lecousin.framework.util.RunnableWithParameter;

/**
 * Deflate decompression: wrap a Readable to uncompress it.
 * It uses the {@link Inflater} provided by Java.
 */
public class DeflateReadable extends ConcurrentCloseable implements IO.Readable {
	
	/** DeflateReadable with a known uncompressed size. */
	public static class SizeKnown extends DeflateReadable implements IO.KnownSize {
		/** Constructor. */
		public SizeKnown(IO.Readable input, byte priority, long uncompressedSize, boolean nowrap) {
			super(input, priority, nowrap);
			this.uncompressedSize = uncompressedSize;
		}

		private long uncompressedSize;
		
		@Override
		public AsyncWork<Long, IOException> getSizeAsync() {
			return new AsyncWork<>(Long.valueOf(uncompressedSize), null);
		}
		
		@Override
		public long getSizeSync() {
			return uncompressedSize;
		}
	}
	
	/** Constructor. */
	public DeflateReadable(IO.Readable input, byte priority, boolean nowrap) {
		inflater = new Inflater(nowrap);
		this.input = input;
		this.priority = priority;
	}
	
	private IO.Readable input;
	private byte priority;
	private Inflater inflater;
	private ByteBuffer readBuf = ByteBuffer.allocate(8192);
	private AsyncWork<Integer, IOException> readTask = null;
	private boolean reachEOF = false;

	@Override
	public TaskManager getTaskManager() {
		return Threading.getCPUTaskManager();
	}
	
	@Override
	public byte getPriority() { return priority; }
	
	@Override
	public void setPriority(byte priority) { this.priority = priority; }

	@Override
	public String getSourceDescription() { return "Deflate stream: " + (input != null ? input.getSourceDescription() : "closed"); }

	@Override
	public IO getWrappedIO() { return null; }

	@Override
	protected ISynchronizationPoint<?> closeUnderlyingResources() {
		return input.closeAsync();
	}
	
	@Override
	protected void closeResources(SynchronizationPoint<Exception> ondone) {
		input = null;
		readBuf = null;
		readTask = null;
		inflater.end();
		inflater = null;
		ondone.unblock();
	}
	
	@Override
	public ISynchronizationPoint<IOException> canStartReading() {
		return new SynchronizationPoint<>(true);
	}
	
	@Override
	public AsyncWork<Integer,IOException> readAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer,IOException>> ondone) {
		if (isClosing() || isClosed()) return new AsyncWork<>(null, null, new CancelException("Deflate stream closed"));
		if (reachEOF) {
			// TODO
			LCCore.getApplication().getDefaultLogger().debug("readAsync called with reachEOF = true");
			if (ondone != null) ondone.run(new Pair<>(Integer.valueOf(-1), null));
			return new AsyncWork<Integer,IOException>(Integer.valueOf(-1), null);
		}
		if (readTask != null && !readTask.isUnblocked()) {
			Task<Integer,IOException> task = new Task.Cpu<Integer,IOException>(
				"Waiting for previous uncompression task", priority, ondone
			) {
				@Override
				public Integer run() throws IOException {
					return Integer.valueOf(readBufferSync(buffer));
				}
			};
			readTask.listenAsync(task, false);
			readTask = operation(task).getOutput();
			return task.getOutput();
		}
		if (!inflater.needsInput()) {
			AsyncWork<Integer, IOException> result = new AsyncWork<>();
			Task<Void, NoException> inflate = new Task.Cpu<Void, NoException>(
				"Uncompressing zip: " + input.getSourceDescription(), priority
			) {
				@Override
				public Void run() {
					readBufferAsync(buffer, ondone, result);
					return null;
				}
			};
			inflate.start();
			readTask = operation(result);
			return result;
		}
		if (inflater.finished()) {
			// TODO
			long size = -1;
			if (DeflateReadable.this instanceof IO.KnownSize) try { size = ((IO.KnownSize)DeflateReadable.this).getSizeSync(); } catch (Throwable t) {}
			LCCore.getApplication().getDefaultLogger().debug("readAsync called, inflater finished with uncompressed bytes " + inflater.getBytesWritten() + " / " + size);
			reachEOF = true;
			if (ondone != null) ondone.run(new Pair<>(Integer.valueOf(-1), null));
			return new AsyncWork<Integer,IOException>(Integer.valueOf(-1), null);
		}
		AsyncWork<Integer, IOException> result = new AsyncWork<>();
		fillAsync(buffer, result, ondone);
		return readTask = operation(result);
	}
	
	@Override
	public int readSync(ByteBuffer buffer) throws IOException {
		if (readTask != null)
			try { readTask.blockThrow(0); }
			catch (CancelException cancel) { return -1; }
			catch (Exception err) { throw IO.error(err); }
		return readBufferSync(buffer);
	}
	
	private int readBufferSync(ByteBuffer buffer) throws IOException {
		if (reachEOF) {
			// TODO
			long size = -1;
			if (DeflateReadable.this instanceof IO.KnownSize) try { size = ((IO.KnownSize)DeflateReadable.this).getSizeSync(); } catch (Throwable t) {}
			LCCore.getApplication().getDefaultLogger().debug("readBufferSync called, inflater finished with uncompressed bytes " + inflater.getBytesWritten() + " / " + size);
			return -1;
		}
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
					// TODO
					long size = -1;
					if (DeflateReadable.this instanceof IO.KnownSize) try { size = ((IO.KnownSize)DeflateReadable.this).getSizeSync(); } catch (Throwable t) {}
					LCCore.getApplication().getDefaultLogger().debug("readBufferSync called, inflater finished = " + inflater.finished() + " needsDictionary = " + inflater.needsDictionary() + ", with uncompressed bytes " + inflater.getBytesWritten() + " / " + size);
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
		ByteBuffer buffer, RunnableWithParameter<Pair<Integer,IOException>> ondone, AsyncWork<Integer, IOException> result
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
						// TODO
						long size = -1;
						if (DeflateReadable.this instanceof IO.KnownSize) try { size = ((IO.KnownSize)DeflateReadable.this).getSizeSync(); } catch (Throwable t) {}
						LCCore.getApplication().getDefaultLogger().debug("readBufferAsync called, inflater finished = " + inflater.finished() + " needsDictionary = " + inflater.needsDictionary() + ", with uncompressed bytes " + inflater.getBytesWritten() + " / " + size);
						if (ondone != null) ondone.run(new Pair<>(Integer.valueOf(-1), null));
						result.unblockSuccess(Integer.valueOf(-1));
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
			Integer r = Integer.valueOf(total);
			if (ondone != null) ondone.run(new Pair<>(r, null));
			result.unblockSuccess(r);
		} catch (DataFormatException e) {
			IOException err = new IOException("Inflate error after " + inflater.getBytesRead()
				+ " compressed bytes read, and " + inflater.getBytesWritten() + " uncompressed bytes written", e);
			if (ondone != null) ondone.run(new Pair<>(null, err));
			result.error(err);
		}
	}
	
	private void fillSync() throws IOException {
		readBuf.clear();
		int len = input.readSync(readBuf);
		if (len <= 0)
			throw new IOException("Unexpected end of zip input");
		inflater.setInput(readBuf.array(), 0, len);
	}
	
	private void fillAsync(ByteBuffer buffer, AsyncWork<Integer, IOException> result, RunnableWithParameter<Pair<Integer,IOException>> ondone) {
		readBuf.clear();
		AsyncWork<Integer, IOException> read = input.readAsync(readBuf);
		Task<Void, NoException> inflate = new Task.Cpu<Void, NoException>(
			"Uncompressing zip: " + input.getSourceDescription(), priority
		) {
			@Override
			public Void run() {
				if (read.hasError()) {
					if (ondone != null) ondone.run(new Pair<>(null, read.getError()));
					result.error(read.getError());
					return null;
				}
				if (read.isCancelled()) {
					result.cancel(read.getCancelEvent());
					return null;
				}
				int len = read.getResult().intValue();
				if (len <= 0) {
					if (isClosing() || isClosed()) result.cancel(new CancelException("Deflate stream closed"));
					else {
						IOException err = new IOException("Unexpected end of zip input");
						if (ondone != null) ondone.run(new Pair<>(null, err));
						result.error(err);
					}
					return null;
				}
				inflater.setInput(readBuf.array(), 0, len);
				readBufferAsync(buffer, ondone, result);
				return null;
			}
		};
		inflate.startOn(read, true);
	}

	@Override
	public int readFullySync(ByteBuffer buffer) throws IOException {
		if (reachEOF) {
			// TODO
			long size = -1;
			if (DeflateReadable.this instanceof IO.KnownSize) try { size = ((IO.KnownSize)DeflateReadable.this).getSizeSync(); } catch (Throwable t) {}
			LCCore.getApplication().getDefaultLogger().debug("readFullySync called, inflater finished with uncompressed bytes " + inflater.getBytesWritten() + " / " + size);
			return -1;
		}
		if (readTask != null)
			try { readTask.blockThrow(0); }
			catch (CancelException e) { return -1; }
			catch (Exception e) { throw IO.error(e); }
		return IOUtil.readFully(this, buffer);
	}

	@Override
	public AsyncWork<Integer,IOException> readFullyAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer,IOException>> ondone) {
		return operation(IOUtil.readFullyAsync(this, buffer, ondone));
	}

	@Override
	public long skipSync(long n) throws IOException {
		return IOUtil.skipSyncByReading(this, n);
	}

	@Override
	public AsyncWork<Long,IOException> skipAsync(long n, RunnableWithParameter<Pair<Long,IOException>> ondone) {
		return operation(IOUtil.skipAsyncByReading(this, n, ondone));
	}
	
}
