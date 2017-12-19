package net.lecousin.compression.deflate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

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
		getInflater = InflaterCache.get(nowrap);
		this.input = input;
		this.priority = priority;
		this.nowrap = nowrap;
	}
	
	private IO.Readable input;
	private byte priority;
	private boolean nowrap;
	private AsyncWork<Inflater,NoException> getInflater;
	private ByteBuffer readBuf = ByteBuffer.allocate(8192);
	private Task<Integer,IOException> readTask = null;
	private boolean reachEOF = false;
	private ISynchronizationPoint<IOException> closing = null;

	@Override
	public TaskManager getTaskManager() {
		return Threading.getCPUTaskManager();
	}
	
	@Override
	public byte getPriority() { return priority; }
	
	@Override
	public void setPriority(byte priority) { this.priority = priority; }

	@Override
	public String getSourceDescription() { return "Deflate stream: " + input.getSourceDescription(); }

	@Override
	public IO getWrappedIO() { return null; }

	@Override
	protected ISynchronizationPoint<IOException> closeIO() {
		if (closing != null) return closing;
		if (!getInflater.isUnblocked()) {
			SynchronizationPoint<IOException> sp = new SynchronizationPoint<>();
			closing = sp;
			getInflater.listenInline(() -> {
				InflaterCache.free(getInflater.getResult(), nowrap);
				input.closeAsync().listenInline(sp);
			});
			return sp;
		}
		// do not end, because this closes it definitely and the cache wants to reuse it
		// getInflater.getResult().end();
		InflaterCache.free(getInflater.getResult(), nowrap);
		return closing = input.closeAsync();
	}
	
	@Override
	public ISynchronizationPoint<IOException> canStartReading() {
		SynchronizationPoint<IOException> sp = new SynchronizationPoint<>();
		getInflater.synchWithNoError(sp);
		return sp;
	}
	
	@Override
	public AsyncWork<Integer,IOException> readAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer,IOException>> ondone) {
		if (!getInflater.isUnblocked()) {
			AsyncWork<Integer,IOException> res = new AsyncWork<Integer,IOException>();
			getInflater.listenInline(() -> { readAsync(buffer, ondone).listenInline(res); });
			return res;
		}
		if (reachEOF) {
			if (ondone != null) ondone.run(new Pair<>(Integer.valueOf(-1), null));
			return new AsyncWork<Integer,IOException>(Integer.valueOf(-1), null);
		}
		if (readTask != null && !readTask.isDone()) {
			Task<Integer,IOException> task = new Task.Cpu<Integer,IOException>(
				"Waiting for previous uncompression task", Task.PRIORITY_IMPORTANT, ondone
			) {
				@Override
				public Integer run() throws IOException {
					return Integer.valueOf(readBufferSync(buffer));
				}
			};
			readTask.ondone(task, false);
			readTask = task;
			return task.getOutput();
		}
		if (!getInflater.getResult().needsInput()) {
			Task<Integer,IOException> inflate = new Task.Cpu<Integer,IOException>(
				"Uncompressing zip: " + input.getSourceDescription(), priority, ondone
			) {
				@Override
				public Integer run() throws IOException {
					byte[] b;
					int off;
					if (buffer.hasArray()) {
						b = buffer.array();
						off = buffer.position();
					} else {
						b = new byte[buffer.remaining()];
						off = 0;
					}
					Inflater inflater = getInflater.getResult();
					try {
						int n;
						int total = 0;
						do {
							while ((n = inflater.inflate(b, off + total, buffer.remaining() - total)) == 0) {
								if (total > 0) break;
				                if (inflater.finished() || inflater.needsDictionary()) {
				                    reachEOF = true;
			                    	return Integer.valueOf(-1);
				                }
				                if (inflater.needsInput())
									return Integer.valueOf(readBufferSync(buffer));
				            }
							total += n;
						} while (n > 0 && total < buffer.remaining() && !inflater.needsInput());
						if (!buffer.hasArray())
							buffer.put(b, 0, total);
						else
							buffer.position(off + total);
						return Integer.valueOf(total);
					} catch (DataFormatException e) {
						throw new IOException(e);
					}
				}
			};
			inflate.start();
			readTask = inflate;
			return inflate.getOutput();
		}
		if (getInflater.getResult().finished()) {
			reachEOF = true;
			if (ondone != null) ondone.run(new Pair<>(Integer.valueOf(-1), null));
			return new AsyncWork<Integer,IOException>(Integer.valueOf(-1), null);
		}
		readBuf.clear();
		AsyncWork<Integer,IOException> read = input.readAsync(readBuf);
		Task<Integer,IOException> inflate = new Task.Cpu<Integer,IOException>(
			"Uncompressing zip: " + input.getSourceDescription(), priority, ondone
		) {
			@Override
			public Integer run() throws IOException {
				if (!read.isSuccessful())
					throw read.getError();
				int len = read.getResult().intValue();
				if (len <= 0)
					throw new IOException("Unexpected end of zip input");
				Inflater inflater = getInflater.getResult();
				inflater.setInput(readBuf.array(), 0, len);
				byte[] b;
				int off;
				if (buffer.hasArray()) {
					b = buffer.array();
					off = buffer.position();
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
		                    	return Integer.valueOf(-1);
			                }
			                if (inflater.needsInput())
								return Integer.valueOf(readBufferSync(buffer));
			            }
						total += n;
					} while (n > 0 && !inflater.needsInput());
					if (!buffer.hasArray())
						buffer.put(b, 0, total);
					else
						buffer.position(off + total);
					return Integer.valueOf(total);
				} catch (DataFormatException e) {
					throw new IOException("Inflate error after " + inflater.getBytesRead()
						+ " compressed bytes read, and " + inflater.getBytesWritten() + " uncompressed bytes written", e);
				}
			}
		};
		inflate.startOn(read, true);
		readTask = inflate;
		return inflate.getOutput();
	}
	
	@Override
	public int readSync(ByteBuffer buffer) throws IOException {
		if (!getInflater.isUnblocked()) getInflater.block(0);
		if (readTask != null && !readTask.isDone())
			readTask.getOutput().blockException(0);
		return readBufferSync(buffer);
	}
	
	private int readBufferSync(ByteBuffer buffer) throws IOException {
		if (reachEOF) return -1;
		byte[] b;
		int off;
		if (buffer.hasArray()) {
			b = buffer.array();
			off = buffer.position();
		} else {
			b = new byte[buffer.remaining()];
			off = 0;
		}
		Inflater inflater = getInflater.getResult();
		try {
			int n;
			while ((n = inflater.inflate(b, off, buffer.remaining())) == 0) {
                if (inflater.finished() || inflater.needsDictionary()) {
                    reachEOF = true;
                    return -1;
                }
                if (inflater.needsInput()) fill();
            }
			if (!buffer.hasArray())
				buffer.put(b, 0, n);
			else
				buffer.position(off + n);
			return n;
		} catch (DataFormatException e) {
			throw new IOException(e);
		}
	}
	
	private void fill() throws IOException {
		readBuf.clear();
		int len = input.readSync(readBuf);
		if (len <= 0)
			throw new IOException("Unexpected end of zip input");
		getInflater.getResult().setInput(readBuf.array(), 0, len);
	}

	@Override
	public int readFullySync(ByteBuffer buffer) throws IOException {
		if (!getInflater.isUnblocked()) getInflater.block(0);
		if (reachEOF) return -1;
		if (readTask != null && !readTask.isDone())
			readTask.getOutput().blockException(0);
		return IOUtil.readFully(this, buffer);
	}

	@Override
	public AsyncWork<Integer,IOException> readFullyAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer,IOException>> ondone) {
		if (!getInflater.isUnblocked()) {
			AsyncWork<Integer,IOException> res = new AsyncWork<Integer,IOException>();
			getInflater.listenInline(new Runnable() {
				@Override
				public void run() {
					readFullyAsync(buffer, ondone).listenInline(res);
				}
			});
			return res;
		}
		return IOUtil.readFullyAsync(this, buffer, ondone);
	}

	@Override
	public long skipSync(long n) throws IOException {
		if (!getInflater.isUnblocked()) getInflater.block(0);
		return IOUtil.skipSyncByReading(this, n);
	}

	@Override
	public AsyncWork<Long,IOException> skipAsync(long n, RunnableWithParameter<Pair<Long,IOException>> ondone) {
		if (!getInflater.isUnblocked()) {
			AsyncWork<Long,IOException> res = new AsyncWork<Long,IOException>();
			getInflater.listenInline(new Runnable() {
				@Override
				public void run() {
					skipAsync(n, ondone).listenInline(res);
				}
			});
			return res;
		}
		return IOUtil.skipAsyncByReading(this, n, ondone);
	}
	
}
