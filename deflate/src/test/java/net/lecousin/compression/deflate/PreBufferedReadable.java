package net.lecousin.compression.deflate;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.lecousin.framework.collections.TurnArray;
import net.lecousin.framework.concurrent.CancelException;
import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.TaskManager;
import net.lecousin.framework.concurrent.Threading;
import net.lecousin.framework.concurrent.synch.AsyncWork;
import net.lecousin.framework.concurrent.synch.ISynchronizationPoint;
import net.lecousin.framework.concurrent.synch.JoinPoint;
import net.lecousin.framework.concurrent.synch.SynchronizationPoint;
import net.lecousin.framework.exception.NoException;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.IOUtil;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;
import net.lecousin.framework.util.RunnableWithParameter;

/**
 * Often the best implementation when reading sequentially (streaming, without seek): it fills a first buffer
 * as soon as instantiated, once the first buffer is filled new buffers are pre-filled.
 * <br/>
 * We can specify the maximum number of buffers to be pre-filled to limit memory usage.
 * Once a buffer is completely read, it becomes available to be pre-filled again with new data.
 * <br/>
 * The goal of this implementation is to fill buffers while data is computed, reducing the time waiting for
 * read operations (ideally not blocking if buffers are filled fats enough compare to the data computation).
 */
public class PreBufferedReadable extends ConcurrentCloseable implements IO.Readable.Buffered {

	/** Constructor. */
	public PreBufferedReadable(
		IO.Readable src, int firstBuffer, byte firstBufferPriority, int nextBuffer, byte nextBufferPriority, int maxNbNextBuffersReady
	) {
		this.src = src;
		this.priority = firstBufferPriority;
		if (src instanceof IO.KnownSize) {
			AsyncWork<Long,IOException> getSize = ((IO.KnownSize)src).getSizeAsync();
			Task<Void,NoException> start = new Task.Cpu<Void,NoException>(
				"Start PreBufferedReadable after size is known", firstBufferPriority
			) {
				@Override
				public Void run() {
					if (getSize.hasError()) {
						PreBufferedReadable.this.error = getSize.getError();
						synchronized (PreBufferedReadable.this) {
							if (dataReady != null) {
								dataReady.unblock();
								dataReady = null;
							}
						}
						return null;
					}
					if (getSize.isCancelled()) return null;
					size = getSize.getResult().longValue();
					startWithKnownSize(firstBuffer, firstBufferPriority, nextBuffer, nextBufferPriority, maxNbNextBuffersReady);
					return null;
				}
			};
			operation(start).startOn(getSize, true);
		} else {
			start(firstBuffer, firstBufferPriority, nextBuffer, nextBufferPriority, maxNbNextBuffersReady);
		}
	}
	
	/** Constructor. */
	public <T extends IO.Readable.Seekable & IO.KnownSize> PreBufferedReadable(
		T src, int firstBuffer, byte firstBufferPriority, int nextBuffer, byte nextBufferPriority, int maxNbNextBuffersReady
	) throws IOException {
		this.src = src;
		this.priority = firstBufferPriority;
		this.read = src.getPosition();
		this.size = src.getSizeSync();
		startWithKnownSize(firstBuffer, firstBufferPriority, nextBuffer, nextBufferPriority, maxNbNextBuffersReady);
	}
	
	/** Constructor. */
	public PreBufferedReadable(
		IO.Readable src, long size, int firstBuffer, byte firstBufferPriority, int nextBuffer, byte nextBufferPriority,
		int maxNbNextBuffersReady
	) {
		this.src = src;
		this.priority = firstBufferPriority;
		this.size = size;
		startWithKnownSize(firstBuffer, firstBufferPriority, nextBuffer, nextBufferPriority, maxNbNextBuffersReady);
	}
	
	/** Constructor. */
	public <T extends IO.Readable.Seekable & IO.KnownSize> PreBufferedReadable(
		T src, long size, int firstBuffer, byte firstBufferPriority, int nextBuffer, byte nextBufferPriority, int maxNbNextBuffersReady
	) throws IOException {
		this.src = src;
		this.priority = firstBufferPriority;
		this.read = src.getPosition();
		this.size = size;
		startWithKnownSize(firstBuffer, firstBufferPriority, nextBuffer, nextBufferPriority, maxNbNextBuffersReady);
	}
	
	private void startWithKnownSize(
		int firstBuffer, byte firstBufferPriority, int nextBuffer, byte nextBufferPriority, int maxNbNextBuffersReady
	) {
		if (size == read) {
			synchronized (PreBufferedReadable.this) {
				endReached = true;
				if (dataReady != null) {
					dataReady.unblock();
					dataReady = null;
				}
			}
		} else if (size - read <= firstBuffer)
			start((int)(size - read), firstBufferPriority, 0, (byte)0, 0);
		else if (size - read <= firstBuffer + nextBuffer)
			start(firstBuffer, firstBufferPriority, (int)(size - read - firstBuffer), nextBufferPriority, 1);
		else
			start(firstBuffer, firstBufferPriority, nextBuffer, nextBufferPriority, maxNbNextBuffersReady);
	}
	
	private IO.Readable src;
	private byte priority;
	private IOException error = null;
	private long size = -1;
	private long read = 0;
	private boolean endReached = false;
	private boolean stopReading = false;
	private ByteBuffer current = null;
	private boolean currentIsFirst = true;
	private SynchronizationPoint<NoException> dataReady = null;
	private TurnArray<ByteBuffer> buffersReady;
	private TurnArray<ByteBuffer> reusableBuffers;
	private AsyncWork<?,?> nextReadTask = null;
	private int maxBufferedSize;
	
	@Override
	public String getSourceDescription() { return src != null ? src.getSourceDescription() : "closed"; }
	
	/** Return the next synchronization point that will be unblocked once data is ready to be read. */
	public SynchronizationPoint<IOException> getDataReadySynchronization() {
		synchronized (this) {
			SynchronizationPoint<IOException> sp = new SynchronizationPoint<>();
			if (error != null) {
				sp.error(error);
				return sp;
			} else if (current != null || endReached) {
				sp.unblock();
				return sp;
			}
			if (isClosing() || isClosed()) {
				sp.cancel(new CancelException("IO closed"));
				return sp;
			}
			if (dataReady == null)
				dataReady = new SynchronizationPoint<>();
			dataReady.listenInline(new Runnable() {
				@Override
				public void run() {
					if (error != null)
						sp.error(error);
					else
						sp.unblock();
				}
			});
			return sp;
		}
	}
	
	@Override
	public ISynchronizationPoint<IOException> canStartReading() {
		return getDataReadySynchronization();
	}
	
	@Override
	public IO getWrappedIO() {
		return src;
	}
	
	@Override
	public TaskManager getTaskManager() {
		// should be buffered...
		return Threading.getCPUTaskManager();
	}
	
	@Override
	protected ISynchronizationPoint<?> closeUnderlyingResources() {
		AsyncWork<?,?> nextRead;
		synchronized (this) {
			nextRead = nextReadTask;
		}
		if (nextRead != null && !nextRead.isUnblocked())
			nextRead.cancel(new CancelException("IO closed"));
		while (dataReady != null) {
			SynchronizationPoint<NoException> dr;
			synchronized (this) {
				dr = dataReady;
				dataReady = null;
			}
			if (dr != null)
				dr.unblock();
		}
		return src.closeAsync();
	}
	
	@Override
	protected void closeResources(SynchronizationPoint<Exception> ondone) {
		SynchronizationPoint<NoException> dr = null;
		synchronized (this) {
			endReached = true;
			if (dataReady != null) {
				dr = dataReady;
				dataReady = null;
			}
			buffersReady = null;
			reusableBuffers = null;
			nextReadTask = null;
		}
		if (dr != null)
			dr.unblock();
		src = null;
		ondone.unblock();
	}
	
	@Override
	public byte getPriority() { return priority; }
	
	@Override
	public void setPriority(byte priority) { this.priority = priority; }
	
	@SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
	private void start(int firstBuffer, byte firstBufferPriority, int nextBuffer, byte nextBufferPriority, int maxNbNextBuffersReady) {
		if (nextBuffer < 0)
			throw new IllegalArgumentException("next buffer size must be positive, or zero to disable it, given: " + nextBuffer);
		if (maxNbNextBuffersReady < 0)
			throw new IllegalArgumentException(
				"maximum number of next buffers must be positive, or zero to disable it, given: " + maxNbNextBuffersReady);
		if (nextBuffer == 0) maxNbNextBuffersReady = 0;
		if (maxNbNextBuffersReady == 0) nextBuffer = 0;
		maxBufferedSize = maxNbNextBuffersReady * nextBuffer;
		if (maxBufferedSize == 0) maxBufferedSize = firstBuffer;
		buffersReady = new TurnArray<>(maxNbNextBuffersReady + 1);
		if (maxNbNextBuffersReady > 0)
			reusableBuffers = new TurnArray<>(maxNbNextBuffersReady);
		// first read
		ByteBuffer buffer = ByteBuffer.allocate(firstBuffer);
		src.setPriority(firstBufferPriority);
		JoinPoint<NoException> jpNextRead = new JoinPoint<>();
		jpNextRead.addToJoin(1);
		AsyncWork<Integer,IOException> firstReadTask;
		if (nextBuffer > 0)
			firstReadTask = operation(src.readAsync(buffer));
		else
			firstReadTask = operation(src.readFullyAsync(buffer));
		boolean singleRead = nextBuffer <= 0;
		firstReadTask.listenInline(new Runnable() {
			@Override
			public void run() {
				if (buffersReady == null) {
					jpNextRead.joined();
					return;
				}
				buffer.flip();
				synchronized (PreBufferedReadable.this) {
					if (firstReadTask.isCancelled()) {
						if (dataReady != null) {
							SynchronizationPoint<NoException> dr = dataReady;
							dataReady = null;
							dr.unblock();
						}
						jpNextRead.joined();
						return;
					}
					Throwable e = firstReadTask.getError();
					if (singleRead && e == null && firstReadTask.getResult().intValue() < size)
						e = new IOException("Only " + firstReadTask.getResult().intValue()
							+ " bytes read, expected is " + size);
					if (e != null) {
						if (e instanceof IOException) error = (IOException)e;
						else error = new IOException("Read failed", e);
						if (dataReady != null) {
							SynchronizationPoint<NoException> dr = dataReady;
							dataReady = null;
							dr.unblock();
						}
					} else {
						if (buffer.remaining() == 0)
							endReached = true;
						else
							current = buffer;
						read += buffer.remaining();
						if (size > 0 && read == size) 
							endReached = true;
						if (endReached && size > 0 && read < size) {
							error = new IOException("Unexpected end after " + read + " bytes read, known size is " + size);
						}
						if (dataReady != null) {
							SynchronizationPoint<NoException> dr = dataReady;
							dataReady = null;
							dr.unblock();
						}
					}
				}
				if (buffersReady == null) {
					jpNextRead.joined();
					return;
				}
				src.setPriority(nextBufferPriority);
				jpNextRead.joined();
			}
		});
		if (nextBuffer > 0) {
			// prepare buffers
			final int nextBufferSize = nextBuffer;
			final int nbNext = maxNbNextBuffersReady;
			jpNextRead.addToJoin(1);
			Task<Void,NoException> prepare = new Task.Cpu<Void,NoException>(
				"Allocate buffers for pre-buffered IO " + getSourceDescription(), nextBufferPriority
			) {
				@Override
				public Void run() {
					TurnArray<ByteBuffer> buffers = reusableBuffers;
					if (buffers == null) {
						jpNextRead.joined();
						return null; // already closed
					}
					for (int i = 0; i < nbNext; ++i) {
						ByteBuffer b = ByteBuffer.allocate(nextBufferSize);
						buffers.addLast(b);
					}
					jpNextRead.joined();
					return null;
				}
			};
			Task<Void,NoException> task = new Task.Cpu<Void,NoException>(
				"First next read of pre-buffered IO " + getSourceDescription(), nextBufferPriority
			) {
				@Override
				public Void run() {
					SynchronizationPoint<NoException> dr = null;
					synchronized (PreBufferedReadable.this) {
						nextReadTask = null;
						if (error == null && !endReached && !stopReading && !isClosing() && !isClosed())
							nextRead();
						else if (dataReady != null) {
							dr = dataReady;
							dataReady = null;
						}
					}
					if (dr != null) dr.unblock();
					return null;
				}
			};
			operation(prepare).start();
			nextReadTask = operation(task.getOutput());
			jpNextRead.start();
			jpNextRead.listenAsync(task, true);
		}
	}
	
	@Override
	public int readSync(ByteBuffer buffer) throws IOException {
		do {
			SynchronizationPoint<NoException> sp;
			synchronized (this) {
				if (error != null) throw error;
				if (current != null) break;
				if (endReached) return -1;
				if (isClosing() || isClosed()) return -1;
				if (dataReady == null) dataReady = new SynchronizationPoint<>();
				sp = dataReady;
			}
			sp.block(0);
		} while (true);
		if (buffersReady == null) throw new IOException("IO Closed");
		int nb = buffer.remaining();
		if (current.remaining() > nb) {
			int limit = current.limit();
			current.limit(limit - (current.remaining() - nb));
			buffer.put(current);
			current.limit(limit);
			return nb;
		}
		nb = current.remaining();
		buffer.put(current);
		moveNextBuffer(true);
		return nb;
	}
	
	@Override
	public int readAsync() throws IOException {
		synchronized (this) {
			if (error != null) throw error;
			if (current != null) {
				if (!current.hasRemaining() && endReached)
					return -1;
			} else {
				if (endReached) return -1;
				if (isClosing() || isClosed()) return -1;
				if (dataReady == null) dataReady = new SynchronizationPoint<>();
				if (!dataReady.isUnblocked())
					return -2;
			}
		}
		int res = current.get() & 0xFF;
		if (!current.hasRemaining())
			moveNextBuffer(true);
		return res;
	}

	@Override
	public AsyncWork<Integer,IOException> readAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer,IOException>> ondone) {
		SynchronizationPoint<NoException> sp = null;
		synchronized (this) {
			if (error != null) {
				if (ondone != null) ondone.run(new Pair<>(null, error));
				return new AsyncWork<Integer,IOException>(null, error);
			}
			if (current == null) {
				if (endReached) {
					if (ondone != null) ondone.run(new Pair<>(Integer.valueOf(-1), null));
					return new AsyncWork<Integer,IOException>(Integer.valueOf(-1), null);
				}
				if (isClosing() || isClosed()) return new AsyncWork<>(null, null, new CancelException("IO closed"));
				if (dataReady == null) dataReady = new SynchronizationPoint<>();
				sp = dataReady;
			}
		}
		Task<Integer,IOException> t = new Task.Cpu<Integer,IOException>(
			"Async read on pre-buffered IO " + getSourceDescription(), getPriority(), ondone
		) {
			@Override
			public Integer run() throws IOException, CancelException {
				if (error != null) throw error;
				if (buffersReady == null) {
					if (endReached) return Integer.valueOf(-1); // case of empty readable
					throw new CancelException("IO Closed");
				}
				if (current == null) {
					if (endReached) return Integer.valueOf(-1);
					if (isClosing() || isClosed()) throw new CancelException("IO Closed");
					throw new IOException("Unexpected error: current buffer is null but end is not reached");
				}
				int nb = buffer.remaining();
				if (current.remaining() > nb) {
					int limit = current.limit();
					current.limit(limit - (current.remaining() - nb));
					buffer.put(current);
					current.limit(limit);
					return Integer.valueOf(nb);
				}
				nb = current.remaining();
				buffer.put(current);
				moveNextBuffer(true);
				return Integer.valueOf(nb);
			}
		};
		operation(t);
		if (sp == null) {
			t.start();
			return t.getOutput();
		}
		t.startOn(sp, false);
		return t.getOutput();
	}
	
	@Override
	public int readFullySync(ByteBuffer buffer) throws IOException {
		return IOUtil.readFully(this, buffer);
	}
	
	@Override
	public AsyncWork<Integer,IOException> readFullyAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer,IOException>> ondone) {
		return operation(IOUtil.readFullyAsync(this, buffer, ondone));
	}
	
	@Override
	public long skipSync(long n) throws IOException {
		long skipped = 0;
		do {
			SynchronizationPoint<NoException> sp = null;
			long remaining = -1;
			synchronized (this) {
				if (error != null) throw error;
				if (n <= 0) {
					stopReading = false;
					return 0;
				}
				if (current != null) {
					int nb = current.remaining();
					if (nb > n) {
						stopReading = false;
						current.position(current.position() + ((int)n));
						return n + skipped;
					}
					if (nb == n) {
						stopReading = false;
						moveNextBuffer(true);
						return n + skipped;
					}
					skipped += nb;
					if (reusableBuffers == null) {
						// we reach the end
						endReached = true;
						if (size > 0 && read < size)
							error = new IOException("Unexpected end after " + read + " bytes read, known size is " + size);
						if (dataReady != null) dataReady.unblock();
						return skipped;
					}
					stopReading = true;
					moveNextBuffer(false);
					remaining = n - nb;
				} else {
					if (endReached) return skipped;
					if (nextReadTask == null && reusableBuffers != null) {
						// we cancelled all buffers, let's do a move
						long n2 = src.skipSync(n);
						skipped += n2;
						read += n2;
						// restart reading
						stopReading = false;
						moveNextBuffer(true);
						return skipped;
					}
					if (isClosing() || isClosed()) return skipped;
					if (dataReady == null) dataReady = new SynchronizationPoint<>();
					sp = dataReady;
				}
			}
			if (remaining > 0) {
				n = remaining;
				continue;
			}
			if (sp != null)
				sp.block(0);
		} while (true);		
	}
	
	@Override
	public int skip(int skip) throws IOException {
		return (int)skipSync(skip);
	}
	
	@Override
	public AsyncWork<Long,IOException> skipAsync(long n, RunnableWithParameter<Pair<Long,IOException>> ondone) {
		synchronized (this) {
			if (error != null) {
				if (ondone != null) ondone.run(new Pair<>(null, error));
				return new AsyncWork<Long,IOException>(null, error);
			}
			if (n <= 0) {
				if (ondone != null) ondone.run(new Pair<>(Long.valueOf(0), null));
				return new AsyncWork<Long,IOException>(Long.valueOf(0), null);
			}
			Task<Long,IOException> t = new Task.Cpu<Long,IOException>(
				"Skipping data from pre-buffered IO " + getSourceDescription(), priority, ondone
			) {
				@Override
				public Long run() throws IOException {
					return Long.valueOf(skipSync(n));
				}
			};
			operation(t.start());
			return t.getOutput();
		}
	}
	
	private void moveNextBuffer(boolean startNextRead) {
		synchronized (this) {
			if (!currentIsFirst && current != null /* current can be null when reading has been paused, while skipping bytes */) {
				reusableBuffers.addLast(current);
			}
			current = buffersReady.pollFirst();
			currentIsFirst = false;
			if (!endReached &&
				error == null &&
				nextReadTask == null &&
				reusableBuffers != null &&
				!reusableBuffers.isEmpty() &&
				startNextRead &&
				!stopReading)
				nextRead();
		}
	}
	
	private void nextRead() {
		if (isClosing() || isClosed()) {
			SynchronizationPoint<NoException> dr;
			synchronized (this) {
				dr = dataReady;
				dataReady = null;
			}
			if (dr != null)
				dr.unblock();
			return;
		}
		ByteBuffer buffer = reusableBuffers.removeFirst();
		buffer.clear();
		nextReadTask = operation(src.readFullyAsync(buffer));
		// TODO this listener may take few milliseconds, see how to make it in a task
		nextReadTask.listenInline(new Runnable() {
			@Override
			public void run() {
				if (nextReadTask == null) return; // case we have been closed
				Throwable e;
				try { e = nextReadTask.getError(); }
				catch (NullPointerException ex) { return; /* we are closed */ }
				if (e != null) {
					if (e instanceof IOException) error = (IOException)e;
					else error = new IOException("Read failed", e);
					nextReadTask = null;
					synchronized (PreBufferedReadable.this) {
						if (dataReady != null) {
							SynchronizationPoint<NoException> dr = dataReady;
							dataReady = null;
							dr.unblock();
						}
					}
				} else {
					int nb;
					try { nb = ((Integer)nextReadTask.getResult()).intValue(); }
					catch (NullPointerException ex) { nb = 0; /* we are closed */ }
					SynchronizationPoint<NoException> sp = null;
					synchronized (PreBufferedReadable.this) {
						nextReadTask = null;
						if (buffersReady == null) return; // closed
						if (nb <= 0) {
							endReached = true;
						} else {
							read += nb;
							if (nb < buffer.limit() || (size > 0 && read == size)) 
								endReached = true;
							buffer.flip();
							if (current == null) current = buffer;
							else buffersReady.addLast(buffer);
							if (!endReached && !reusableBuffers.isEmpty() && !stopReading)
								nextRead();
						}
						if (endReached && size > 0 && read < size)
							error = new IOException("Unexpected end after " + read + " bytes read, known size is " + size);
						if (dataReady != null) {
							sp = dataReady;
							dataReady = null;
						}
					}
					if (sp != null)
						sp.unblock();
				}
			}
		});
	}
	
	@Override
	public int read() throws IOException {
		do {
			SynchronizationPoint<NoException> sp;
			synchronized (this) {
				if (error != null) throw error;
				if (current != null) {
					if (!current.hasRemaining() && endReached)
						return -1;
					break;
				}
				if (endReached) return -1;
				if (isClosing() || isClosed()) return -1;
				if (dataReady == null) dataReady = new SynchronizationPoint<>();
				sp = dataReady;
			}
			sp.block(0);
		} while (true);
		int res = current.get() & 0xFF;
		if (!current.hasRemaining())
			moveNextBuffer(true);
		return res;
	}
	
	@Override
	public int read(byte[] buffer, int offset, int len) throws IOException {
		do {
			SynchronizationPoint<NoException> sp;
			synchronized (this) {
				if (error != null) throw error;
				if (current != null) {
					if (!current.hasRemaining() && endReached)
						return -1;
					break;
				}
				if (endReached) return -1;
				if (isClosing() || isClosed()) return -1;
				if (dataReady == null) dataReady = new SynchronizationPoint<>();
				sp = dataReady;
			}
			sp.block(0);
		} while (true);
		if (current.remaining() > len) {
			current.get(buffer, offset, len);
			return len;
		}
		len = current.remaining();
		current.get(buffer, offset, len);
		moveNextBuffer(true);
		return len;
	}
	
	@Override
	public int readFully(byte[] buffer) throws IOException {
		do {
			SynchronizationPoint<NoException> sp;
			synchronized (this) {
				if (error != null) throw error;
				if (current != null) {
					if (!current.hasRemaining() && endReached)
						return -1;
					break;
				}
				if (endReached) return -1;
				if (isClosing() || isClosed()) return -1;
				if (dataReady == null) dataReady = new SynchronizationPoint<>();
				sp = dataReady;
			}
			sp.block(0);
		} while (true);
		if (current.remaining() > buffer.length) {
			current.get(buffer);
			return buffer.length;
		}
		int len = current.remaining();
		current.get(buffer, 0, len);
		moveNextBuffer(true);
		int pos = len;
		while (pos < buffer.length) {
			len = read(buffer, pos, buffer.length - pos);
			if (len < 0) break;
			pos += len;
		}
		return pos;
	}

	@Override
	public AsyncWork<ByteBuffer, IOException> readNextBufferAsync(RunnableWithParameter<Pair<ByteBuffer, IOException>> ondone) {
		SynchronizationPoint<NoException> sp;
		synchronized (this) {
			if (error != null) {
				if (ondone != null) ondone.run(new Pair<>(null, null));
				return new AsyncWork<>(null, error);
			}
			if (current != null) {
				if (!current.hasRemaining() && endReached) {
					if (ondone != null) ondone.run(new Pair<>(null, null));
					return new AsyncWork<>(null, null);
				}
				Task.Cpu<ByteBuffer, IOException> task = new Task.Cpu<ByteBuffer, IOException>(
					"Read next buffer", getPriority(), ondone
				) {
					@Override
					public ByteBuffer run() {
						ByteBuffer buf = ByteBuffer.allocate(current.remaining());
						buf.put(current);
						moveNextBuffer(true);
						buf.flip();
						return buf;
					}
				};
				operation(task.start());
				return task.getOutput();
			}
			if (endReached) {
				if (ondone != null) ondone.run(new Pair<>(null, null));
				return new AsyncWork<>(null, null);
			}
			if (isClosing() || isClosed()) return new AsyncWork<>(null, null, new CancelException("IO closed"));
			if (dataReady == null) dataReady = new SynchronizationPoint<>();
			sp = dataReady;
		}
		AsyncWork<ByteBuffer, IOException> result = new AsyncWork<>();
		sp.listenInline(new Runnable() {
			@Override
			public void run() {
				readNextBufferAsync(ondone).listenInline(result);
			}
		});
		return operation(result);
	}
	
}
