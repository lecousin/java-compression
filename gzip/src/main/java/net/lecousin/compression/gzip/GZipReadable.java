package net.lecousin.compression.gzip;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import net.lecousin.compression.deflate.InflaterCache;
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
import net.lecousin.framework.io.util.DataUtil;
import net.lecousin.framework.util.Pair;
import net.lecousin.framework.util.RunnableWithParameter;

/** GZip decompression. */
public class GZipReadable extends IO.AbstractIO implements IO.Readable {

	/** GZipReadable with a known uncompressed size. */
	public static class SizeKnown extends GZipReadable implements IO.KnownSize {
		/** Constructor. */
		public SizeKnown(IO.Readable.Buffered input, byte priority, long uncompressedSize) {
			super(input, priority);
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
	public GZipReadable(IO.Readable.Buffered input, byte priority) {
		this.input = input;
		this.priority = priority;
		header = new SynchronizationPoint<>();
		readHeader();
		getInflater = InflaterCache.get(true);
	}
	
	private IO.Readable.Buffered input;
	private byte priority;
	private AsyncWork<Inflater,NoException> getInflater;
	private SynchronizationPoint<IOException> header;
	private byte[] currentBuffer = null;
	private int currentPos = 0;
	private int currentLen = 0;
	private IOException error = null;
	private boolean eof = false;
	private AsyncWork<Integer,IOException> currentRead = null;
	private ISynchronizationPoint<IOException> closing = null;
	
	@Override
	public byte getPriority() {
		return priority;
	}
	
	@Override
	public void setPriority(byte priority) {
		this.priority = priority;
	}
	
	@Override
	public ISynchronizationPoint<IOException> canStartReading() {
		if (getInflater.isUnblocked() && header.isUnblocked())
			return header;
		SynchronizationPoint<IOException> sp = new SynchronizationPoint<>();
		JoinPoint.fromSynchronizationPoints(getInflater, header).listenInline(() -> {
			if (error != null)
				sp.error(error);
			else
				sp.unblock();
		});
		return sp;
	}
	
	private SynchronizationPoint<NoException> nextBuffer() {
		SynchronizationPoint<NoException> sp = new SynchronizationPoint<>();
		AsyncWork<ByteBuffer, IOException> read = input.readNextBufferAsync();
		read.listenInline(() -> {
			if (read.hasError()) error = read.getError();
			else if (read.isCancelled()) error = new IOException("Operation cancelled", read.getCancelEvent());
			else {
				ByteBuffer b = read.getResult();
				if (b == null) {
					eof = true;
					currentBuffer = null;
					currentPos = 0;
					currentLen = 0;
					sp.unblock();
					return;
				}
				if (b.hasArray()) {
					currentBuffer = b.array();
					currentPos = b.position();
					currentLen = b.limit();
				} else {
					new Task.Cpu<Void, NoException>("Convert native buffer into java buffer", priority) {
						@Override
						public Void run() {
							currentLen = b.remaining();
							currentBuffer = new byte[currentLen];
							currentPos = 0;
							b.get(currentBuffer);
							sp.unblock();
							return null;
						}
					}.start();
					return;
				}
				sp.unblock();
			}
		});
		return sp;
	}
	
	private void readHeader() {
		if (error != null) {
			header.error(error);
			return;
		}
		if (eof) {
			header.unblock();
			return;
		}
		if (currentPos == currentLen) {
			nextBuffer().listenInline(() -> { readHeader(); });
			return;
		}
		new Task.Cpu<Void, NoException>("Read GZip header", priority) {
			@Override
			public Void run() {
				if (currentLen - currentPos < 10) {
					byte[] b = new byte[10];
					int rem = currentLen - currentPos;
					System.arraycopy(currentBuffer, currentPos, b, 0, rem);
					int nb;
					try { nb = IOUtil.readFully(input, b, rem, 10 - rem); }
					catch (IOException e) {
						error = e;
						header.error(e);
						return null;
					}
					if (nb != 10 - rem) {
						error = new IOException("Unexpected end of GZIP data");
						header.error(error);
						return null;
					}
					currentBuffer = b;
					currentPos = 0;
					currentLen = 10;
				}
				int b = currentBuffer[currentPos++] & 0xFF;
				if (b != 0x1F) {
					error = new IOException("Invalid GZIP header: first byte must be 1F");
					header.error(error);
					return null;
				}
				b = currentBuffer[currentPos++] & 0xFF;
				if (b != 0x8B) {
					error = new IOException("Invalid GZIP header: second byte must be 8B");
					header.error(error);
					return null;
				}
				b = currentBuffer[currentPos++] & 0xFF;
				if (b != 8) {
					error = new IOException("Unsupported compression method " + b
						+ " for GZIP, only method 8 (deflate) is supported");
					header.error(error);
					return null;
				}
				b = currentBuffer[currentPos++] & 0xFF;
				// skip MTIME + XFL + OS
				currentPos += 6;
				
				// extra data
				if ((b & 4) != 0) {
					// skip extra
					int extraLen;
					if (currentPos == currentLen)
						try { extraLen = DataUtil.readUnsignedShortLittleEndian(input); }
						catch (IOException e) {
							error = e;
							header.error(e);
							return null;
						}
					else if (currentPos == currentLen - 1)
						try { extraLen = (currentBuffer[currentPos++] & 0xFF) | ((input.readByte() & 0xFF) << 8); }
						catch (IOException e) {
							error = e;
							header.error(e);
							return null;
						}
					else {
						extraLen = currentBuffer[currentPos++] & 0xFF;
						extraLen |= (currentBuffer[currentPos++] & 0xFF) << 8;
					}
					if (currentLen - currentPos >= extraLen) {
						currentPos += extraLen;
					} else {
						int nb = extraLen - (currentLen - currentPos);
						try {
							if (input.skip(nb) != nb) throw new EOFException();
						} catch (IOException e) {
							error = e;
							header.error(e);
							return null;
						}
						currentPos = currentLen;
					}
				}
				
				// filename
				if ((b & 8) != 0)
					if (!skipString())
						return null;
				
				// comment
				if ((b & 16) != 0)
					if (!skipString())
						return null;
				
				// crc
				if ((b & 2) != 0) {
					if (currentPos == currentLen)
						try { if (input.skip(2) != 2) throw new EOFException(); }
						catch (IOException e) {
							error = e;
							header.error(e);
							return null;
						}
					else if (currentPos == currentLen - 1) {
						currentPos = currentLen;
						try { if (input.skip(1) != 1) throw new EOFException(); }
						catch (IOException e) {
							error = e;
							header.error(e);
							return null;
						}
					} else
						currentPos += 2;
				}
				
				header.unblock();
				return null;
			}
			
			private boolean skipString() {
				while (currentPos < currentLen) {
					if (currentBuffer[currentPos++] == 0)
						return true;
				}
				do {
					try {
						if (input.readByte() == 0) return true;
					} catch (IOException e) {
						error = e;
						header.error(e);
						return false;
					}
				} while (true);
			}
		}.start();
	}
	
	private void skipTrailer() {
		int rem = currentLen - currentPos;
		if (rem >= 8) {
			currentPos += 8;
			return;
		}
		currentPos = currentLen;
		try { input.skip(8 - rem); }
		catch (IOException e) {
			// ignore as we don't do anything with the trailer so far
		}
	}
	
	@Override
	public AsyncWork<Integer,IOException> readAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer,IOException>> ondone) {
		return readAsync(buffer, ondone, false);
	}

	private AsyncWork<Integer,IOException> readAsync(
		ByteBuffer buffer, RunnableWithParameter<Pair<Integer,IOException>> ondone, boolean isCurrent
	) {
		if (error != null) {
			if (ondone != null) ondone.run(new Pair<>(null, error));
			return new AsyncWork<>(null, error);
		}
		if (!getInflater.isUnblocked()) {
			AsyncWork<Integer,IOException> res = new AsyncWork<Integer,IOException>();
			currentRead = res;
			getInflater.listenInline(() -> { readAsync(buffer, ondone, true).listenInline(res); });
			return res;
		}
		if (!header.isUnblocked()) {
			AsyncWork<Integer,IOException> res = new AsyncWork<Integer,IOException>();
			currentRead = res;
			header.listenInline(() -> { readAsync(buffer, ondone, true).listenInline(res); });
			return res;
		}
		if (eof) {
			if (ondone != null) ondone.run(new Pair<>(Integer.valueOf(-1), null));
			return new AsyncWork<Integer,IOException>(Integer.valueOf(-1), null);
		}
		if (!isCurrent && currentRead != null && !currentRead.isUnblocked()) {
			// wait for current read to finish
			AsyncWork<Integer, IOException> result = new AsyncWork<>();
			AsyncWork<Integer, IOException> previous = currentRead;
			currentRead = result;
			previous.listenInline(() -> {
				readAsync(buffer, ondone, true).listenInline(result);
			});
			return result;
		}
		Inflater inflater = getInflater.getResult();
		if (!inflater.needsInput()) {
			AsyncWork<Integer, IOException> result = new AsyncWork<>();
			InflateTask inflate = new InflateTask(buffer, result, ondone, false);
			currentRead = result;
			inflate.start();
			return result;
		}
		AsyncWork<Integer, IOException> result = new AsyncWork<>();
		if (currentPos == currentLen) {
			currentRead = result;
			nextBuffer().listenInline(() -> {
				readAsync(buffer, ondone, true).listenInline(result);
			});
			return result;
		}
		InflateTask inflate = new InflateTask(buffer, result, ondone, true);
		currentRead = result;
		inflate.start();
		return result;
	}
	
	private class InflateTask extends Task.Cpu<Void, NoException> {
		private InflateTask(
			ByteBuffer buffer, AsyncWork<Integer, IOException> result,
			RunnableWithParameter<Pair<Integer,IOException>> ondone, boolean setInput
		) {
			super("Uncompressing gzip: " + input.getSourceDescription(), priority);
			this.buffer = buffer;
			this.result = result;
			this.ondone = ondone;
			this.setInput = setInput;
		}
		
		private ByteBuffer buffer;
		private AsyncWork<Integer, IOException> result;
		private RunnableWithParameter<Pair<Integer,IOException>> ondone;
		private boolean setInput;

		@Override
		public Void run() {
			Inflater inflater = getInflater.getResult();
			if (setInput) {
				inflater.setInput(currentBuffer, currentPos, currentLen - currentPos);
			}
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
					n = inflater.inflate(b, off + total, buffer.remaining() - total);
					if (n > 0) total += n;
	                if (inflater.finished() || inflater.needsDictionary()) {
	                	currentPos = currentLen - inflater.getRemaining();
	                	skipTrailer();
	                	header = new SynchronizationPoint<>();
	                	readHeader();
	                	inflater.reset();
	                	if (total <= 0) {
	                		// no data read yet
	                		header.listenInline(() -> {
	                			readAsync(buffer, ondone, true).listenInline(result);
	                		});
	                		return null;
	                	}
	                	// some data read
	                	break;
	                }
	                if (inflater.needsInput()) {
	                	currentPos = currentLen;
		                if (total > 0) break; // some data read
	                	readAsync(buffer, ondone, true).listenInline(result);
	                	return null;
	                }
				} while (n > 0 && total < buffer.remaining() && !inflater.needsInput());
				if (!buffer.hasArray())
					buffer.put(b, 0, total);
				else
					buffer.position(off + total);
				Integer r = Integer.valueOf(total);
				if (ondone != null) ondone.run(new Pair<>(r, null));
				result.unblockSuccess(r);
				return null;
			} catch (DataFormatException e) {
				error = new IOException("Invalid compressed data", e);
				if (ondone != null) ondone.run(new Pair<>(null, error));
				result.error(error);
			}
			return null;
		}
	}

	@Override
	public String getSourceDescription() {
		return "GZIP: " + input.getSourceDescription();
	}

	@Override
	public IO getWrappedIO() {
		return null;
	}

	@Override
	public TaskManager getTaskManager() {
		return Threading.getCPUTaskManager();
	}

	@Override
	public int readSync(ByteBuffer buffer) throws IOException {
		try {
			return readAsync(buffer).blockResult(0).intValue();
		} catch (CancelException e) {
			throw new IOException("Operation cancelled", e);
		}
	}

	@Override
	public int readFullySync(ByteBuffer buffer) throws IOException {
		try {
			return readFullyAsync(buffer).blockResult(0).intValue();
		} catch (CancelException e) {
			throw new IOException("Operation cancelled", e);
		}
	}

	@Override
	public AsyncWork<Integer, IOException> readFullyAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer, IOException>> ondone) {
		return IOUtil.readFullyAsynch(this, buffer, ondone);
	}

	@Override
	public long skipSync(long n) throws IOException {
		if (n <= 0) return 0;
		return IOUtil.skipSyncByReading(this, n);
	}

	@Override
	public AsyncWork<Long, IOException> skipAsync(long n, RunnableWithParameter<Pair<Long, IOException>> ondone) {
		return IOUtil.skipAsyncByReading(this, n, ondone);
	}

	@Override
	protected ISynchronizationPoint<IOException> closeIO() {
		if (closing != null) return closing;
		if (!getInflater.isUnblocked()) {
			SynchronizationPoint<IOException> sp = new SynchronizationPoint<>();
			closing = sp;
			getInflater.listenInline(() -> {
				InflaterCache.free(getInflater.getResult(), true);
				input.closeAsync().listenInline(sp);
			});
			return sp;
		}
		// do not end, because this closes it definitely and the cache wants to reuse it
		// getInflater.getResult().end();
		InflaterCache.free(getInflater.getResult(), true);
		return closing = input.closeAsync();
	}
	
}
