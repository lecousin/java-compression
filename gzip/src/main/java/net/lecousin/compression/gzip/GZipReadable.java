package net.lecousin.compression.gzip;

import java.io.EOFException;
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
import net.lecousin.framework.io.data.ByteArray;
import net.lecousin.framework.io.util.DataUtil;
import net.lecousin.framework.memory.ByteArrayCache;
import net.lecousin.framework.text.StringUtil;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;

/** GZip decompression. */
public class GZipReadable extends ConcurrentCloseable<IOException> implements IO.Readable {

	/** GZipReadable with a known uncompressed size. */
	public static class SizeKnown extends GZipReadable implements IO.KnownSize {
		/** Constructor. */
		public SizeKnown(IO.Readable.Buffered input, Priority priority, long uncompressedSize) {
			super(input, priority);
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
	public GZipReadable(IO.Readable.Buffered input, Priority priority) {
		this.input = input;
		this.priority = priority;
		header = new Async<>();
		readHeader();
		inflater = new Inflater(true);
	}
	
	private IO.Readable.Buffered input;
	private Priority priority;
	private Inflater inflater;
	private Async<IOException> header;
	private ByteArray currentBuffer = null;
	private IOException error = null;
	private boolean eof = false;
	private AsyncSupplier<Integer,IOException> currentRead = null;
	
	@Override
	public Priority getPriority() {
		return priority;
	}
	
	@Override
	public void setPriority(Priority priority) {
		this.priority = priority;
	}
	
	@Override
	protected IAsync<IOException> closeUnderlyingResources() {
		return input.closeAsync();
	}
	
	@Override
	protected void closeResources(Async<IOException> ondone) {
		input = null;
		currentBuffer = null;
		inflater.end();
		inflater = null;
		ondone.unblock();
	}
	
	@Override
	public IAsync<IOException> canStartReading() {
		return header;
	}
	
	private Async<NoException> nextBuffer() {
		Async<NoException> sp = new Async<>();
		AsyncSupplier<ByteBuffer, IOException> read = input.readNextBufferAsync();
		read.onDone(() -> {
			if (read.hasError()) error = read.getError();
			else if (read.isCancelled()) error = IO.errorCancelled(read.getCancelEvent());
			else {
				if (currentBuffer != null)
					currentBuffer.free();
				ByteBuffer b = read.getResult();
				if (b == null) {
					eof = true;
					currentBuffer = null;
					sp.unblock();
					return;
				}
				currentBuffer = ByteArray.fromByteBuffer(b);
			}
			sp.unblock();
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
		if (currentBuffer == null || !currentBuffer.hasRemaining()) {
			nextBuffer().onDone(this::readHeader);
			return;
		}
		Task.cpu("Read GZip header", priority, this::readHeaderTask).start();
	}
	
	private Void readHeaderTask() {
		if (currentBuffer.remaining() < 10 && !readHeaderEnsure10Bytes())
			return null;
		int b = currentBuffer.get() & 0xFF;
		if (b != 0x1F) {
			error = new IOException("Invalid GZIP header: first byte must be 1F, found is "
				+ StringUtil.encodeHexa((byte)b));
			header.error(error);
			return null;
		}
		b = currentBuffer.get() & 0xFF;
		if (b != 0x8B) {
			error = new IOException("Invalid GZIP header: second byte must be 8B, found is "
				+ StringUtil.encodeHexa((byte)b));
			header.error(error);
			return null;
		}
		b = currentBuffer.get() & 0xFF;
		if (b != 8) {
			error = new IOException("Unsupported compression method " + b
				+ " for GZIP, only method 8 (deflate) is supported");
			header.error(error);
			return null;
		}
		b = currentBuffer.get() & 0xFF;
		// skip MTIME + XFL + OS
		currentBuffer.moveForward(6);
		
		// extra data
		if ((b & 4) != 0 && !skipExtra())
			return null;
		
		// filename
		if ((b & 8) != 0 && !skipString())
			return null;
		
		// comment
		if ((b & 16) != 0 && !skipString())
			return null;
		
		// crc
		if ((b & 2) != 0) {
			try {
				switch (currentBuffer.remaining()) {
				case 0:
					if (input.skip(2) != 2) throw new EOFException();
					break;
				case 1:
					if (input.skip(1) != 1) throw new EOFException();
					currentBuffer.moveForward(1);
					break;
				default:
					currentBuffer.moveForward(2);
					break;
				}
			} catch (IOException e) {
				error = e;
				header.error(e);
				return null;
			}
		}
		
		header.unblock();
		return null;
	}
	
	private boolean readHeaderEnsure10Bytes() {
		try {
			do {
				ByteBuffer next = input.readNextBuffer();
				if (next == null) throw new EOFException();
				if (!currentBuffer.hasRemaining()) {
					currentBuffer.free();
					currentBuffer = ByteArray.fromByteBuffer(next);
					if (currentBuffer.remaining() >= 10)
						return true;
					continue;
				}
				int len = next.remaining();
				int r = currentBuffer.remaining();
				if (currentBuffer instanceof ByteArray.Writable &&
					currentBuffer.getArray().length >= r + len) {
					// copy into currentBuffer
					byte[] b = currentBuffer.getArray();
					if (r > 0)
						System.arraycopy(b, currentBuffer.getCurrentArrayOffset(), b, 0, r);
					next.get(b, r, len);
					currentBuffer = new ByteArray.Writable(b, 0, r + len, true);
					if (currentBuffer.remaining() >= 10)
						return true;
					continue;
				}
				byte[] b = ByteArrayCache.getInstance().get(r + len, true);
				currentBuffer.get(b, 0, r);
				next.get(b, r, len);
				ByteArray.Writable ba = new ByteArray.Writable(b, 0, r + len, true);
				currentBuffer.free();
				currentBuffer = ba;
				if (currentBuffer.remaining() >= 10)
					return true;
			} while (true);
		} catch (EOFException e) {
			error = new IOException("Unexpected end of GZIP data");
			header.error(error);
			return false;
		} catch (IOException e) {
			error = e;
			header.error(e);
			return false;
		}
	}

	private boolean skipString() {
		while (currentBuffer.hasRemaining()) {
			if (currentBuffer.get() == 0)
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
	
	private boolean skipExtra() {
		// skip extra
		try {
			int extraLen;
			switch (currentBuffer.remaining()) {
			case 0:
				extraLen = DataUtil.Read16U.LE.read(input);
				break;
			case 1:
				extraLen = (currentBuffer.get() & 0xFF) | ((input.readByte() & 0xFF) << 8);
				break;
			default:
				extraLen = currentBuffer.get() & 0xFF;
				extraLen |= (currentBuffer.get() & 0xFF) << 8;
				break;
			}
			int rem = currentBuffer.remaining();
			if (rem >= extraLen) {
				currentBuffer.moveForward(extraLen);
			} else {
				currentBuffer.goToEnd();
				rem = extraLen - rem;
				int skipped = input.skip(rem);
				if (skipped != rem) throw new EOFException(skipped + " byte(s) of extra data, expected is " + rem);
			}
			return true;
		} catch (IOException e) {
			error = e;
			header.error(e);
			return false;
		}
	}
	
	
	@Override
	public AsyncSupplier<Integer,IOException> readAsync(ByteBuffer buffer, Consumer<Pair<Integer,IOException>> ondone) {
		return readAsync(buffer, ondone, false);
	}

	private AsyncSupplier<Integer,IOException> readAsync(
		ByteBuffer buffer, Consumer<Pair<Integer,IOException>> ondone, boolean isCurrent
	) {
		if (error != null)
			return IOUtil.error(error, ondone);
		if (!header.isDone()) {
			AsyncSupplier<Integer,IOException> res = new AsyncSupplier<>();
			currentRead = res;
			header.onDone(() -> readAsync(buffer, ondone, true).forward(res));
			return operation(res);
		}
		if (eof)
			return IOUtil.success(Integer.valueOf(-1), ondone);
		if (!isCurrent && currentRead != null && !currentRead.isDone()) {
			// wait for current read to finish
			AsyncSupplier<Integer, IOException> result = new AsyncSupplier<>();
			AsyncSupplier<Integer, IOException> previous = currentRead;
			currentRead = result;
			previous.onDone(() -> readAsync(buffer, ondone, true).forward(result));
			return operation(result);
		}
		if (!inflater.needsInput()) {
			AsyncSupplier<Integer, IOException> result = new AsyncSupplier<>();
			Task<Void, NoException> inflate = Task.cpu("Uncompressing gzip: " + input.getSourceDescription(), priority,
				new InflateTask(buffer, result, ondone, false));
			currentRead = result;
			operation(inflate.start());
			if (inflate.isCancelling())
				result.cancel(inflate.getCancelEvent());
			return result;
		}
		AsyncSupplier<Integer, IOException> result = new AsyncSupplier<>();
		if (!currentBuffer.hasRemaining()) {
			currentRead = result;
			nextBuffer().onDone(() -> readAsync(buffer, ondone, true).forward(result));
			return operation(result);
		}
		Task<Void, NoException> inflate = Task.cpu("Uncompressing gzip: " + input.getSourceDescription(), priority,
			new InflateTask(buffer, result, ondone, true));
		currentRead = result;
		operation(inflate.start());
		if (inflate.isCancelling())
			result.cancel(inflate.getCancelEvent());
		return result;
	}
	
	private class InflateTask implements Executable<Void, NoException> {
		private InflateTask(
			ByteBuffer buffer, AsyncSupplier<Integer, IOException> result,
			Consumer<Pair<Integer,IOException>> ondone, boolean setInput
		) {
			this.buffer = buffer;
			this.inflateResult = result;
			this.onInflateDone = ondone;
			this.setInput = setInput;
		}
		
		private ByteBuffer buffer;
		private AsyncSupplier<Integer, IOException> inflateResult;
		private Consumer<Pair<Integer,IOException>> onInflateDone;
		private boolean setInput;

		@Override
		public Void execute() {
			if (isClosing() || isClosed() || input == null) {
				inflateResult.cancel(new CancelException("GZip closed"));
				return null;
			}
			if (setInput) {
				inflater.setInput(currentBuffer.getArray(), currentBuffer.getCurrentArrayOffset(), currentBuffer.remaining());
				currentBuffer.goToEnd();
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
				int total = 0;
				do {
					if (isClosing() || isClosed() || input == null) {
						inflateResult.cancel(new CancelException("GZip closed"));
						return null;
					}
					n = inflater.inflate(b, off + total, buffer.remaining() - total);
					if (n > 0)
						total += n;
	                if (inflater.finished() || inflater.needsDictionary()) {
	                	currentBuffer.setPosition(currentBuffer.length() - inflater.getRemaining());
	                	skipTrailer();
	                	header = new Async<>();
	                	readHeader();
	                	inflater.reset();
	                	if (total <= 0) {
	                		// no data read yet
	                		header.onDone(() -> readAsync(buffer, onInflateDone, true).forward(inflateResult));
	                		return null;
	                	}
	                	// some data read
	                	break;
	                }
	                if (inflater.needsInput()) {
		                if (total > 0) break; // some data read
	                	readAsync(buffer, onInflateDone, true).forward(inflateResult);
	                	return null;
	                }
				} while (n > 0 && total < buffer.remaining() && !inflater.needsInput());
				if (!buffer.hasArray())
					buffer.put(b, 0, total);
				else
					buffer.position(off + total - buffer.arrayOffset());
				IOUtil.success(Integer.valueOf(total), inflateResult, onInflateDone);
			} catch (DataFormatException e) {
				error = new IOException("Invalid compressed data after " + inflater.getBytesRead()
					+ " bytes (" + inflater.getBytesWritten() + " uncompressed)", e);
				IOUtil.error(error, inflateResult, onInflateDone);
			}
			return null;
		}
		
		private void skipTrailer() {
			int rem = currentBuffer.remaining();
			if (rem >= 8) {
				currentBuffer.moveForward(8);
				return;
			}
			currentBuffer.goToEnd();
			try { input.skip(8 - rem); }
			catch (IOException e) {
				// ignore as we don't do anything with the trailer so far
			}
		}

	}

	@Override
	public String getSourceDescription() {
		return "GZIP: " + (input != null ? input.getSourceDescription() : "null");
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
			throw IO.errorCancelled(e);
		}
	}

	@Override
	public int readFullySync(ByteBuffer buffer) throws IOException {
		try {
			return readFullyAsync(buffer).blockResult(0).intValue();
		} catch (CancelException e) {
			throw IO.errorCancelled(e);
		}
	}

	@Override
	public AsyncSupplier<Integer, IOException> readFullyAsync(ByteBuffer buffer, Consumer<Pair<Integer, IOException>> ondone) {
		return operation(IOUtil.readFullyAsync(this, buffer, ondone));
	}

	@Override
	public long skipSync(long n) throws IOException {
		if (n <= 0) return 0;
		return IOUtil.skipSyncByReading(this, n);
	}

	@Override
	public AsyncSupplier<Long, IOException> skipAsync(long n, Consumer<Pair<Long, IOException>> ondone) {
		return operation(IOUtil.skipAsyncByReading(this, n, ondone));
	}
	
}
