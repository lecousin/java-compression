package net.lecousin.compression.deflate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.zip.Deflater;

import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.concurrent.threads.Task.Priority;
import net.lecousin.framework.concurrent.threads.TaskManager;
import net.lecousin.framework.concurrent.threads.Threading;
import net.lecousin.framework.exception.NoException;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.util.LimitWriteOperations;
import net.lecousin.framework.memory.ByteArrayCache;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;

/**
 * Deflate compression: wrap a Writable and compress data in it.
 * One of the method finishSync or finishAsync must be called when no more data needs to be compressed.
 * It uses the {@link Deflater} provided by Java.
 */
public class DeflateWritable extends ConcurrentCloseable<IOException> implements IO.Writable {
	
	/** Constructor. */
	public DeflateWritable(IO.Writable output, Priority priority, int level, boolean nowrap, int maxPendingWrite) {
		this.output = output;
		this.priority = priority;
		deflater = new Deflater(level, nowrap);
		writeOps = new LimitWriteOperations(output, maxPendingWrite, null);
		bufferCache = ByteArrayCache.getInstance();
	}
	
	protected IO.Writable output;
	protected Priority priority;
	protected Deflater deflater;
	protected ByteArrayCache bufferCache;
	protected LimitWriteOperations writeOps;
	protected Async<IOException> finishing = null;

	@Override
	public TaskManager getTaskManager() {
		return Threading.getCPUTaskManager();
	}
	
	@Override
	public IO getWrappedIO() { return null; }
	
	@Override
	public String getSourceDescription() {
		return "Zip compression to " + output.getSourceDescription();
	}
	
	@Override
	public Priority getPriority() { return priority; }
	
	@Override
	public void setPriority(Priority priority) { this.priority = priority; }
	
	@Override
	protected IAsync<IOException> closeUnderlyingResources() {
		if (finishing == null)
			finishAsync();
		Async<IOException> sp = new Async<>();
		finishing.onDone(() -> {
			IAsync<IOException> close = output.closeAsync();
			close.onDone(() -> {
				if (!finishing.forwardIfNotSuccessful(sp))
					close.onDone(sp);
			});
		});
		return sp;
	}
	
	@Override
	protected void closeResources(Async<IOException> ondone) {
		output = null;
		deflater = null;
		writeOps = null;
		ondone.unblock();
	}
	
	@Override
	public IAsync<IOException> canStartWriting() {
		return new Async<>(true);
	}
	
	private int setInput(ByteBuffer buffer) {
		int len = buffer.remaining();
		if (buffer.hasArray()) {
			deflater.setInput(buffer.array(), buffer.arrayOffset() + buffer.position(), len);
			buffer.position(buffer.position() + len);
		} else {
			byte[] buf = new byte[len];
			buffer.get(buf);
			deflater.setInput(buf);
		}
		return len;
	}
	
	@Override
	public int writeSync(ByteBuffer buffer) throws IOException {
		int len = setInput(buffer);
		byte[] writeBuf = bufferCache.get(len > 128 * 1024 ? 128 * 1024 : len, true);
		AsyncSupplier<Integer, IOException> lastWrite = writeOps.getLastPendingOperation();
		if (lastWrite != null) {
			lastWrite.blockException(0);
		}
		while (!deflater.needsInput()) {
			int nb = deflater.deflate(writeBuf, 0, writeBuf.length);
			if (nb <= 0) break;
			output.writeSync(ByteBuffer.wrap(writeBuf, 0, nb).asReadOnlyBuffer());
		}
		bufferCache.free(writeBuf);
		return len;
	}
	
	@Override
	public AsyncSupplier<Integer,IOException> writeAsync(ByteBuffer buffer, Consumer<Pair<Integer,IOException>> ondone) {
		return operation(Task.cpu("Compressing data using deflate", priority, t -> {
			int len = setInput(buffer);
			while (!deflater.needsInput()) {
				byte[] writeBuf = bufferCache.get(len > 8192 ? 8192 : len, true);
				int nb = deflater.deflate(writeBuf, 0, writeBuf.length);
				if (nb <= 0) break;
				AsyncSupplier<Integer, IOException> write = writeOps.write(ByteBuffer.wrap(writeBuf, 0, nb));
				if (write.hasError())
					throw write.getError();
			}
			return Integer.valueOf(len);
		}, ondone).start()).getOutput();
	}
	
	/** Indicates that no more data will be compressed and flushes remaining compressed data to the output. */
	public void finishSynch() throws IOException {
		if (finishing != null) {
			finishing.blockException(0);
			return;
		}
		finishing = new Async<>();
		try {
			AsyncSupplier<Integer, IOException> lastWrite = writeOps.getLastPendingOperation();
			if (lastWrite != null) {
				lastWrite.blockException(0);
			}
			deflater.finish();
			if (!deflater.finished()) {
				byte[] writeBuf = bufferCache.get(8192, true);
				do {
					int nb = deflater.deflate(writeBuf, 0, writeBuf.length);
					if (nb <= 0) break;
					output.writeSync(ByteBuffer.wrap(writeBuf, 0, nb).asReadOnlyBuffer());
				} while (!deflater.finished());
				bufferCache.free(writeBuf);
			}
			finishing.unblock();
		} catch (IOException e) {
			finishing.error(e);
			throw e;
		}
	}
	
	/** Indicates that no more data will be compressed and flushes remaining compressed data to the output. */
	public IAsync<IOException> finishAsync() {
		if (finishing != null) return finishing;
		finishing = new Async<>();
		Task.cpu("Finishing zip compression", priority, (Task<Void, NoException> task) -> {
			AsyncSupplier<Integer, IOException> lastWrite = null;
			deflater.finish();
			if (!deflater.finished()) {
				do {
					byte[] writeBuf = bufferCache.get(8192, true);
					int nb = deflater.deflate(writeBuf, 0, writeBuf.length);
					if (nb <= 0) break;
					try { lastWrite = writeOps.write(ByteBuffer.wrap(writeBuf, 0, nb)); }
					catch (IOException e) {
						finishing.error(e);
						return null;
					}
				} while (!deflater.finished());
			}
			if (lastWrite == null) lastWrite = writeOps.getLastPendingOperation();
			if (lastWrite != null)
				lastWrite.onDone(finishing);
			else
				finishing.unblock();
			return null;
		}).start();
		Async<IOException> op = operation(new Async<>());
		finishing.onDone(op);
		return finishing;
	}

}
