package net.lecousin.compression.deflate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.zip.Deflater;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.TaskManager;
import net.lecousin.framework.concurrent.Threading;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.exception.NoException;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.util.LimitWriteOperations;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;

/**
 * Deflate compression: wrap a Writable and compress data in it.
 * One of the method finishSync or finishAsync must be called when no more data needs to be compressed.
 * It uses the {@link Deflater} provided by Java.
 */
public class DeflateWritable extends ConcurrentCloseable<IOException> implements IO.Writable {
	
	/** Constructor. */
	public DeflateWritable(IO.Writable output, byte priority, int level, boolean nowrap, int maxPendingWrite) {
		this.output = output;
		this.priority = priority;
		deflater = new Deflater(level, nowrap);
		writeOps = new LimitWriteOperations(output, maxPendingWrite);
	}
	
	protected IO.Writable output;
	protected byte priority;
	protected Deflater deflater;
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
	public byte getPriority() { return priority; }
	
	@Override
	public void setPriority(byte priority) { this.priority = priority; }
	
	@Override
	protected IAsync<IOException> closeUnderlyingResources() {
		if (finishing == null)
			finishAsync();
		if (finishing.isDone())
			return output.closeAsync();
		Async<IOException> sp = new Async<>();
		finishing.onDone(() -> output.closeAsync().onDone(sp), sp);
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
	
	@Override
	public int writeSync(ByteBuffer buffer) throws IOException {
		int len = buffer.remaining();
		if (buffer.hasArray()) {
			deflater.setInput(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
			buffer.position(buffer.position() + buffer.remaining());
		} else {
			byte[] buf = new byte[buffer.remaining()];
			buffer.get(buf);
			deflater.setInput(buf);
		}
		byte[] writeBuf = new byte[len > 128 * 1024 ? 128 * 1024 : len];
		AsyncSupplier<Integer, IOException> lastWrite = writeOps.getLastPendingOperation();
		if (lastWrite != null) {
			lastWrite.blockException(0);
		}
		while (!deflater.needsInput()) {
			int nb = deflater.deflate(writeBuf, 0, writeBuf.length);
			if (nb <= 0) break;
			output.writeSync(ByteBuffer.wrap(writeBuf, 0, nb));
		}
		return len;
	}
	
	@Override
	public AsyncSupplier<Integer,IOException> writeAsync(ByteBuffer buffer, Consumer<Pair<Integer,IOException>> ondone) {
		Task<Integer,IOException> task = new Task.Cpu<Integer,IOException>("Compressing data using deflate", priority, ondone) {
			@Override
			public Integer run() throws IOException {
				if (isCancelled()) return Integer.valueOf(0);
				int len = buffer.remaining();
				if (buffer.hasArray()) {
					deflater.setInput(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
					buffer.position(buffer.position() + buffer.remaining());
				} else {
					byte[] buf = new byte[buffer.remaining()];
					buffer.get(buf);
					deflater.setInput(buf);
				}
				while (!deflater.needsInput()) {
					byte[] writeBuf = new byte[len > 8192 ? 8192 : len];
					int nb = deflater.deflate(writeBuf, 0, writeBuf.length);
					if (nb <= 0) break;
					writeOps.write(ByteBuffer.wrap(writeBuf, 0, nb));
				}
				return Integer.valueOf(len);
			}
		};
		operation(task.start());
		return task.getOutput();
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
				byte[] writeBuf = new byte[8192];
				do {
					int nb = deflater.deflate(writeBuf, 0, writeBuf.length);
					if (nb <= 0) break;
					output.writeSync(ByteBuffer.wrap(writeBuf, 0, nb));
				} while (!deflater.finished());
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
		Task<Void,NoException> task = new Task.Cpu<Void,NoException>("Finishing zip compression", priority) {
			@Override
			public Void run() {
				AsyncSupplier<Integer, IOException> lastWrite = null;
				deflater.finish();
				if (!deflater.finished()) {
					do {
						byte[] writeBuf = new byte[8192];
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
			}
		};
		task.start();
		return operation(finishing);
	}

}
