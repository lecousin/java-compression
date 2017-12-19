package net.lecousin.compression.deflate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.TaskManager;
import net.lecousin.framework.concurrent.Threading;
import net.lecousin.framework.concurrent.synch.AsyncWork;
import net.lecousin.framework.concurrent.synch.ISynchronizationPoint;
import net.lecousin.framework.concurrent.synch.SynchronizationPoint;
import net.lecousin.framework.exception.NoException;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.util.LimitWriteOperations;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;
import net.lecousin.framework.util.RunnableWithParameter;

/**
 * Deflate compression: wrap a Writable and compress data in it.
 * One of the method finishSync or finishAsync must be called when no more data needs to be compressed.
 * It uses the {@link Deflater} provided by Java.
 */
public class DeflateWritable extends ConcurrentCloseable implements IO.Writable {
	
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
	protected ISynchronizationPoint<IOException> closeIO() {
		return output.closeAsync();
	}
	
	@Override
	public ISynchronizationPoint<IOException> canStartWriting() {
		return new SynchronizationPoint<>(true);
	}
	
	@Override
	public int writeSync(ByteBuffer buffer) throws IOException {
		int len = buffer.remaining();
		if (buffer.hasArray())
			deflater.setInput(buffer.array(), buffer.position(), buffer.remaining());
		else {
			byte[] buf = new byte[buffer.remaining()];
			buffer.get(buf);
			deflater.setInput(buf);
		}
		byte[] writeBuf = new byte[len];
		AsyncWork<Integer, IOException> lastWrite = writeOps.getLastPendingOperation();
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
	public AsyncWork<Integer,IOException> writeAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer,IOException>> ondone) {
		Task<Integer,IOException> task = new Task.Cpu<Integer,IOException>("Compressing data using deflate", priority, ondone) {
			@Override
			public Integer run() throws IOException {
				if (isCancelled()) return Integer.valueOf(0);
				int len = buffer.remaining();
				if (buffer.hasArray())
					deflater.setInput(buffer.array(), buffer.position(), buffer.remaining());
				else {
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
		task.start();
		return task.getOutput();
	}
	
	/** Indicates that no more data will be compressed and flushes remaining compressed data to the output. */
	public void finishSynch() throws IOException {
		AsyncWork<Integer, IOException> lastWrite = writeOps.getLastPendingOperation();
		if (lastWrite != null) {
			lastWrite.blockException(0);
		}
		deflater.finish();
		if (!deflater.finished()) {
			byte[] writeBuf = new byte[1024];
			do {
				int nb = deflater.deflate(writeBuf, 0, writeBuf.length);
				if (nb <= 0) break;
				output.writeSync(ByteBuffer.wrap(writeBuf, 0, nb));
			} while (!deflater.finished());
		}
	}
	
	/** Indicates that no more data will be compressed and flushes remaining compressed data to the output. */
	public ISynchronizationPoint<IOException> finishAsync() {
		SynchronizationPoint<IOException> result = new SynchronizationPoint<>();
		Task<Void,NoException> task = new Task.Cpu<Void,NoException>("Finishing zip compression", priority) {
			@Override
			public Void run() {
				AsyncWork<Integer, IOException> lastWrite = null;
				deflater.finish();
				if (!deflater.finished()) {
					do {
						byte[] writeBuf = new byte[1024];
						int nb = deflater.deflate(writeBuf, 0, writeBuf.length);
						if (nb <= 0) break;
						try { lastWrite = writeOps.write(ByteBuffer.wrap(writeBuf, 0, nb)); }
						catch (IOException e) {
							result.error(e);
							return null;
						}
					} while (!deflater.finished());
				}
				if (lastWrite != null)
					lastWrite.listenInline(result);
				else
					result.unblock();
				return null;
			}
		};
		task.start();
		return result;
	}

}
