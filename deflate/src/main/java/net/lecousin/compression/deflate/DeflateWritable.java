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
import net.lecousin.framework.io.IO;
import net.lecousin.framework.util.Pair;
import net.lecousin.framework.util.RunnableWithParameter;

/**
 * Deflate compression: wrap a Writable and compress data in it.
 * One of the method finishSync or finishAsync must be called when no more data needs to be compressed.
 * It uses the {@link Deflater} provided by Java.
 */
public class DeflateWritable extends IO.AbstractIO implements IO.Writable {
	
	/** Constructor. */
	public DeflateWritable(IO.Writable output, byte priority, int level) {
		this.output = output;
		this.priority = priority;
		deflater = new Deflater(level);
	}
	
	private IO.Writable output;
	private byte priority;
	private Deflater deflater;

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
		while (!deflater.needsInput()) {
			int nb = deflater.deflate(writeBuf, 0, writeBuf.length);
			if (nb <= 0) break;
			output.writeSync(ByteBuffer.wrap(writeBuf, 0, nb));
		}
		return len;
	}
	
	@Override
	public AsyncWork<Integer,IOException> writeAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer,IOException>> ondone) {
		Task<Integer,IOException> task = new Task.Cpu<Integer,IOException>("Writing data with zip compression", priority, ondone) {
			@Override
			public Integer run() {
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
					output.writeAsync(ByteBuffer.wrap(writeBuf, 0, nb));
				}
				return Integer.valueOf(len);
			}
		};
		task.start();
		return task.getSynch();
	}
	
	/** Indicates that no more data will be compressed and flushes remaining compressed data to the output. */
	public void finishSynch() throws IOException {
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
	public Task<Void,IOException> finishAsynch() {
		Task<Void,IOException> task = new Task.Cpu<Void,IOException>("Finishing zip compression", priority) {
			@Override
			public Void run() {
				deflater.finish();
				if (!deflater.finished()) {
					byte[] writeBuf = new byte[1024];
					do {
						int nb = deflater.deflate(writeBuf, 0, writeBuf.length);
						if (nb <= 0) break;
						output.writeAsync(ByteBuffer.wrap(writeBuf, 0, nb));
					} while (!deflater.finished());
				}
				return null;
			}
		};
		task.start();
		return task;
	}

}
