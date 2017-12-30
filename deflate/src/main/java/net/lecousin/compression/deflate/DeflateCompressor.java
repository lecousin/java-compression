package net.lecousin.compression.deflate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import net.lecousin.framework.concurrent.CancelException;
import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.synch.AsyncWork;
import net.lecousin.framework.concurrent.synch.ISynchronizationPoint;
import net.lecousin.framework.concurrent.synch.SynchronizationPoint;
import net.lecousin.framework.event.Listener;
import net.lecousin.framework.io.IO.Readable;
import net.lecousin.framework.io.IO.Writable;
import net.lecousin.framework.io.util.LimitWriteOperationsReuseBuffers;

/**
 * Compress a Readable into a Writable using deflate method.
 * It uses the {@link Deflater} provided by Java.
 */
public class DeflateCompressor {

	/** Constructor.
	 * @param level one of the value defined in {@link Deflater}.
	 * @param nowrap if true then support GZIP compatible compression
	 */
	public DeflateCompressor(int level, boolean nowrap) {
		this.level = level;
		this.nowrap = nowrap;
	}
	
	/** Constructor with default compression level to BEST_COMPRESSION.
	 * @param nowrap if true then support GZIP compatible compression
	 */
	public DeflateCompressor(boolean nowrap) {
		this(Deflater.BEST_COMPRESSION, nowrap);
	}

	/** Constructor with default compression level to BEST_COMPRESSION and nowrap to false.
	 */
	public DeflateCompressor() {
		this(Deflater.BEST_COMPRESSION, false);
	}
	
	private int level;
	private boolean nowrap;
	
	/** Compress from a Readable to a Writable. */
	public ISynchronizationPoint<Exception> compress(Readable input, Writable output, int bufferSize, int maxBuffers, byte priority) {
		Deflater deflater = new Deflater(level, nowrap);
		LimitWriteOperationsReuseBuffers limit = new LimitWriteOperationsReuseBuffers(output, bufferSize, maxBuffers);
		byte[] bufRead = new byte[bufferSize];
		ByteBuffer buffer = ByteBuffer.wrap(bufRead);
		AsyncWork<Integer,IOException> task = input.readAsync(buffer);
		SynchronizationPoint<Exception> end = new SynchronizationPoint<>();
		task.listenAsync(new Compress(input, output, task, bufRead, deflater, limit, priority, end), true);
		return end;
	}
	
	private static class Compress extends Task.Cpu<Void,Exception> {
		private Compress(
			Readable input, Writable output, AsyncWork<Integer,IOException> readTask, byte[] readBuf,
			Deflater delfater, LimitWriteOperationsReuseBuffers limit, byte priority, SynchronizationPoint<Exception> end
		) {
			super("Zip compression", priority);
			this.input = input;
			this.output = output;
			this.readTask = readTask;
			this.readBuf = readBuf;
			this.deflater = delfater;
			this.limit = limit;
			this.end = end;
			end.onCancel(new Listener<CancelException>() {
				@Override
				public void fire(CancelException event) {
					readTask.unblockCancel(event);
					Compress.this.cancel(event);
				}
			});
		}
		
		private Readable input;
		private Writable output;
		private AsyncWork<Integer,IOException> readTask;
		private byte[] readBuf;
		private Deflater deflater;
		private LimitWriteOperationsReuseBuffers limit;
		private SynchronizationPoint<Exception> end;
		
		@Override
		public Void run() throws Exception {
			if (readTask.isCancelled() || end.isCancelled()) return null;
			if (!readTask.isSuccessful()) {
				end.error(readTask.getError());
				throw readTask.getError();
			}
			try {
				// compress data
				int nb = readTask.getResult().intValue();
				int pos = 0;
				ByteBuffer writeBuf = limit.getBuffer();
				if (nb <= 0) {
					// end of data
					deflater.finish();
					while (!deflater.finished()) {
						if (writeBuf == null) writeBuf = limit.getBuffer();
						nb = deflater.deflate(writeBuf.array(), pos, writeBuf.capacity() - pos);
						if (nb <= 0) break;
						pos += nb;
						if (pos == writeBuf.capacity()) {
							writeCompressedData(writeBuf, pos);
							pos = 0;
							writeBuf = null;
						}
					}
					deflater.end();
					deflater = null;
				} else {
					deflater.setInput(readBuf, 0, nb);
					while (!deflater.needsInput() && !end.isCancelled()) {
						if (writeBuf == null) writeBuf = limit.getBuffer();
						nb = deflater.deflate(writeBuf.array(), pos, writeBuf.capacity() - pos);
						if (nb <= 0) break;
						pos += nb;
						if (pos == writeBuf.capacity()) {
							writeCompressedData(writeBuf, pos);
							pos = 0;
							writeBuf = null;
						}
					}
				}
				if (end.isCancelled()) return null;
				if (deflater != null && !deflater.finished()) {
					// write compressed data
					if (pos > 0)
						writeCompressedData(writeBuf, pos);
					else if (writeBuf != null)
						limit.freeBuffer(writeBuf);
					// next read
					AsyncWork<Integer,IOException> task = input.readAsync(ByteBuffer.wrap(readBuf));
					task.listenAsync(new Compress(input, output, task, readBuf, deflater, limit, getPriority(), end), true);
				} else {
					// write compressed data
					AsyncWork<Integer, IOException> write = null;
					if (pos > 0)
						write = writeCompressedData(writeBuf, pos);
					else {
						if (writeBuf != null)
							limit.freeBuffer(writeBuf);
						write = limit.getLastPendingOperation();
					}
					if (write == null)
						end.unblock();
					else
						write.listenInline(new Runnable() {
							@Override
							public void run() {
								end.unblock();
							}
						});
				}
			} catch (Exception e) {
				end.error(e);
				throw e;
			}
			return null;
		}
		
		private AsyncWork<Integer,IOException> writeCompressedData(ByteBuffer writeBuf, int nb) throws IOException {
			writeBuf.limit(nb);
			writeBuf.position(0);
			// may block to wait for writing operations
			return limit.write(writeBuf);
		}
	}
	
}
