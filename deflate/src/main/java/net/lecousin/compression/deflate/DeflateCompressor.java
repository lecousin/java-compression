package net.lecousin.compression.deflate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
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
	public IAsync<Exception> compress(Readable input, Writable output, int bufferSize, int maxBuffers, byte priority) {
		Deflater deflater = new Deflater(level, nowrap);
		LimitWriteOperationsReuseBuffers limit = new LimitWriteOperationsReuseBuffers(output, bufferSize, maxBuffers);
		byte[] bufRead = new byte[bufferSize];
		ByteBuffer buffer = ByteBuffer.wrap(bufRead);
		AsyncSupplier<Integer,IOException> task = input.readAsync(buffer);
		Async<Exception> end = new Async<>();
		task.thenStart(new Compress(input, output, task, bufRead, deflater, limit, priority, end), true);
		return end;
	}
	
	private static class Compress extends Task.Cpu<Void,Exception> {
		@SuppressWarnings("squid:S00107") // 8 parameters
		private Compress(
			Readable input, Writable output, AsyncSupplier<Integer,IOException> readTask, byte[] readBuf,
			Deflater delfater, LimitWriteOperationsReuseBuffers limit, byte priority, Async<Exception> end
		) {
			super("Zip compression", priority);
			this.input = input;
			this.output = output;
			this.readTask = readTask;
			this.readBuf = readBuf;
			this.deflater = delfater;
			this.limit = limit;
			this.end = end;
			end.onCancel(event -> {
				readTask.unblockCancel(event);
				Compress.this.cancel(event);
			});
		}
		
		private Readable input;
		private Writable output;
		private AsyncSupplier<Integer,IOException> readTask;
		private byte[] readBuf;
		private Deflater deflater;
		private LimitWriteOperationsReuseBuffers limit;
		private Async<Exception> end;
		
		@Override
		public Void run() throws Exception {
			if (readTask.isCancelled() || end.isCancelled()) return null;
			if (!readTask.isSuccessful()) {
				end.error(readTask.getError());
				throw readTask.getError();
			}
			try {
				compress();
			} catch (Exception e) {
				end.error(e);
				throw e;
			}
			return null;
		}
		
		private void compress() throws IOException {
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
			if (end.isCancelled()) return;
			if (deflater != null && !deflater.finished()) {
				writeAndContinue(pos, writeBuf);
			} else {
				writeAndEnd(pos, writeBuf);
			}
		}
		
		private void writeAndContinue(int pos, ByteBuffer writeBuf) throws IOException {
			// write compressed data
			if (pos > 0)
				writeCompressedData(writeBuf, pos);
			else if (writeBuf != null)
				limit.freeBuffer(writeBuf);
			// next read
			AsyncSupplier<Integer,IOException> task = input.readAsync(ByteBuffer.wrap(readBuf));
			task.thenStart(new Compress(input, output, task, readBuf, deflater, limit, getPriority(), end), true);
		}
		
		private void writeAndEnd(int pos, ByteBuffer writeBuf) throws IOException {
			// write compressed data
			AsyncSupplier<Integer, IOException> write = null;
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
				write.onDone(end::unblock);
		}
		
		private AsyncSupplier<Integer,IOException> writeCompressedData(ByteBuffer writeBuf, int nb) throws IOException {
			writeBuf.limit(nb);
			writeBuf.position(0);
			// may block to wait for writing operations
			return limit.write(writeBuf);
		}
	}
	
}
