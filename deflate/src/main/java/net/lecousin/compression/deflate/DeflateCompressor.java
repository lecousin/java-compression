package net.lecousin.compression.deflate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import net.lecousin.framework.concurrent.Executable;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.concurrent.threads.Task.Priority;
import net.lecousin.framework.io.IO.Readable;
import net.lecousin.framework.io.IO.Writable;
import net.lecousin.framework.io.util.LimitWriteOperations;
import net.lecousin.framework.memory.ByteArrayCache;

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
	
	private static final String TASK_NAME = "Zip compression";
	
	/** Compress from a Readable to a Writable. */
	public IAsync<Exception> compress(Readable input, Writable output, int bufferSize, int maxBuffers, Priority priority) {
		Deflater deflater = new Deflater(level, nowrap);
		ByteArrayCache cache = ByteArrayCache.getInstance();
		LimitWriteOperations limit = new LimitWriteOperations(output, maxBuffers, buf -> cache.free(buf.array()));
		byte[] bufRead = cache.get(bufferSize, false);
		ByteBuffer buffer = ByteBuffer.wrap(bufRead);
		AsyncSupplier<Integer,IOException> task = input.readAsync(buffer);
		Async<Exception> end = new Async<>();
		Task<Void, Exception> compress = Task.cpu(
			TASK_NAME, priority, new Compress(input, output, task, bufRead, cache, bufferSize, deflater, limit, end));
		end.onCancel(compress::cancel);
		task.thenStart(compress, true);
		return end;
	}
	
	private static class Compress implements Executable<Void, Exception> {
		@SuppressWarnings("squid:S00107") // 8 parameters
		private Compress(
			Readable input, Writable output, AsyncSupplier<Integer,IOException> readTask, byte[] readBuf,
			ByteArrayCache cache, int bufferSize,
			Deflater delfater, LimitWriteOperations limit, Async<Exception> end
		) {
			this.input = input;
			this.output = output;
			this.readTask = readTask;
			this.cache = cache;
			this.bufferSize = bufferSize;
			this.readBuf = readBuf;
			this.deflater = delfater;
			this.limit = limit;
			this.end = end;
			end.onCancel(readTask::cancel);
		}
		
		private Readable input;
		private Writable output;
		private AsyncSupplier<Integer,IOException> readTask;
		private ByteArrayCache cache;
		private int bufferSize;
		private byte[] readBuf;
		private Deflater deflater;
		private LimitWriteOperations limit;
		private Async<Exception> end;
		
		@Override
		public Void execute() throws Exception {
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
		
		private static class CompressStatus {
			private CompressStatus(int pos, ByteBuffer writeBuf) {
				this.pos = pos;
				this.writeBuf = writeBuf;
			}
			
			private int pos;
			private ByteBuffer writeBuf;
		}
		
		private void compress() throws IOException {
			// compress data
			int nb = readTask.getResult().intValue();
			CompressStatus status = new CompressStatus(0, ByteBuffer.wrap(cache.get(bufferSize, false)));
			if (nb <= 0) {
				// end of data
				deflater.finish();
				while (!deflater.finished()) {
					if (compressLoop(status))
						break;
				}
				deflater.end();
				deflater = null;
			} else {
				deflater.setInput(readBuf, 0, nb);
				while (!deflater.needsInput() && !end.isCancelled()) {
					if (compressLoop(status))
						break;
				}
			}
			if (end.isCancelled()) return;
			if (deflater != null && !deflater.finished()) {
				writeAndContinue(status);
			} else {
				writeAndEnd(status);
			}
		}
		
		private boolean compressLoop(CompressStatus status) throws IOException {
			if (status.writeBuf == null) status.writeBuf = ByteBuffer.wrap(cache.get(bufferSize, false));
			int nb = deflater.deflate(status.writeBuf.array(), status.pos, status.writeBuf.capacity() - status.pos);
			if (nb <= 0) return true;
			status.pos += nb;
			if (status.pos == status.writeBuf.capacity()) {
				writeCompressedData(status);
				status.pos = 0;
				status.writeBuf = null;
			}
			return false;
		}
		
		private void writeAndContinue(CompressStatus status) throws IOException {
			// write compressed data
			if (status.pos > 0)
				writeCompressedData(status);
			else if (status.writeBuf != null)
				cache.free(status.writeBuf.array());
			// next read
			AsyncSupplier<Integer,IOException> task = input.readAsync(ByteBuffer.wrap(readBuf));
			Task<Void, Exception> compress = Task.cpu(
				TASK_NAME, new Compress(input, output, task, readBuf, cache, bufferSize, deflater, limit, end));
			end.onCancel(compress::cancel);
			task.thenStart(compress, true);
		}
		
		private void writeAndEnd(CompressStatus status) throws IOException {
			// write compressed data
			AsyncSupplier<Integer, IOException> write = null;
			if (status.pos > 0)
				write = writeCompressedData(status);
			else {
				if (status.writeBuf != null)
					cache.free(status.writeBuf.array());
				write = limit.getLastPendingOperation();
			}
			if (write == null)
				end.unblock();
			else
				write.onDone(end, e -> e);
		}
		
		private AsyncSupplier<Integer,IOException> writeCompressedData(CompressStatus status) throws IOException {
			status.writeBuf.limit(status.pos);
			status.writeBuf.position(0);
			// may block to wait for writing operations
			AsyncSupplier<Integer,IOException> res = limit.write(status.writeBuf);
			if (res.hasError()) throw res.getError();
			return res;
		}
	}
	
}
