package net.lecousin.compression.mszip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.zip.Inflater;

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
import net.lecousin.framework.memory.ByteArrayCache;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;

/**
 * MSZip format is composed of blocks of maximum 32K of uncompressed data, starting with a 2 bytes signature (CK), 
 * then deflate compressed data.
 * Each block can be uncompressed independently, so this implementation tries to uncompress 2 blocks concurrently.
 * The MSZip compression cannot be used as an usual compression method, because it relies on <i>blocks</i> but
 * does not allow to detect blocks, so it needs to be encapsulated in a format that delimits blocks of data.
 */
public class MSZipReadable extends ConcurrentCloseable<IOException> implements IO.Readable.Buffered {
	
	/** Interface to implement in order to provide block of compressed data. */
	public static interface BlockProvider {
		/** Return the next block of data, or null if this is the end. */
		AsyncSupplier<ByteBuffer,IOException> readNextBlock();
		
		/** Description. */
		String getSourceDescription();
		
		/** Underlying IO. */
		IO getWrappedIO();
		
		/** Close. */
		IAsync<IOException> closeAsync();
	}

	/** MSZipReadable with a known uncompressed size. */
	public static class SizeKnown extends MSZipReadable implements IO.KnownSize {
		/** Constructor. */
		public SizeKnown(BlockProvider input, Priority priority, long uncompressedSize) {
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
	public MSZipReadable(BlockProvider input, Priority priority) {
		this.input = input;
		this.priority = priority;
		nextUncompress = null;
		uncompress = new BlockUncompressor(1);
	}
	
	private BlockProvider input;
	private Priority priority;
	private BlockUncompressor uncompress;
	private BlockUncompressor nextUncompress;
	private IOException error = null;
	private boolean eof = false;
	
	private class BlockUncompressor {
		
		private int blockIndex;
		private Inflater inflater;
		private AsyncSupplier<ByteBuffer,IOException> read;
		private byte[] uncompressed;
		private int pos = 0;
		private int size = 0;
		private Async<IOException> dataReady;

		public BlockUncompressor(int blockIndex) {
			inflater = new Inflater(true);
			read = input.readNextBlock();
			dataReady = new Async<>();
			this.blockIndex = blockIndex;
			uncompressed = new byte[32768];
			read.thenStart("Start uncompressing MSZIP block", priority, (Task<Void, NoException> task) -> {
				if (read.hasError()) {
					error = read.getError();
					dataReady.unblock();
					return null;
				}
				ByteBuffer comp = read.getResult();
				if (comp == null) {
					eof = true;
					dataReady.unblock();
					return null;
				}
				if (comp.remaining() < 2) {
					error = new IOException("Invalid MSZIP: " + comp.remaining() + " byte read in block " + blockIndex);
					dataReady.unblock();
					return null;
				}
				byte[] compressed = comp.array();
				int nb = comp.remaining();
				int p = comp.arrayOffset() + comp.position();
				if (compressed[p] != 'C' || compressed[p + 1] != 'K') {
					error = new IOException("Invalid MSZIP: no CK signature in block " + blockIndex);
					dataReady.unblock();
					return null;
				}
				synchronized (MSZipReadable.this) {
					if (BlockUncompressor.this == MSZipReadable.this.uncompress) {
						// start reading the next block
						nextUncompress = new BlockUncompressor(blockIndex + 1);
					}
				}
				inflater.setInput(compressed, p + 2, nb - 2);
				int n;
				try {
					do {
						while ((n = inflater.inflate(uncompressed, size, 32768 - size)) == 0) {
							if (inflater.finished() || inflater.needsDictionary()) {
								inflater.end();
								inflater = null;
								dataReady.unblock();
								return null;
							}
							if (inflater.needsInput()) {
								inflater.end();
								inflater = null;
								error = new IOException("Truncated MSZIP data");
								dataReady.unblock();
								return null;
							}
						}
						size += n;
					} while (true);
				} catch (Exception t) {
					error = new IOException("Invalid deflated data in MSZip block " + blockIndex, t);
					if (inflater != null) {
						inflater.end();
						inflater = null;
					}
					dataReady.unblock();
					return null;
				}
			}, true);
		}
	}
	
	@Override
	@SuppressWarnings("squid:S2583") // error can change while waiting for dataReady
	public int read() throws IOException {
		if (error != null) throw error;
		// wait for current block to have some data uncompressed
		uncompress.dataReady.block(0);
		if (error != null) throw error;
		if (uncompress.pos < uncompress.size)
			return uncompress.uncompressed[uncompress.pos++] & 0xFF;
		synchronized (this) {
			// current block is completely read
			if (nextUncompress == null)
				return -1;
			uncompress = nextUncompress;
			if (!eof)
				nextUncompress = new BlockUncompressor(uncompress.blockIndex + 1);
		}
		return read();
	}
	
	@Override
	@SuppressWarnings("squid:S2583") // error can change while waiting for dataReady
	public int read(byte[] buffer, int offset, int len) throws IOException {
		if (error != null) throw error;
		// wait for current block to have some data uncompressed
		uncompress.dataReady.block(0);
		if (error != null) throw error;
		if (uncompress.pos < uncompress.size) {
			int l = uncompress.size - uncompress.pos;
			if (l > len) l = len;
			System.arraycopy(uncompress.uncompressed, uncompress.pos, buffer, offset, l);
			uncompress.pos += l;
			return l;
		}
		// current block is completely read
		synchronized (this) {
			if (nextUncompress == null)
				return -1;
			uncompress = nextUncompress;
			if (!eof)
				nextUncompress = new BlockUncompressor(uncompress.blockIndex + 1);
		}
		return read(buffer, offset, len);
	}
	
	@Override
	public int readFully(byte[] buffer) throws IOException {
		return IOUtil.readFully(this, buffer);
	}
	
	@Override
	@SuppressWarnings("squid:S2583") // error can change while waiting for dataReady
	public int readSync(ByteBuffer buffer) throws IOException {
		if (error != null) throw error;
		// wait for current block to have some data uncompressed
		uncompress.dataReady.block(0);
		if (error != null) throw error;
		if (uncompress.pos < uncompress.size) {
			int l = uncompress.size - uncompress.pos;
			if (l > buffer.remaining()) l = buffer.remaining();
			buffer.put(uncompress.uncompressed, uncompress.pos, l);
			uncompress.pos += l;
			return l;
		}
		synchronized (this) {
			// current block is completely read
			if (nextUncompress == null)
				return -1;
			uncompress = nextUncompress;
			if (!eof)
				nextUncompress = new BlockUncompressor(uncompress.blockIndex + 1);
		}
		return readSync(buffer);
	}
	
	@Override
	public int readFullySync(ByteBuffer buffer) throws IOException {
		return IOUtil.readFully(this, buffer);
	}
	
	@Override
	public AsyncSupplier<Integer, IOException> readFullySyncIfPossible(ByteBuffer buffer, Consumer<Pair<Integer, IOException>> ondone) {
		int done = 0;
		do {
			if (error != null)
				return IOUtil.error(error, ondone);
			if (!uncompress.dataReady.isDone()) {
				if (done == 0)
					return readFullyAsync(buffer, ondone);
				return continueAsynchronously(done, buffer, ondone);
			}
			if (uncompress.pos < uncompress.size) {
				int l = uncompress.size - uncompress.pos;
				if (l > buffer.remaining()) l = buffer.remaining();
				buffer.put(uncompress.uncompressed, uncompress.pos, l);
				uncompress.pos += l;
				done += l;
				if (!buffer.hasRemaining())
					return IOUtil.success(Integer.valueOf(done), ondone);
			}
			synchronized (this) {
				// current block is completely read
				if (nextUncompress == null)
					return IOUtil.success(Integer.valueOf(done > 0 ? done : -1), ondone);
				uncompress = nextUncompress;
				if (!eof)
					nextUncompress = new BlockUncompressor(uncompress.blockIndex + 1);
			}
		} while (true);
	}
	
	private AsyncSupplier<Integer, IOException> continueAsynchronously(int d, ByteBuffer buffer, Consumer<Pair<Integer, IOException>> ondone) {
		AsyncSupplier<Integer, IOException> r = new AsyncSupplier<>();
		readFullyAsync(buffer, res -> {
			if (ondone == null) return;
			if (res.getValue1() == null) ondone.accept(res);
			else {
				int n = res.getValue1().intValue();
				if (n < 0) n = d;
				else n += d;
				ondone.accept(new Pair<>(Integer.valueOf(n), null));
			}
		}).onDone(nb -> {
			int n = nb.intValue();
			if (n < 0) n = d;
			else n += d;
			r.unblockSuccess(Integer.valueOf(n));
		}, r);
		return r;
	}
	
	@Override
	public int readAsync() throws IOException {
		if (error != null) throw error;
		// wait for current block to have some data uncompressed
		if (!uncompress.dataReady.isDone()) return -2;
		if (uncompress.pos < uncompress.size)
			return uncompress.uncompressed[uncompress.pos++] & 0xFF;
		synchronized (this) {
			// current block is completely read
			if (nextUncompress == null)
				return -1;
			uncompress = nextUncompress;
			if (!eof)
				nextUncompress = new BlockUncompressor(uncompress.blockIndex + 1);
		}
		return readAsync();
	}
	
	@Override
	public AsyncSupplier<Integer, IOException> readAsync(ByteBuffer buffer, Consumer<Pair<Integer, IOException>> ondone) {
		if (error != null) {
			if (ondone != null) ondone.accept(new Pair<>(null, error));
			return new AsyncSupplier<>(null, error);
		}
		// wait for current block to have some data uncompressed
		AsyncSupplier<Integer, IOException> result = new AsyncSupplier<>();
		uncompress.dataReady.thenStart("Read data from MSZip", priority, task -> {
			if (error != null) {
				if (ondone != null) ondone.accept(new Pair<>(null, error));
				result.error(error);
				return null;
			}
			if (uncompress.pos < uncompress.size) {
				int l = uncompress.size - uncompress.pos;
				if (l > buffer.remaining()) l = buffer.remaining();
				buffer.put(uncompress.uncompressed, uncompress.pos, l);
				uncompress.pos += l;
				if (ondone != null) ondone.accept(new Pair<>(Integer.valueOf(l), null));
				result.unblockSuccess(Integer.valueOf(l));
				return null;
			}
			// current block is completely read
			if (nextUncompress == null) {
				if (ondone != null) ondone.accept(new Pair<>(Integer.valueOf(-1), null));
				result.unblockSuccess(Integer.valueOf(-1));
				return null;
			}
			synchronized (MSZipReadable.this) {
				uncompress = nextUncompress;
				if (!eof)
					nextUncompress = new BlockUncompressor(uncompress.blockIndex + 1);
			}
			readAsync(buffer, ondone).forward(result);
			return null;
		}, true);
		return operation(result);
	}
	
	@Override
	public AsyncSupplier<Integer, IOException> readFullyAsync(ByteBuffer buffer, Consumer<Pair<Integer, IOException>> ondone) {
		return operation(IOUtil.readFullyAsync(this, buffer, ondone));
	}
	
	@Override
	public AsyncSupplier<ByteBuffer, IOException> readNextBufferAsync(Consumer<Pair<ByteBuffer, IOException>> ondone) {
		AsyncSupplier<ByteBuffer, IOException> result = new AsyncSupplier<>();
		ByteBuffer buffer = ByteBuffer.wrap(ByteArrayCache.getInstance().get(32768, false));
		AsyncSupplier<Integer, IOException> read = readAsync(buffer);
		read.onDone(() -> {
			if (read.hasError()) {
				if (ondone != null) ondone.accept(new Pair<>(null, read.getError()));
				result.error(read.getError());
				return;
			}
			buffer.flip();
			if (ondone != null) ondone.accept(new Pair<>(buffer, null));
			result.unblockSuccess(buffer);
		});
		return result;
	}
	
	@Override
	public ByteBuffer readNextBuffer() throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(ByteArrayCache.getInstance().get(32768, false));
		int nb = readSync(buffer);
		if (nb <= 0) return null;
		buffer.flip();
		return buffer;
	}

	@Override
	public int skip(int skip) throws IOException {
		return (int)skipSync(skip);
	}

	@Override
	public long skipSync(long n) throws IOException {
		return IOUtil.skipSyncByReading(this, n);
	}

	@Override
	public AsyncSupplier<Long, IOException> skipAsync(long n, Consumer<Pair<Long, IOException>> ondone) {
		return operation(IOUtil.skipAsyncByReading(this, n, ondone));
	}

	@Override
	public String getSourceDescription() {
		return "MSZIP: " + input.getSourceDescription();
	}

	@Override
	public IO getWrappedIO() {
		return input.getWrappedIO();
	}

	@Override
	public Priority getPriority() {
		return priority;
	}

	@Override
	public void setPriority(Priority priority) {
		this.priority = priority;
	}

	@Override
	public TaskManager getTaskManager() {
		return Threading.getCPUTaskManager();
	}

	@Override
	public IAsync<IOException> canStartReading() {
		return uncompress.dataReady;
	}

	@Override
	protected IAsync<IOException> closeUnderlyingResources() {
		return input.closeAsync();
	}
	
	@Override
	protected void closeResources(Async<IOException> ondone) {
		input = null;
		ondone.unblock();
	}
	
}
