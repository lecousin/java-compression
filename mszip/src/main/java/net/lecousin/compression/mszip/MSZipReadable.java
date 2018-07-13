package net.lecousin.compression.mszip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Inflater;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.TaskManager;
import net.lecousin.framework.concurrent.Threading;
import net.lecousin.framework.concurrent.synch.AsyncWork;
import net.lecousin.framework.concurrent.synch.ISynchronizationPoint;
import net.lecousin.framework.concurrent.synch.SynchronizationPoint;
import net.lecousin.framework.exception.NoException;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.IOUtil;
import net.lecousin.framework.util.ConcurrentCloseable;
import net.lecousin.framework.util.Pair;
import net.lecousin.framework.util.RunnableWithParameter;

/**
 * MSZip format is composed of blocks of maximum 32K of uncompressed data, starting with a 2 bytes signature (CK), 
 * then deflate compressed data.
 * Each block can be uncompressed independently, so this implementation tries to uncompress 2 blocks concurrently.
 * The MSZip compression cannot be used as an usual compression method, because it relies on <i>blocks</i> but
 * does not allow to detect blocks, so it needs to be encapsulated in a format that delimits blocks of data.
 */
public class MSZipReadable extends ConcurrentCloseable implements IO.Readable.Buffered {
	
	/** Interface to implement in order to provide block of compressed data. */
	public static interface BlockProvider {
		/** Return the next block of data, or null if this is the end. */
		AsyncWork<ByteBuffer,IOException> readNextBlock();
		
		/** Description. */
		String getSourceDescription();
		
		/** Underlying IO. */
		IO getWrappedIO();
		
		/** Close. */
		ISynchronizationPoint<?> closeAsync();
	}

	/** MSZipReadable with a known uncompressed size. */
	public static class SizeKnown extends MSZipReadable implements IO.KnownSize {
		/** Constructor. */
		public SizeKnown(BlockProvider input, byte priority, long uncompressedSize) {
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
	public MSZipReadable(BlockProvider input, byte priority) {
		this.input = input;
		this.priority = priority;
		nextUncompress = null;
		uncompress = new BlockUncompressor(1);
	}
	
	private BlockProvider input;
	private byte priority;
	private BlockUncompressor uncompress;
	private BlockUncompressor nextUncompress;
	private IOException error = null;
	private boolean eof = false;
	
	private class BlockUncompressor {
		public BlockUncompressor(int blockIndex) {
			inflater = new Inflater(true);
			read = input.readNextBlock();
			dataReady = new SynchronizationPoint<>();
			this.blockIndex = blockIndex;
			uncompressed = new byte[32768];
			read.listenAsync(new StartUncompress(), true);
		}
		
		private int blockIndex;
		private Inflater inflater;
		private AsyncWork<ByteBuffer,IOException> read;
		private byte[] uncompressed;
		private int pos = 0;
		private int size = 0;
		private SynchronizationPoint<IOException> dataReady;
		
		private class StartUncompress extends Task.Cpu<Void,NoException> {
			private StartUncompress() {
				super("Start uncompressing MSZIP block", priority);
			}
			
			@Override
			public Void run() {
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
				int pos = comp.arrayOffset() + comp.position();
				if (compressed[pos] != 'C' || compressed[pos + 1] != 'K') {
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
				inflater.setInput(compressed, pos + 2, nb - 2);
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
				} catch (Throwable t) {
					error = new IOException("Invalid deflated data in MSZip block " + blockIndex, t);
					inflater.end();
					inflater = null;
					dataReady.unblock();
					return null;
				}
			}
		}
	}
	
	@Override
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
	public AsyncWork<Integer, IOException> readFullySyncIfPossible(ByteBuffer buffer, RunnableWithParameter<Pair<Integer, IOException>> ondone) {
		int done = 0;
		do {
			if (error != null) {
				if (ondone != null) ondone.run(new Pair<>(null, error));
				return new AsyncWork<>(null, error);
			}
			if (!uncompress.dataReady.isUnblocked()) {
				if (done == 0)
					return readFullyAsync(buffer, ondone);
				AsyncWork<Integer, IOException> r = new AsyncWork<>();
				int d = done;
				readFullyAsync(buffer, (res) -> {
					if (ondone == null) return;
					if (res.getValue1() == null) ondone.run(res);
					else {
						int n = res.getValue1().intValue();
						if (n < 0) n = d;
						else n += d;
						ondone.run(new Pair<>(Integer.valueOf(n), null));
					}
				}).listenInline((nb) -> {
					int n = nb.intValue();
					if (n < 0) n = d;
					else n += d;
					r.unblockSuccess(Integer.valueOf(n));
				}, r);
				return r;
			}
			if (uncompress.pos < uncompress.size) {
				int l = uncompress.size - uncompress.pos;
				if (l > buffer.remaining()) l = buffer.remaining();
				buffer.put(uncompress.uncompressed, uncompress.pos, l);
				uncompress.pos += l;
				done += l;
				if (!buffer.hasRemaining())
					return new AsyncWork<>(Integer.valueOf(done), null);
			}
			synchronized (this) {
				// current block is completely read
				if (nextUncompress == null)
					return new AsyncWork<>(Integer.valueOf(done > 0 ? done : -1), null);
				uncompress = nextUncompress;
				if (!eof)
					nextUncompress = new BlockUncompressor(uncompress.blockIndex + 1);
			}
		} while (true);
	}
	
	@Override
	public int readAsync() throws IOException {
		if (error != null) throw error;
		// wait for current block to have some data uncompressed
		if (!uncompress.dataReady.isUnblocked()) return -2;
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
	public AsyncWork<Integer, IOException> readAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer, IOException>> ondone) {
		if (error != null) {
			if (ondone != null) ondone.run(new Pair<>(null, error));
			return new AsyncWork<>(null, error);
		}
		// wait for current block to have some data uncompressed
		AsyncWork<Integer, IOException> result = new AsyncWork<>();
		uncompress.dataReady.listenAsync(new Task.Cpu<Void,NoException>("Read data from MSZip", priority) {
			@Override
			public Void run() {
				if (error != null) {
					if (ondone != null) ondone.run(new Pair<>(null, error));
					result.error(error);
					return null;
				}
				if (uncompress.pos < uncompress.size) {
					int l = uncompress.size - uncompress.pos;
					if (l > buffer.remaining()) l = buffer.remaining();
					buffer.put(uncompress.uncompressed, uncompress.pos, l);
					uncompress.pos += l;
					if (ondone != null) ondone.run(new Pair<>(Integer.valueOf(l), null));
					result.unblockSuccess(Integer.valueOf(l));
					return null;
				}
				// current block is completely read
				if (nextUncompress == null) {
					if (ondone != null) ondone.run(new Pair<>(Integer.valueOf(-1), null));
					result.unblockSuccess(Integer.valueOf(-1));
					return null;
				}
				synchronized (MSZipReadable.this) {
					uncompress = nextUncompress;
					if (!eof)
						nextUncompress = new BlockUncompressor(uncompress.blockIndex + 1);
				}
				readAsync(buffer, ondone).listenInline(result);
				return null;
			}
		}, true);
		return operation(result);
	}
	
	@Override
	public AsyncWork<Integer, IOException> readFullyAsync(ByteBuffer buffer, RunnableWithParameter<Pair<Integer, IOException>> ondone) {
		return operation(IOUtil.readFullyAsync(this, buffer, ondone));
	}
	
	@Override
	public AsyncWork<ByteBuffer, IOException> readNextBufferAsync(RunnableWithParameter<Pair<ByteBuffer, IOException>> ondone) {
		AsyncWork<ByteBuffer, IOException> result = new AsyncWork<>();
		ByteBuffer buffer = ByteBuffer.allocate(32768);
		AsyncWork<Integer, IOException> read = readAsync(buffer);
		read.listenInline(new Runnable() {
			@Override
			public void run() {
				if (read.hasError()) {
					if (ondone != null) ondone.run(new Pair<>(null, read.getError()));
					result.error(read.getError());
					return;
				}
				buffer.flip();
				if (ondone != null) ondone.run(new Pair<>(buffer, null));
				result.unblockSuccess(buffer);
			}
		});
		return result;
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
	public AsyncWork<Long, IOException> skipAsync(long n, RunnableWithParameter<Pair<Long, IOException>> ondone) {
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
	public byte getPriority() {
		return priority;
	}

	@Override
	public void setPriority(byte priority) {
		this.priority = priority;
	}

	@Override
	public TaskManager getTaskManager() {
		return Threading.getCPUTaskManager();
	}

	@Override
	public ISynchronizationPoint<IOException> canStartReading() {
		return uncompress.dataReady;
	}

	@Override
	protected ISynchronizationPoint<?> closeUnderlyingResources() {
		return input.closeAsync();
	}
	
	@Override
	protected void closeResources(SynchronizationPoint<Exception> ondone) {
		input = null;
		ondone.unblock();
	}
	
}
