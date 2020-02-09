package net.lecousin.compression.gzip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.zip.Inflater;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.util.AsyncConsumer;
import net.lecousin.framework.memory.ByteArrayCache;
import net.lecousin.framework.text.StringUtil;

/** Consumer of GZip data, and call another consumer to read unzipped data. */
public class GZipConsumer implements AsyncConsumer<ByteBuffer, IOException> {

	/** Constructor. */
	public GZipConsumer(int unzipBufferSize, AsyncConsumer<ByteBuffer, IOException> unzipConsumer) {
		this.unzipConsumer = unzipConsumer;
		header = new HeaderConsumer();
		inflater = new Inflater(true);
		this.unzipBufferSize = unzipBufferSize;
	}

	/** Constructor. */
	public GZipConsumer(AsyncConsumer<ByteBuffer, IOException> unzipConsumer) {
		this(8192, unzipConsumer);
	}
	
	private AsyncConsumer<ByteBuffer, IOException> unzipConsumer;
	private Inflater inflater;
	private HeaderConsumer header;
	private int unzipBufferSize;
	private int trailerNeeded = 0;
	private ByteArrayCache cache = ByteArrayCache.getInstance();
	
	@Override
	public IAsync<IOException> end() {
		inflater.end();
		return unzipConsumer.end();
	}
	
	@Override
	public void error(IOException error) {
		inflater.end();
		unzipConsumer.error(error);
	}
	
	@Override
	public IAsync<IOException> consume(ByteBuffer data, Consumer<ByteBuffer> onDataRelease) {
		Async<IOException> result = new Async<>();
		consume(data, result, onDataRelease, true);
		return result;
	}
	
	private void consume(ByteBuffer data, Async<IOException> result, Consumer<ByteBuffer> onDataRelease, boolean setInput) {
		if (header != null) {
			try {
				if (!header.consume(data)) {
					// need data for header
					if (onDataRelease != null) onDataRelease.accept(data);
					result.unblock();
					return;
				}
			} catch (IOException e) {
				if (onDataRelease != null) onDataRelease.accept(data);
				result.error(e);
				inflater.end();
				unzipConsumer.error(e);
				return;
			}
			header = null;
			if (!data.hasRemaining()) {
				if (onDataRelease != null) onDataRelease.accept(data);
				result.unblock();
				return;
			}
		}
		
		if (trailerNeeded > 0) {
			int l = Math.min(trailerNeeded, data.remaining());
			data.position(data.position() + l);
			trailerNeeded -= l;
			if (trailerNeeded == 0) {
				header = new HeaderConsumer();
				if (!data.hasRemaining()) {
					if (onDataRelease != null) onDataRelease.accept(data);
					result.unblock();
					return;
				}
			} else {
				if (!data.hasRemaining() && onDataRelease != null) onDataRelease.accept(data);
				result.unblock();
				return;
			}
		}
		
		// send data to inflater
		if (setInput) {
			if (data.hasArray()) {
				inflater.setInput(data.array(), data.arrayOffset() + data.position(), data.remaining());
			} else {
				byte[] b = new byte[data.remaining()];
				data.get(b);
				inflater.setInput(b);
			}
		}

		// inflate
		byte[] unzipBuffer = cache.get(unzipBufferSize, true);
		int n;
		try { n = inflater.inflate(unzipBuffer); }
		catch (Exception e) {
			inflater.end();
			IOException err = new IOException("Invalid GZip data", e);
			unzipConsumer.error(err);
			if (onDataRelease != null) onDataRelease.accept(data);
			result.error(err);
			return;
		}
		if (n > 0) {
			IAsync<IOException> consume = unzipConsumer.consume(ByteBuffer.wrap(unzipBuffer, 0, n), b -> cache.free(b.array()));
			consume.thenStart("Consume unzipped buffer", Task.PRIORITY_NORMAL, () -> afterInflate(data, result, onDataRelease), result);
			return;
		}
		afterInflate(data, result, onDataRelease);
	}
	
	private void afterInflate(ByteBuffer data, Async<IOException> result, Consumer<ByteBuffer> onDataRelease) {
		data.position(data.limit() - inflater.getRemaining());
        if (inflater.finished() || inflater.needsDictionary()) {
        	trailerNeeded = 8;
        	inflater.reset();
        	if (!data.hasRemaining()) {
    			if (onDataRelease != null)
    				onDataRelease.accept(data);
        		result.unblock();
        	} else {
        		consume(data, result, onDataRelease, true);
        	}
        	return;
        }
        if (inflater.needsInput()) {
			if (onDataRelease != null)
				onDataRelease.accept(data);
       		result.unblock();
        	return;
        }
    	if (!data.hasRemaining()) {
			if (onDataRelease != null)
				onDataRelease.accept(data);
    		result.unblock();
    	} else {
    		consume(data, result, onDataRelease, false);
    	}
	}
	
	private static class HeaderConsumer {
		
		private int pos = 0;
		private int toSkip = 0;
		private byte flags;
		private int shortLen = 0;
		private int shortLenPos = 0;
		
		@SuppressWarnings("squid:S3776") // complexity
		public boolean consume(ByteBuffer data) throws IOException {
			while (data.hasRemaining()) {
				if (pos <= 3) {
					readFirstBytes(data.get());
					pos++;
					continue;
				}
				if (pos <= 9) {
					// skip MTIME + XFL + OS
					int l = Math.min(6, data.remaining());
					data.position(data.position() + l);
					pos += l;
					if (l < 6) break;
				}
				if (pos == 10) {
					// extra data
					if ((flags & 4) != 0 && !skipExtra(data))
						return false;
					pos++;
				}
				if (pos == 11) {
					// filename
					if ((flags & 8) != 0 && !skipString(data))
						return false;
					pos++;
				}
				if (pos == 12) {
					// comment
					if ((flags & 16) != 0 && !skipString(data))
						return false;
					pos++;
				}
				// crc
				if ((flags & 2) != 0) {
					if (toSkip == 1) {
						data.position(data.position() + 1);
						return true;
					}
					if (data.remaining() == 1) {
						data.position(data.position() + 1);
						toSkip = 1;
						return false;
					}
					data.position(data.position() + 2);
				}
				return true;
			}
			return false;
		}
		
		private void readFirstBytes(byte b) throws IOException {
			switch (pos) {
			case 0:
				if (b != 0x1F)
					throw new IOException("Invalid GZIP header: first byte must be 1F, "
						+ StringUtil.encodeHexa(b) + " found");
				break;
			case 1:
				if (b != (byte)0x8B)
					throw new IOException("Invalid GZIP header: second byte must be 8B, "
						+ StringUtil.encodeHexa(b) + " found");
				break;
			case 2:
				if (b != 8)
					throw new IOException("Unsupported compression method " + b
						+ " for GZIP, only method 8 (deflate) is supported");
				break;
			case 3:
				flags = b;
				break;
			default: break; // not possible
			}
		}

		private boolean skipExtra(ByteBuffer data) {
			do {
				switch (shortLenPos) {
				case 0:
					shortLen = data.get() & 0xFF;
					shortLenPos++;
					if (!data.hasRemaining())
						return false;
					break;
				case 1:
					toSkip = (shortLen << 8) | (data.get() & 0xFF);
					shortLenPos++;
					if (!data.hasRemaining())
						return false;
					break;
				case 2:
					int l = Math.min(toSkip, data.remaining());
					data.position(data.position() + l);
					toSkip -= l;
					if (toSkip > 0)
						return false;
					shortLenPos = 0;
					if (!data.hasRemaining()) {
						pos++;
						return false;
					}
					return true;
				default: return true; // not possible
				}
			} while (true);
		}
		
		private boolean skipString(ByteBuffer data) {
			do {
				if (data.get() == 0) {
					if (!data.hasRemaining()) {
						pos++;
						return false;
					}
					return true;
				}
			} while (data.hasRemaining());
			return false;
		}
		
	}
	
}
