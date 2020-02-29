package net.lecousin.compression.lzma;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.IOUtil;
import net.lecousin.framework.memory.ByteArrayCache;
import net.lecousin.framework.memory.IntArrayCache;
import net.lecousin.framework.util.Pair;

public class LZMA1Writable extends AbstractLZMAWritable {

	private final RangeEncoderToStream rc;

	private final boolean useEndMarker;
	private final long expectedUncompressedSize;
	private long currentUncompressedSize = 0;

	private LZMA1Writable(
		IO.Writable.Buffered output, LZMA2Options options, boolean useHeader, boolean useEndMarker,
		long expectedUncompressedSize, ByteArrayCache byteArrayCache, IntArrayCache intArrayCache
	) throws IOException {
		super(output, byteArrayCache, intArrayCache);
		this.useEndMarker = useEndMarker;
		this.expectedUncompressedSize = expectedUncompressedSize;

		rc = new RangeEncoderToStream(output);

		int dictSize = options.getDictSize();
		lzma = LZMAEncoder.getInstance(rc, options.getLc(), options.getLp(), options.getPb(), options.getMode(),
				dictSize, 0, options.getNiceLen(), options.getMatchFinder(), options.getDepthLimit(), byteArrayCache, intArrayCache);

		lz = lzma.getLZEncoder();

		byte[] presetDict = options.getPresetDict();
		if (presetDict != null && presetDict.length > 0) {
			if (useHeader)
				throw new UnsupportedOptionsException(
						"Preset dictionary cannot be used in .lzma files " + "(try a raw LZMA stream instead)");

			lz.setPresetDict(dictSize, presetDict);
		}

		props = (options.getPb() * 5 + options.getLp()) * 9 + options.getLc();

		if (useHeader) {
			// Props byte stores lc, lp, and pb.
			output.write((byte)props);

			// Dictionary size is stored as a 32-bit unsigned little endian
			// integer.
			for (int i = 0; i < 4; ++i) {
				output.write((byte)(dictSize & 0xFF));
				dictSize >>>= 8;
			}

			// Uncompressed size is stored as a 64-bit unsigned little endian
			// integer. The max value (-1 in two's complement) indicates
			// unknown size.
			for (int i = 0; i < 8; ++i)
				output.write((byte) ((expectedUncompressedSize >>> (8 * i)) & 0xFF));
		}
	}

	/**
     * Creates a new compressor for the legacy .lzma file format.
     * <p>
     * If the uncompressed size of the input data is known, it will be stored
     * in the .lzma header and no end of stream marker will be used. Otherwise
     * the header will indicate unknown uncompressed size and the end of stream
     * marker will be used.
     * <p>
     * Note that a preset dictionary cannot be used in .lzma files but
     * it can be used for raw LZMA streams.
     *
     * @param       out         output stream to which the compressed data
     *                          will be written
     *
     * @param       options     LZMA compression options; the same class
     *                          is used here as is for LZMA2
     *
     * @param       inputSize   uncompressed size of the data to be compressed;
     *                          use <code>-1</code> when unknown
     *
     * @throws      IOException may be thrown from <code>out</code>
     */
    public LZMA1Writable(IO.Writable.Buffered output, LZMA2Options options, long inputSize)
    throws IOException {
        this(output, options, inputSize, ByteArrayCache.getInstance(), IntArrayCache.getInstance());
    }

	/**
     * Creates a new compressor for the legacy .lzma file format.
     * <p>
     * This is identical to
     * <code>LZMAOutputStream(OutputStream, LZMA2Options, long)</code>
     * except that this also takes the <code>arrayCache</code> argument.
     *
     * @param       out         output stream to which the compressed data
     *                          will be written
     *
     * @param       options     LZMA compression options; the same class
     *                          is used here as is for LZMA2
     *
     * @param       inputSize   uncompressed size of the data to be compressed;
     *                          use <code>-1</code> when unknown
     *
     * @param       arrayCache  cache to be used for allocating large arrays
     *
     * @throws      IOException may be thrown from <code>out</code>
     *
     * @since 1.7
     */
    public LZMA1Writable(
    	IO.Writable.Buffered output, LZMA2Options options, long inputSize, ByteArrayCache byteArrayCache, IntArrayCache intArrayCache
    ) throws IOException {
        this(output, options, true, inputSize == -1, inputSize, byteArrayCache, intArrayCache);
    }

	/**
     * Creates a new compressor for raw LZMA (also known as LZMA1) stream.
     * <p>
     * Raw LZMA streams can be encoded with or without end of stream marker.
     * When decompressing the stream, one must know if the end marker was used
     * and tell it to the decompressor. If the end marker wasn't used, the
     * decompressor will also need to know the uncompressed size.
     *
     * @param       out         output stream to which the compressed data
     *                          will be written
     *
     * @param       options     LZMA compression options; the same class
     *                          is used here as is for LZMA2
     *
     * @param       useEndMarker
     *                          if end of stream marker should be written
     *
     * @throws      IOException may be thrown from <code>out</code>
     */
    public LZMA1Writable(IO.Writable.Buffered output, LZMA2Options options, boolean useEndMarker) throws IOException {
        this(output, options, useEndMarker, ByteArrayCache.getInstance(), IntArrayCache.getInstance());
    }

	/**
     * Creates a new compressor for raw LZMA (also known as LZMA1) stream.
     * <p>
     * This is identical to
     * <code>LZMAOutputStream(OutputStream, LZMA2Options, boolean)</code>
     * except that this also takes the <code>arrayCache</code> argument.
     *
     * @param       out         output stream to which the compressed data
     *                          will be written
     *
     * @param       options     LZMA compression options; the same class
     *                          is used here as is for LZMA2
     *
     * @param       useEndMarker
     *                          if end of stream marker should be written
     *
     * @param       arrayCache  cache to be used for allocating large arrays
     *
     * @throws      IOException may be thrown from <code>out</code>
     *
     * @since 1.7
     */
    public LZMA1Writable(
    	IO.Writable.Buffered output, LZMA2Options options, boolean useEndMarker, ByteArrayCache byteArrayCache, IntArrayCache intArrayCache
    ) throws IOException {
        this(output, options, false, useEndMarker, -1, byteArrayCache, intArrayCache);
    }

	/**
	 * Returns the LZMA lc/lp/pb properties encoded into a single byte. This might
	 * be useful when handling file formats other than .lzma that use the same
	 * encoding for the LZMA properties as .lzma does.
	 */
	public int getProps() {
		return props;
	}

	/**
	 * Gets the amount of uncompressed data written to the stream. This is useful
	 * when creating raw LZMA streams without the end of stream marker.
	 */
	public long getUncompressedSize() {
		return currentUncompressedSize;
	}

	@Override
	public int writeSync(ByteBuffer buffer) throws IOException {
		if (finished)
			throw new IOException("Stream finished or closed");

		if (expectedUncompressedSize != -1 && expectedUncompressedSize - currentUncompressedSize < buffer.remaining())
			throw new IOException("Expected uncompressed input size (" + expectedUncompressedSize + " bytes) was exceeded");

		currentUncompressedSize += buffer.remaining();

		int done = 0;
		while (buffer.hasRemaining()) {
			int used = lz.fillWindow(buffer);
            done += used;
			lzma.encodeForLZMA1();
		}
		return done;
	}
	
	@Override
    public AsyncSupplier<Integer, IOException> writeAsync(ByteBuffer buffer, Consumer<Pair<Integer, IOException>> ondone) {
        if (finished)
            return IOUtil.error(new IOException("Stream finished or closed"), ondone);
		if (expectedUncompressedSize != -1 && expectedUncompressedSize - currentUncompressedSize < buffer.remaining())
			return IOUtil.error(new IOException("Expected uncompressed input size (" + expectedUncompressedSize + " bytes) was exceeded"), ondone);

		currentUncompressedSize += buffer.remaining();
		
        AsyncSupplier<Integer, IOException> result = new AsyncSupplier<>();
        TaskUtil.compressionTask(output, () -> writeAsync(buffer, 0, result, ondone)).start();
        return result;
    }
	
	private void writeAsync(ByteBuffer buffer, int done, AsyncSupplier<Integer, IOException> result, Consumer<Pair<Integer, IOException>> ondone) {
        while (buffer.hasRemaining()) {
            int used = lz.fillWindow(buffer);
            done += used;

            try {
            	lzma.encodeForLZMA1();
            } catch (IOException e) {
            	IOUtil.error(e, result, ondone);
            	return;
            }
        }
        IOUtil.success(Integer.valueOf(done), result, ondone);
    }

	/**
	 * Finishes the stream without closing the underlying OutputStream.
	 */
	@Override
	public void finishSync() throws IOException {
		if (!finished) {
			if (expectedUncompressedSize != -1 && expectedUncompressedSize != currentUncompressedSize)
				throw new IOException(
						"Expected uncompressed size (" + expectedUncompressedSize + ") doesn't equal "
								+ "the number of bytes written to the stream (" + currentUncompressedSize + ")");

			lz.setFinishing();
			lzma.encodeForLZMA1();

			if (useEndMarker)
				lzma.encodeLZMA1EndMarker();

			rc.finish();

			finished = true;

			lzma.putArraysToCache(byteArrayCache, intArrayCache);
			lzma = null;
			lz = null;
			output.flush().block(0);
		}
	}
	
	@Override
	public IAsync<IOException> finishAsync() {
		return Task.cpu("Finishing LZMA1 compression", (Task<Void, IOException> t) -> {
			finishSync();
			return null;
		}).start().getOutput();
	}

}
