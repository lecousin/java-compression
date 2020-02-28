package net.lecousin.compression.gzip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.zip.CRC32;

import net.lecousin.compression.deflate.DeflateWritable;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.concurrent.threads.Task.Priority;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.IOUtil;
import net.lecousin.framework.io.util.DataUtil;
import net.lecousin.framework.util.Pair;

/**
 * GZip compression.
 */
public class GZipWritable extends DeflateWritable {

	/** Constructor. */
	public GZipWritable(IO.Writable out, Priority priority, int level, int maxPendingWrites) {
		super(out, priority, level, true, maxPendingWrites);
		writeHeader();
		crc.reset();
	}
	
	private Async<IOException> writeHeader = new Async<>();
	private CRC32 crc = new CRC32();
	
	@Override
	public IAsync<IOException> canStartWriting() {
		return writeHeader;
	}

	@Override
	public int writeSync(ByteBuffer buffer) throws IOException {
		writeHeader.blockException(0);
		int pos = buffer.position();
		crc.update(buffer);
		buffer.position(pos);
		return super.writeSync(buffer);
	}

	@Override
	public AsyncSupplier<Integer, IOException> writeAsync(ByteBuffer buffer, Consumer<Pair<Integer, IOException>> ondone) {
		if (writeHeader.hasError())
			return IOUtil.error(writeHeader.getError(), ondone);
		if (!writeHeader.isDone()) {
			AsyncSupplier<Integer, IOException> result = new AsyncSupplier<>();
			writeHeader.onDone(() -> writeAsync(buffer, ondone).forward(result));
			return operation(result);
		}
		int initPos = buffer.position();
		AsyncSupplier<Integer, IOException> write = super.writeAsync(buffer, null);
		AsyncSupplier<Integer, IOException> result = new AsyncSupplier<>();
		operation(Task.cpu("Update GZip CRC", getPriority(), () -> {
			if (write.forwardIfNotSuccessful(result))
				return null;
			int nb = write.getResult().intValue();
			if (nb <= 0) {
				result.unblockSuccess(write.getResult());
				return null;
			}
			int newPos = buffer.position();
			int limit = buffer.limit();
			buffer.position(initPos);
			buffer.limit(initPos + nb);
			crc.update(buffer);
			buffer.limit(limit);
			buffer.position(newPos);
			result.unblockSuccess(write.getResult());
			return null;
		})).startOn(write, true);
		return result;
	}

	@Override
	public void finishSynch() throws IOException {
		writeHeader.blockException(0);
		super.finishSynch();
		byte[] trailer = new byte[8];
		DataUtil.Write32U.LE.write(trailer, 0, (int)crc.getValue());
		DataUtil.Write32U.LE.write(trailer, 4, deflater.getTotalIn());
		output.writeSync(ByteBuffer.wrap(trailer));
	}

	@Override
	public IAsync<IOException> finishAsync() {
		Async<IOException> result = new Async<>();
		if (writeHeader.forwardIfNotSuccessful(result)) {
			finishing = result;
			return result;
		}
		if (!writeHeader.isDone()) {
			writeHeader.onDone(() -> {
				if (!writeHeader.isSuccessful()) {
					finishing = result;
					writeHeader.forwardIfNotSuccessful(result);
					return;
				}
				finishAsync().onDone(result);
			});
			return operation(result);
		}
		IAsync<IOException> finish = super.finishAsync();
		finish.thenStart("Write GZip trailer", getPriority(), () -> {
			byte[] trailer = new byte[8];
			DataUtil.Write32U.LE.write(trailer, 0, (int)crc.getValue());
			DataUtil.Write32U.LE.write(trailer, 4, deflater.getTotalIn());
			output.writeAsync(ByteBuffer.wrap(trailer)).onDone(result);
			return null;
		}, result);
		return operation(result);
	}
	
	private void writeHeader() {
		Task.cpu("Prepare GZip header", getPriority(), () -> {
			byte[] header = new byte[10];
			// magic number
			header[0] = 0x1F;
			header[1] = (byte)0x8B;
			// compression method
			header[2] = 8;
			// flags
			header[3] = 0;
			// mtime
			DataUtil.Write32U.LE.write(header, 4, System.currentTimeMillis() / 1000);
			// XFL
			header[8] = 0;
			// OS
			header[9] = 0;
			output.writeAsync(ByteBuffer.wrap(header)).onDone(writeHeader);
			return null;
		}).start();
	}
	
}
