package net.lecousin.compression.gzip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.core.test.LCCoreAbstractTest;
import net.lecousin.framework.core.test.io.TestIOError;

import org.junit.Test;

public class TestGZipWritableErrors extends LCCoreAbstractTest {
	
	@SuppressWarnings("resource")
	@Test
	public void testWriteErrorAlways() throws Exception {
		GZipWritable gout = new GZipWritable(new TestIOError.WritableAlwaysError(), Task.Priority.NORMAL, Deflater.BEST_COMPRESSION, 3);
		try {
			gout.writeSync(ByteBuffer.allocate(16));
			throw new AssertionError("error expected");
		} catch (IOException e) {
			// ok
		}
		try {
			gout.writeAsync(ByteBuffer.allocate(16)).blockResult(0);
			throw new AssertionError("error expected");
		} catch (IOException e) {
			// ok
		}
		try {
			gout.close();
		} catch (Exception e) {
			// ok
		}
	}

}
