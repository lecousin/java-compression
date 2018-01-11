package net.lecousin.compression.gzip;

import java.nio.ByteBuffer;

import net.lecousin.framework.application.LCCore;
import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.core.test.LCCoreAbstractTest;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.buffering.SimpleBufferedReadable;

import org.junit.Test;

public class TestGZipReadable extends LCCoreAbstractTest {

	@Test
	public void testMultipleWithFilename() throws Exception {
		testFile("gzip-test/multiple.gz");
	}
	
	@Test
	public void testWithExtraDataInHeader() throws Exception {
		testFile("gzip-test/sample.txt.gz");
	}
	
	@SuppressWarnings("resource")
	private static void testFile(String filename) throws Exception {
		IO.Readable source = LCCore.getApplication().getResource(filename, Task.PRIORITY_NORMAL);
		IO.Readable.Buffered bin = new SimpleBufferedReadable(source, 1024);
		GZipReadable gzip = new GZipReadable(bin, Task.PRIORITY_NORMAL);
		byte[] buf = new byte[4096];
		while (gzip.readFullySync(ByteBuffer.wrap(buf)) == 4096);
		gzip.close();
	}
	
}
