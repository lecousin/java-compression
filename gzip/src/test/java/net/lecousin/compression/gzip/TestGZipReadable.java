package net.lecousin.compression.gzip;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import net.lecousin.framework.application.LCCore;
import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.core.test.LCCoreAbstractTest;
import net.lecousin.framework.core.test.runners.LCConcurrentRunner;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.buffering.SimpleBufferedReadable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(LCConcurrentRunner.Parameterized.class) @org.junit.runners.Parameterized.UseParametersRunnerFactory(LCConcurrentRunner.ConcurrentParameterizedRunnedFactory.class)
public class TestGZipReadable extends LCCoreAbstractTest {

	private static final String[] files = {
		"gzip-test/multiple.gz",
		"gzip-test/multiple_with_comment.gz",
		"gzip-test/multiple_with_empty_extra.gz",
		"gzip-test/sample.txt.gz"
	};
	
	@Parameters(name = "file = {0}, bufferSize = {1}")
	public static Collection<Object[]> parameters() {
		ArrayList<Object[]> list = new ArrayList<>(files.length * 2);
		for (String file : files) {
			list.add(new Object[] { file, Integer.valueOf(1024) });
			list.add(new Object[] { file, Integer.valueOf(2) });
		}
		return list;
	}

	public TestGZipReadable(String filename, int bufferSize) {
		this.filename = filename;
		this.bufferSize = bufferSize;
	}
	
	private String filename;
	private int bufferSize;
	
	@Test
	public void test() throws Exception {
		testFile(filename, bufferSize);
	}
	
	static void testFile(String filename, int bufferSize) throws Exception {
		IO.Readable source = LCCore.getApplication().getResource(filename, Task.PRIORITY_NORMAL);
		IO.Readable.Buffered bin = new SimpleBufferedReadable(source, bufferSize);
		try (GZipReadable gzip = new GZipReadable(bin, Task.PRIORITY_NORMAL)) {
			byte[] buf = new byte[4096];
			while (gzip.readFullySync(ByteBuffer.wrap(buf)) == 4096);
		}
	}
	
}
