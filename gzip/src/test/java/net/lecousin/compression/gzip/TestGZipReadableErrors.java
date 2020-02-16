package net.lecousin.compression.gzip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.core.test.LCCoreAbstractTest;
import net.lecousin.framework.core.test.io.TestIOError;
import net.lecousin.framework.core.test.runners.LCConcurrentRunner;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.buffering.SimpleBufferedReadable;
import net.lecousin.framework.io.data.ByteArray;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(LCConcurrentRunner.Parameterized.class) @org.junit.runners.Parameterized.UseParametersRunnerFactory(LCConcurrentRunner.ConcurrentParameterizedRunnedFactory.class)
public class TestGZipReadableErrors extends LCCoreAbstractTest {

	private static final String[] files = {
		"gzip-test/error/empty.gz",
		"gzip-test/error/1byte.gz",
		"gzip-test/error/invalid_first_byte.gz",
		"gzip-test/error/invalid_first_byte_2.gz",
		"gzip-test/error/invalid_second_byte.gz",
		"gzip-test/error/invalid_second_byte_2.gz",
		"gzip-test/error/invalid_method.gz",
		"gzip-test/error/filename_never_ending.gz",
		"gzip-test/error/comment_never_ending.gz",
		"gzip-test/error/extra_never_ending.gz",
		"gzip-test/error/crc_eof0.gz",
		"gzip-test/error/crc_eof1.gz",
		"gzip-test/error/sample.txt_error.gz",
	};
	
	@Parameters(name = "file = {0}, bufferSize = {1}")
	public static Collection<Object[]> parameters() {
		ArrayList<Object[]> list = new ArrayList<>(files.length * 2);
		for (String file : files) {
			list.add(new Object[] { file, Integer.valueOf(1024) });
			list.add(new Object[] { file, Integer.valueOf(14) });
			list.add(new Object[] { file, Integer.valueOf(11) });
			list.add(new Object[] { file, Integer.valueOf(2) });
			list.add(new Object[] { file, Integer.valueOf(1) });
		}
		return list;
	}

	public TestGZipReadableErrors(String filename, int bufferSize) {
		this.filename = filename;
		this.bufferSize = bufferSize;
	}
	
	private String filename;
	private int bufferSize;
	
	@Test(expected = IOException.class)
	public void test() throws Exception {
		TestGZipReadable.testFile(filename, bufferSize);
	}
	
	@Test
	public void testInvalidIOReadableAlwaysError() throws Exception {
		Assume.assumeTrue(parameters().iterator().next()[0].equals(filename));
		TestIOError.ReadableAlwaysError io = new TestIOError.ReadableAlwaysError();
		IO.Readable.Buffered bin = new SimpleBufferedReadable(io, bufferSize);
		try (GZipReadable gzip = new GZipReadable(bin, Task.PRIORITY_NORMAL)) {
			byte[] buf = new byte[4096];
			while (gzip.readFullySync(ByteBuffer.wrap(buf)) == 4096);
		} catch (IOException e) {
			// ok
		}
	}
	
	@Test
	public void testInvalidIOReadableErrorAfterBeginning() throws Exception {
		Assume.assumeTrue(parameters().iterator().next()[0].equals(filename));
		TestIOError.ReadableErrorAfterBeginning io = new TestIOError.ReadableErrorAfterBeginning(new ByteArray(new byte[] { 0x1F }));
		IO.Readable.Buffered bin = new SimpleBufferedReadable(io, bufferSize);
		try (GZipReadable gzip = new GZipReadable(bin, Task.PRIORITY_NORMAL)) {
			byte[] buf = new byte[4096];
			while (gzip.readFullySync(ByteBuffer.wrap(buf)) == 4096);
		} catch (IOException e) {
			// ok
		}
	}
	
}
