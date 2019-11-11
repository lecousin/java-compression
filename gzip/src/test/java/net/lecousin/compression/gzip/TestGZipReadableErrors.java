package net.lecousin.compression.gzip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import net.lecousin.framework.core.test.LCCoreAbstractTest;
import net.lecousin.framework.core.test.runners.LCConcurrentRunner;

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
	
}
