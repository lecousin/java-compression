package net.lecousin.compression.gzip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import net.lecousin.framework.application.LCCore;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.concurrent.util.AsyncConsumer;
import net.lecousin.framework.core.test.LCCoreAbstractTest;
import net.lecousin.framework.core.test.io.TestIOError;
import net.lecousin.framework.core.test.runners.LCConcurrentRunner;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.data.ByteArray;
import net.lecousin.framework.mutable.Mutable;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(LCConcurrentRunner.Parameterized.class) @org.junit.runners.Parameterized.UseParametersRunnerFactory(LCConcurrentRunner.ConcurrentParameterizedRunnedFactory.class)
public class TestGZipConsumerErrors extends LCCoreAbstractTest {

	@Parameters(name = "file = {0}, bufferSize = {1}")
	public static Collection<Object[]> parameters() {
		return TestGZipReadableErrors.parameters();
	}

	public TestGZipConsumerErrors(String filename, int bufferSize) {
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
		testError(LCCore.getApplication().getResource(filename, Task.Priority.NORMAL), bufferSize);
	}
	
	static void testError(IO.Readable io, int bufferSize) throws Exception {
		Mutable<IOException> err = new Mutable<>(null);
		try {
			AsyncConsumer<ByteBuffer, IOException> consumer = new AsyncConsumer<ByteBuffer, IOException>() {
				@Override
				public IAsync<IOException> consume(ByteBuffer data) {
					return new Async<>(true);
				}
				@Override
				public IAsync<IOException> end() {
					return new Async<>(true);
				}
				@Override
				public void error(IOException error) {
					err.set(error);
				}
			};
			io.createProducer(bufferSize, true, true).toConsumer(new GZipConsumer(bufferSize, consumer), "test", Task.Priority.NORMAL).blockThrow(0);
			throw new AssertionError("error expected");
		} catch (IOException e) {
			// ok
		} finally {
			io.close();
		}
		Assert.assertNotNull(err.get());
	}
	
	@Test
	public void testInvalidIOReadableAlwaysError() throws Exception {
		Assume.assumeTrue(parameters().iterator().next()[0].equals(filename));
		TestIOError.ReadableAlwaysError io = new TestIOError.ReadableAlwaysError();
		testError(io, bufferSize);
	}
	
	@Test
	public void testInvalidIOReadableErrorAfterBeginning() throws Exception {
		Assume.assumeTrue(parameters().iterator().next()[0].equals(filename));
		TestIOError.ReadableErrorAfterBeginning io = new TestIOError.ReadableErrorAfterBeginning(new ByteArray(new byte[] { 0x1F }));
		testError(io, bufferSize);
	}

}
