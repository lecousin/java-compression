package net.lecousin.compression.gzip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import net.lecousin.framework.application.LCCore;
import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.util.AsyncConsumer;
import net.lecousin.framework.core.test.LCCoreAbstractTest;
import net.lecousin.framework.core.test.runners.LCConcurrentRunner;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.IO.Seekable.SeekType;
import net.lecousin.framework.io.buffering.ByteBuffersIO;
import net.lecousin.framework.io.buffering.SimpleBufferedReadable;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(LCConcurrentRunner.Parameterized.class) @org.junit.runners.Parameterized.UseParametersRunnerFactory(LCConcurrentRunner.ConcurrentParameterizedRunnedFactory.class)
public class TestGZipConsumerWithGZipReadable extends LCCoreAbstractTest {

	@Parameters(name = "file = {0}, bufferSize = {1}")
	public static Collection<Object[]> parameters() {
		return TestGZipReadable.parameters();
	}

	public TestGZipConsumerWithGZipReadable(String filename, int bufferSize) {
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
		ByteBuffersIO fromIO = new ByteBuffersIO(true, "test", Task.PRIORITY_NORMAL);
		try (GZipReadable gzip = new GZipReadable(bin, Task.PRIORITY_NORMAL)) {
			byte[] buf = new byte[4096];
			do {
				ByteBuffer bb = ByteBuffer.wrap(buf);
				int nb = gzip.readFullySync(bb);
				if (nb > 0) {
					bb.flip();
					fromIO.writeSync(bb.asReadOnlyBuffer());
				}
				if (nb < buf.length)
					break;
			} while (true);
		}
		
		source = LCCore.getApplication().getResource(filename, Task.PRIORITY_NORMAL);
		ByteBuffersIO fromConsumer = new ByteBuffersIO(true, "test", Task.PRIORITY_NORMAL);
		AsyncConsumer<ByteBuffer, IOException> consumer = new AsyncConsumer<ByteBuffer, IOException>() {
			@Override
			public IAsync<IOException> consume(ByteBuffer data) {
				return fromConsumer.writeAsync(data, null);
			}
			@Override
			public IAsync<IOException> end() {
				return new Async<>(true);
			}
			@Override
			public void error(IOException error) {
			}
		};
		source.createProducer(bufferSize, true, true).toConsumer(new GZipConsumer(bufferSize, consumer), "test", Task.PRIORITY_NORMAL).blockThrow(0);
		
		fromIO.seekSync(SeekType.FROM_BEGINNING, 0);
		fromConsumer.seekSync(SeekType.FROM_BEGINNING, 0);
		byte[] buf1 = new byte[4096];
		byte[] buf2 = new byte[4096];
		int pos = 0;
		do {
			int nb1 = fromIO.readFullySync(ByteBuffer.wrap(buf1));
			int nb2 = fromConsumer.readFullySync(ByteBuffer.wrap(buf2));
			Assert.assertEquals("at position " + pos, nb1, nb2);
			for (int i = 0; i < nb1; ++i)
				Assert.assertEquals(buf1[i], buf2[i]);
			if (nb1 < buf1.length)
				break;
			pos += nb1;
		} while (true);
		
		fromIO.close();
		fromConsumer.close();
	}
	
}
