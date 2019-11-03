package net.lecousin.compression.deflate;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.zip.Deflater;
import java.util.zip.InflaterInputStream;

import net.lecousin.framework.collections.ArrayUtil;
import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.core.test.LCCoreAbstractTest;
import net.lecousin.framework.core.test.io.TestIO;
import net.lecousin.framework.core.test.runners.LCConcurrentRunner;
import net.lecousin.framework.io.FileIO;
import net.lecousin.framework.mutable.Mutable;
import net.lecousin.framework.mutable.MutableInteger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(LCConcurrentRunner.Parameterized.class) @org.junit.runners.Parameterized.UseParametersRunnerFactory(LCConcurrentRunner.ConcurrentParameterizedRunnedFactory.class)
public class TestDeflateWritable extends LCCoreAbstractTest {

	@Parameters(name = "nbBuf = {1}")
	public static Collection<Object[]> parameters() {
		return TestIO.UsingTestData.generateTestCases(false);
	}
	
	public TestDeflateWritable(byte[] testBuf, int nbBuf) {
		this.testBuf = testBuf;
		this.nbBuf = nbBuf;
	}
	
	private byte[] testBuf;
	private int nbBuf;
	
	@SuppressWarnings("resource")
	@Test
	public void testCompressSyncUncompress() throws Exception {
		File tmp = File.createTempFile("test", nbBuf + "_deflate_writable");
		tmp.deleteOnExit();
		FileIO.WriteOnly fout = new FileIO.WriteOnly(tmp, Task.PRIORITY_NORMAL);
		DeflateWritable gout = new DeflateWritable(fout, Task.PRIORITY_NORMAL, Deflater.BEST_COMPRESSION, false, 3);
		for (int i = 0; i < nbBuf; ++i)
			gout.writeSync(ByteBuffer.wrap(testBuf));
		gout.finishSynch();
		gout.close();
		checkFile(tmp);
	}
	
	@Test
	@SuppressWarnings("resource")
	public void testCompressAsyncUncompress() throws Exception {
		File tmp = File.createTempFile("test", nbBuf + "_deflate_writable");
		tmp.deleteOnExit();
		FileIO.WriteOnly fout = new FileIO.WriteOnly(tmp, Task.PRIORITY_NORMAL);
		DeflateWritable gout = new DeflateWritable(fout, Task.PRIORITY_NORMAL, Deflater.BEST_COMPRESSION, false, 3);
		MutableInteger nb = new MutableInteger(0);
		Async<Exception> done = new Async<>();
		Mutable<AsyncSupplier<Integer, IOException>> write = new Mutable<>(null);
		if (nbBuf > 0)
			write.set(gout.writeAsync(ByteBuffer.wrap(testBuf)));
		else
			write.set(new AsyncSupplier<>(Integer.valueOf(0), null));
		write.get().onDone(new Runnable() {
			@Override
			public void run() {
				if (write.get().hasError()) {
					done.error(write.get().getError());
					return;
				}
				if (write.get().isCancelled()) {
					done.cancel(write.get().getCancelEvent());
					return;
				}
				if (nb.inc() < nbBuf) {
					write.set(gout.writeAsync(ByteBuffer.wrap(testBuf)));
					write.get().onDone(this);
					return;
				}
				IAsync<IOException> finish = gout.finishAsync();
				finish.onDone(new Runnable() {
					@Override
					public void run() {
						if (finish.hasError()) {
							done.error(finish.getError());
							return;
						}
						if (finish.isCancelled()) {
							done.cancel(finish.getCancelEvent());
							return;
						}
						try {
							fout.close();
							checkFile(tmp);
							done.unblock();
						} catch (Exception e) {
							done.error(e);
						}
					}
				});
			}
		});
		done.blockThrow(0);
		gout.close();
	}
	
	@SuppressWarnings("resource")
	private void checkFile(File f) throws IOException {
		FileInputStream fin = new FileInputStream(f);
		InflaterInputStream gin = new InflaterInputStream(fin);
		byte[] b = new byte[testBuf.length];
		for (int i = 0; i < nbBuf; ++i) {
			int done = 0;
			while (done < b.length) {
				int nb;
				try { nb = gin.read(b, done, b.length - done); }
				catch (Exception e) {
					throw new IOException("Error reading buffer " + i + " at " + done, e);
				}
				if (nb <= 0) throw new IOException("Unexpected end of file for buffer " + i + " at " + done);
				done += nb;
			}
			if (!ArrayUtil.equals(b, testBuf))
				throw new IOException("Invalid read for buffer " + i);
		}
		int nb = gin.read(b, 0, b.length);
		if (nb > 0)
			throw new IOException("Data can be read after the end: " + nb);
		gin.close();
	}
	
}
