package net.lecousin.compression.lzma;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import net.lecousin.framework.collections.ArrayUtil;
import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.core.test.LCCoreAbstractTest;
import net.lecousin.framework.core.test.io.TestIO;
import net.lecousin.framework.io.FileIO;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.buffering.SimpleBufferedWritable;
import net.lecousin.framework.mutable.Mutable;
import net.lecousin.framework.mutable.MutableInteger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestLZMA2Writable extends LCCoreAbstractTest {

	@Parameters(name = "nbBuf = {1}, preset = {2}")
	public static Collection<Object[]> parameters() {
		Collection<Object[]> params = TestIO.UsingTestData.generateTestCases(false);
		ArrayList<Object[]> list = new ArrayList<>(params.size() * (LZMA2Options.PRESET_MAX - LZMA2Options.PRESET_MIN + 1));
		for (Object[] p : params) {
			// do not use MAX because we get OutOfMemoryError
			for (int preset = LZMA2Options.PRESET_MIN; preset <= LZMA2Options.PRESET_MAX - 1; ++preset) {
				Object[] o = new Object[p.length + 1];
				System.arraycopy(p, 0, o, 0, p.length);
				o[p.length] = Integer.valueOf(preset);
				list.add(o);
			}
		}
		return list;
	}
	
	public TestLZMA2Writable(byte[] testBuf, int nbBuf, int preset) {
		this.testBuf = testBuf;
		this.nbBuf = nbBuf;
		this.preset = preset;
	}
	
	private byte[] testBuf;
	private int nbBuf;
	private int preset;
	
	@Test
	public void testCompressSyncUncompress() throws Exception {
		File tmp = File.createTempFile("test", nbBuf + "_lzma2_writable");
		tmp.deleteOnExit();
		FileIO.WriteOnly fout = new FileIO.WriteOnly(tmp, Task.PRIORITY_NORMAL);
		IO.Writable.Buffered bout = new SimpleBufferedWritable(fout, 4096);
		LZMA2Options options = new LZMA2Options(preset);
		LZMA2Writable out = new LZMA2Writable(bout, options);
		for (int i = 0; i < nbBuf; ++i)
			out.writeSync(ByteBuffer.wrap(testBuf));
		out.finishSync();
		bout.flush();
		out.close();
		checkFile(tmp);
	}
	
	@Test
	public void testCompressAsyncUncompress() throws Exception {
		File tmp = File.createTempFile("test", nbBuf + "_deflate_writable");
		tmp.deleteOnExit();
		FileIO.WriteOnly fout = new FileIO.WriteOnly(tmp, Task.PRIORITY_NORMAL);
		IO.Writable.Buffered bout = new SimpleBufferedWritable(fout, 4096);
		LZMA2Options options = new LZMA2Options(preset);
		LZMA2Writable out = new LZMA2Writable(bout, options);
		MutableInteger nb = new MutableInteger(0);
		Async<Exception> done = new Async<>();
		Mutable<AsyncSupplier<Integer, IOException>> write = new Mutable<>(null);
		if (nbBuf > 0)
			write.set(out.writeAsync(ByteBuffer.wrap(testBuf)));
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
					write.set(out.writeAsync(ByteBuffer.wrap(testBuf)));
					write.get().onDone(this);
					return;
				}
				IAsync<IOException> finish = out.finishAsync();
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
		out.close();
	}
	
	private void checkFile(File f) throws IOException {
		FileInputStream fin = new FileInputStream(f);
		org.tukaani.xz.LZMA2Options options = new org.tukaani.xz.LZMA2Options(preset);
		InputStream in = options.getInputStream(fin);
		byte[] b = new byte[testBuf.length];
		for (int i = 0; i < nbBuf; ++i) {
			int done = 0;
			while (done < b.length) {
				int nb;
				try { nb = in.read(b, done, b.length - done); }
				catch (Exception e) {
					throw new IOException("Error reading buffer " + i + " at " + done, e);
				}
				if (nb <= 0) throw new IOException("Unexpected end of file for buffer " + i + " at " + done);
				done += nb;
			}
			if (!ArrayUtil.equals(b, testBuf))
				throw new IOException("Invalid read for buffer " + i);
		}
		int nb = in.read(b, 0, b.length);
		if (nb > 0)
			throw new IOException("Data can be read after the end: " + nb);
		in.close();
	}
	
}
