package net.lecousin.compression.lzma;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.core.test.io.TestIO;
import net.lecousin.framework.core.test.io.TestReadable;
import net.lecousin.framework.io.FileIO;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.buffering.SimpleBufferedReadable;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.tukaani.xz.FinishableOutputStream;
import org.tukaani.xz.FinishableWrapperOutputStream;

@RunWith(Parameterized.class)
public class TestLZMA2Readable extends TestReadable {

	@Parameters(name = "nbBuf = {2}, preset = {3}")
	public static Collection<Object[]> parameters() {
		Collection<Object[]> params = TestIO.UsingGeneratedTestFiles.generateTestCases(false);
		ArrayList<Object[]> list = new ArrayList<>(params.size() * (LZMA2Options.PRESET_MAX - LZMA2Options.PRESET_MIN + 1));
		for (Object[] p : params) {
			// use preset -1 to say no compression
			Object[] o = new Object[p.length + 1];
			System.arraycopy(p, 0, o, 0, p.length);
			o[p.length] = Integer.valueOf(-1);
			list.add(o);
			
			// do not use MAX because we get OutOfMemoryError
			for (int preset = LZMA2Options.PRESET_MIN; preset <= LZMA2Options.PRESET_MAX - 2; ++preset) {
				o = new Object[p.length + 1];
				System.arraycopy(p, 0, o, 0, p.length);
				o[p.length] = Integer.valueOf(preset);
				list.add(o);
			}
		}
		return list;
	}
	
	public TestLZMA2Readable(File testFile, byte[] testBuf, int nbBuf, int preset) {
		super(testFile, testBuf, nbBuf);
		this.preset = preset;
	}
	
	protected int preset;
	
	@Override
	protected IO.Readable createReadableFromFile(FileIO.ReadOnly file, long fileSize) throws Exception {
		File tmp = File.createTempFile("test", "_" + fileSize + "_lzma2");
		tmp.deleteOnExit();
		FileOutputStream fout = new FileOutputStream(tmp);
		org.tukaani.xz.LZMA2Options options;
		if (preset == -1) {
			options = new org.tukaani.xz.LZMA2Options(0);
			options.setMode(org.tukaani.xz.LZMA2Options.MODE_UNCOMPRESSED);
		} else {
			options = new org.tukaani.xz.LZMA2Options(preset);
		}
		FinishableOutputStream out = options.getOutputStream(new FinishableWrapperOutputStream(fout), new LCArrayCache());
		byte[] buffer = new byte[65536];
		while (true) {
			int nb = file.readFullySync(ByteBuffer.wrap(buffer));
			if (nb <= 0) break;
			out.write(buffer, 0, nb);
			if (nb < buffer.length) break;
		}
		out.finish();
		out.close();
		fout.flush();
		fout.close();
		file.closeAsync();
		FileIO.ReadOnly fin = new FileIO.ReadOnly(tmp, Task.Priority.NORMAL);
		SimpleBufferedReadable bin = new SimpleBufferedReadable(fin, 8192);
		LZMA2Readable in = new LZMA2Readable(bin, options.getDictSize());
		return in;
	}

}
