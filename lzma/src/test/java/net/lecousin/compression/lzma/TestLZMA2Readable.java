package net.lecousin.compression.lzma;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;

import net.lecousin.framework.concurrent.Task;
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

	@Parameters(name = "nbBuf = {2}")
	public static Collection<Object[]> parameters() {
		return TestIO.UsingGeneratedTestFiles.generateTestCases(false);
	}
	
	public TestLZMA2Readable(File testFile, byte[] testBuf, int nbBuf) {
		super(testFile, testBuf, nbBuf);
	}
	
	@Override
	protected IO.Readable createReadableFromFile(FileIO.ReadOnly file, long fileSize) throws Exception {
		File tmp = File.createTempFile("test", "_" + fileSize + "_lzma2");
		tmp.deleteOnExit();
		FileOutputStream fout = new FileOutputStream(tmp);
		org.tukaani.xz.LZMA2Options options = new org.tukaani.xz.LZMA2Options(org.tukaani.xz.LZMA2Options.PRESET_DEFAULT);
		FinishableOutputStream out = options.getOutputStream(new FinishableWrapperOutputStream(fout));
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
		FileIO.ReadOnly fin = new FileIO.ReadOnly(tmp, Task.PRIORITY_NORMAL);
		SimpleBufferedReadable bin = new SimpleBufferedReadable(fin, 8192);
		LZMA2Readable in = new LZMA2Readable(bin, options.getDictSize());
		return in;
	}

}
