package net.lecousin.compression.deflate;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.zip.InflaterInputStream;

import net.lecousin.framework.collections.ArrayUtil;
import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.core.test.LCCoreAbstractTest;
import net.lecousin.framework.core.test.io.TestIO;
import net.lecousin.framework.core.test.runners.LCConcurrentRunner;
import net.lecousin.framework.io.FileIO;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(LCConcurrentRunner.Parameterized.class) @org.junit.runners.Parameterized.UseParametersRunnerFactory(LCConcurrentRunner.ConcurrentParameterizedRunnedFactory.class)
public class TestDeflateCompressor extends LCCoreAbstractTest {

	@Parameters(name = "nbBuf = {2}")
	public static Collection<Object[]> parameters() {
		return TestIO.UsingGeneratedTestFiles.generateTestCases(false);
	}
	
	public TestDeflateCompressor(File testFile, byte[] testBuf, int nbBuf) {
		this.testFile = testFile;
		this.testBuf = testBuf;
		this.nbBuf = nbBuf;
	}
	
	private File testFile;
	private byte[] testBuf;
	private int nbBuf;
	
	@Test
	public void test() throws Exception {
		File tmp = File.createTempFile("test", nbBuf + "_deflate_compressor");
		tmp.deleteOnExit();
		FileIO.WriteOnly fout = new FileIO.WriteOnly(tmp, Task.PRIORITY_NORMAL);
		DeflateCompressor compressor = new DeflateCompressor(false);
		FileIO.ReadOnly fin = new FileIO.ReadOnly(testFile, Task.PRIORITY_NORMAL);
		compressor.compress(fin, fout, 4096, 10, Task.PRIORITY_NORMAL).blockThrow(0);
		fin.close();
		fout.close();
		checkFile(tmp);
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
