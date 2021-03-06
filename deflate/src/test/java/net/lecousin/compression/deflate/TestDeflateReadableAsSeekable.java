package net.lecousin.compression.deflate;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.zip.DeflaterOutputStream;

import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.core.test.io.TestIO;
import net.lecousin.framework.core.test.io.TestReadableSeekable;
import net.lecousin.framework.core.test.runners.LCConcurrentRunner;
import net.lecousin.framework.io.FileIO;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.buffering.ReadableToSeekable;
import net.lecousin.framework.io.buffering.SimpleBufferedReadable;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(LCConcurrentRunner.Parameterized.class) @org.junit.runners.Parameterized.UseParametersRunnerFactory(LCConcurrentRunner.ConcurrentParameterizedRunnedFactory.class)
public class TestDeflateReadableAsSeekable extends TestReadableSeekable {

	@Parameters(name = "nbBuf = {2}, efficient = {3}")
	public static Collection<Object[]> parameters() {
		return addTestParameter(TestIO.UsingGeneratedTestFiles.generateTestCases(true), Boolean.TRUE, Boolean.FALSE);
	}

	
	public TestDeflateReadableAsSeekable(File testFile, byte[] testBuf, int nbBuf, boolean efficient) {
		super(testFile, testBuf, nbBuf);
		this.efficient = efficient;
	}
	
	private boolean efficient;

	@Override
	protected IO.Readable.Seekable createReadableSeekableFromFile(FileIO.ReadOnly file, long fileSize) throws Exception {
		File tmp = File.createTempFile("test", "_" + fileSize + "_deflate");
		tmp.deleteOnExit();
		FileOutputStream fout = new FileOutputStream(tmp);
		DeflaterOutputStream gout = new DeflaterOutputStream(fout);
		byte[] buffer = new byte[65536];
		while (true) {
			int nb = file.readFullySync(ByteBuffer.wrap(buffer));
			if (nb <= 0) break;
			gout.write(buffer, 0, nb);
			if (nb < buffer.length) break;
		}
		gout.flush();
		gout.close();
		fout.flush();
		fout.close();
		file.closeAsync();
		FileIO.ReadOnly fin = new FileIO.ReadOnly(tmp, Task.Priority.NORMAL);
		SimpleBufferedReadable bin = new SimpleBufferedReadable(fin, efficient ? 8192 : fileSize < 128000 ? 2 : 16);
		DeflateReadable.SizeKnown gin = new DeflateReadable.SizeKnown(bin, Task.Priority.NORMAL, fileSize, false, efficient ? 8192 : fileSize < 128000 ? 2 : 16);
		return new ReadableToSeekable(gin, efficient ? 4096 : fileSize < 128000 ? 8 : 64);
	}
	
}
