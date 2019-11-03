package net.lecousin.compression.gzip;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.core.test.io.TestIO;
import net.lecousin.framework.core.test.io.TestReadable;
import net.lecousin.framework.core.test.runners.LCConcurrentRunner;
import net.lecousin.framework.io.FileIO;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.buffering.SimpleBufferedReadable;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(LCConcurrentRunner.Parameterized.class) @org.junit.runners.Parameterized.UseParametersRunnerFactory(LCConcurrentRunner.ConcurrentParameterizedRunnedFactory.class)
public class TestGZipReadableSeveralMembers extends TestReadable {

	@Parameters(name = "nbBuf = {2}")
	public static Collection<Object[]> parameters() {
		return TestIO.UsingGeneratedTestFiles.generateTestCases(false);
	}
	
	public TestGZipReadableSeveralMembers(File testFile, byte[] testBuf, int nbBuf) {
		super(testFile, testBuf, nbBuf);
	}
	
	@SuppressWarnings("resource")
	@Override
	protected IO.Readable createReadableFromFile(FileIO.ReadOnly file, long fileSize) throws Exception {
		File tmp = File.createTempFile("test", "_" + fileSize + "_gzip");
		tmp.deleteOnExit();
		FileOutputStream fout = new FileOutputStream(tmp);
		List<GZIPOutputStream> gouts = new LinkedList<>();
		do {
			GZIPOutputStream gout = new GZIPOutputStream(fout);
			gouts.add(gout);
			byte[] buffer = new byte[4096];
			int nb = file.readFullySync(ByteBuffer.wrap(buffer));
			if (nb <= 0) break;
			gout.write(buffer, 0, nb);
			gout.flush();
			gout.finish();
			if (nb < buffer.length) break;
		} while (true);
		fout.flush();
		fout.close();
		file.closeAsync();
		for (GZIPOutputStream gout : gouts) try { gout.close(); } catch (Throwable t) {}
		FileIO.ReadOnly fin = new FileIO.ReadOnly(tmp, Task.PRIORITY_NORMAL);
		IO.Readable.Buffered bin;
		if (fileSize > 0 && fileSize < 10000)
			bin = new SimpleBufferedReadable(fin, 3); // check we can correctly read the header even with slow IO
		else
			bin = new SimpleBufferedReadable(fin, 8192);
		GZipReadable.SizeKnown gin = new GZipReadable.SizeKnown(bin, Task.PRIORITY_NORMAL, fileSize);
		return gin;
	}
}
