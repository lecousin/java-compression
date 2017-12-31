package net.lecousin.compression.gzip;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.zip.GZIPOutputStream;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.core.test.io.TestIO;
import net.lecousin.framework.io.FileIO;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.buffering.ReadableToSeekable;
import net.lecousin.framework.io.buffering.SimpleBufferedReadable;

import org.junit.runners.Parameterized.Parameters;

// TODO @RunWith(Parameterized.class)
public class TestGZipReadableOneMemberAsSeekable { // TODO extends TestReadableSeekable {

	@Parameters(name = "nbBuf = {2}")
	public static Collection<Object[]> parameters() {
		return TestIO.UsingGeneratedTestFiles.generateTestCases(false);
	}
	
	public TestGZipReadableOneMemberAsSeekable(File testFile, byte[] testBuf, int nbBuf) {
		// TODO super(testFile, testBuf, nbBuf);
	}
	
	@SuppressWarnings("resource")
	// TODO @Override
	protected IO.Readable.Seekable createReadableSeekableFromFile(FileIO.ReadOnly file, long fileSize) throws Exception {
		File tmp = File.createTempFile("test", "_" + fileSize + "_gzip");
		tmp.deleteOnExit();
		FileOutputStream fout = new FileOutputStream(tmp);
		GZIPOutputStream gout = new GZIPOutputStream(fout);
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
		FileIO.ReadOnly fin = new FileIO.ReadOnly(tmp, Task.PRIORITY_NORMAL);
		SimpleBufferedReadable bin = new SimpleBufferedReadable(fin, 8192);
		GZipReadable.SizeKnown gin = new GZipReadable.SizeKnown(bin, Task.PRIORITY_NORMAL, fileSize);
		return new ReadableToSeekable(gin, 4096);
	}
}
