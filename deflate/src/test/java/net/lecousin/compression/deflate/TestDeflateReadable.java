package net.lecousin.compression.deflate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.zip.DeflaterOutputStream;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.core.test.io.TestIO;
import net.lecousin.framework.core.test.io.TestIOError;
import net.lecousin.framework.core.test.io.TestReadable;
import net.lecousin.framework.core.test.runners.LCConcurrentRunner;
import net.lecousin.framework.io.FileIO;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.io.buffering.SimpleBufferedReadable;
import net.lecousin.framework.mutable.MutableBoolean;
import net.lecousin.framework.util.Pair;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;


@RunWith(LCConcurrentRunner.Parameterized.class) @org.junit.runners.Parameterized.UseParametersRunnerFactory(LCConcurrentRunner.ConcurrentParameterizedRunnedFactory.class)
public class TestDeflateReadable extends TestReadable {

	@Parameters(name = "nbBuf = {2}, efficient = {3}")
	public static Collection<Object[]> parameters() {
		return addTestParameter(TestIO.UsingGeneratedTestFiles.generateTestCases(false), Boolean.TRUE, Boolean.FALSE);
	}
	
	public TestDeflateReadable(File testFile, byte[] testBuf, int nbBuf, boolean efficient) {
		super(testFile, testBuf, nbBuf);
		this.efficient = efficient;
	}
	
	private boolean efficient;
	
	@Override
	protected IO.Readable createReadableFromFile(FileIO.ReadOnly file, long fileSize) throws Exception {
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
		FileIO.ReadOnly fin = new FileIO.ReadOnly(tmp, Task.PRIORITY_NORMAL);
		SimpleBufferedReadable bin = new SimpleBufferedReadable(fin, efficient ? 8192 : 2);
		DeflateReadable.SizeKnown gin = new DeflateReadable.SizeKnown(bin, Task.PRIORITY_NORMAL, fileSize, false);
		return gin;
	}
	
	private static <T> Consumer<Pair<T, IOException>> createOnDone(MutableBoolean checkOnDone) {
		return p -> checkOnDone.set(p.getValue2() != null);
	}
	
	@Test
	public void testErrors() {
		DeflateReadable.SizeKnown io = new DeflateReadable.SizeKnown(new TestIOError.IOError1(), Task.PRIORITY_NORMAL, 65536, false);
		MutableBoolean checkOnDone = new MutableBoolean(false);
		try {
			io.readAsync(ByteBuffer.allocate(128), createOnDone(checkOnDone)).blockResult(0);
			throw new AssertionError("Error expected");
		} catch (Exception e) {
			// ok
		}
		Assert.assertTrue(checkOnDone.get());
		try {
			io.readFullyAsync(ByteBuffer.allocate(128), createOnDone(checkOnDone)).blockResult(0);
			throw new AssertionError("Error expected");
		} catch (Exception e) {
			// ok
		}
		Assert.assertTrue(checkOnDone.get());
		try {
			io.readSync(ByteBuffer.allocate(128));
		} catch (IOException e) {
			// ok
		}
		try {
			io.skipAsync(15, createOnDone(checkOnDone)).blockResult(0);
			throw new AssertionError("Error expected");
		} catch (Exception e) {
			// ok
		}
		Assert.assertTrue(checkOnDone.get());
		try {
			io.skipSync(15);
		} catch (IOException e) {
			// ok
		}
		io.closeAsync();
	}

}
