package net.lecousin.compression.lzma;

import net.lecousin.framework.core.test.LCCoreAbstractTest;

import org.junit.Test;

public class TestBasics extends LCCoreAbstractTest {

	@Test
	public void test() {
		LZMAEncoderNormal.getMemoryUsage(1024, 1024, LZEncoder.MF_BT4);
		LZMAEncoder.getMemoryUsage(LZMA2Options.Mode.FAST, 1024, 1204, LZEncoder.MF_BT4);
		LZMAEncoder.getMemoryUsage(LZMA2Options.Mode.NORMAL, 1024, 1204, LZEncoder.MF_HC4);
		try {
			LZMAEncoderNormal.getMemoryUsage(1024, 1024, 0);
		} catch (IllegalArgumentException e) {
			// ok
		}
		LZMA2Writable.getMemoryUsage(new LZMA2Options());
		LZMA2Readable.getMemoryUsage(4096);
		try {
			LZMA1Readable.getMemoryUsage(4096, (byte)0);
		} catch (Exception e) {
		}
	}
	
}
