package net.lecousin.compression.lzma;

import net.lecousin.framework.core.test.LCCoreAbstractTest;

import org.junit.Test;

public class TestBasics extends LCCoreAbstractTest {

	@Test
	public void test() {
		LZMAEncoderNormal.getMemoryUsage(1024, 1024, 1);
		LZMAEncoder.getMemoryUsage(LZMA2Options.Mode.FAST, 1024, 1204, 1);
		LZMAEncoder.getMemoryUsage(LZMA2Options.Mode.NORMAL, 1024, 1204, 1);
	}
	
}
