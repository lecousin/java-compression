package net.lecousin.compression.lzma;

import java.util.Arrays;

import net.lecousin.framework.memory.ByteArrayCache;
import net.lecousin.framework.memory.IntArrayCache;

import org.tukaani.xz.ArrayCache;

public class LCArrayCache extends ArrayCache {

	private ByteArrayCache byteCache = ByteArrayCache.getInstance();
	private IntArrayCache intCache = IntArrayCache.getInstance();
	
	@Override
	public byte[] getByteArray(int size, boolean fillWithZeros) {
		byte[] b = byteCache.get(size, false);
		if (fillWithZeros)
			Arrays.fill(b, (byte)0);
		return b;
	}
	
	@Override
	public void putArray(byte[] array) {
		byteCache.free(array);
	}
	
	@Override
	public int[] getIntArray(int size, boolean fillWithZeros) {
		int[] b = intCache.get(size, false);
		if (fillWithZeros)
			Arrays.fill(b, 0);
		return b;
	}
	
	@Override
	public void putArray(int[] array) {
		intCache.free(array);
	}
	
}
