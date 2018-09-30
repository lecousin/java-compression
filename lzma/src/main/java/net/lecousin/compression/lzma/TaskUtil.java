package net.lecousin.compression.lzma;

import java.io.EOFException;
import java.io.IOException;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.synch.AsyncWork;
import net.lecousin.framework.concurrent.synch.ISynchronizationPoint;
import net.lecousin.framework.concurrent.synch.SynchronizationPoint;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.util.Provider;

public class TaskUtil {
	
	public static Task.Cpu.FromRunnable decompressionTask(IO.Readable input, Runnable r) {
		return new Task.Cpu.FromRunnable("LZMA Decompression", input.getPriority(), r);
	}

	public static SynchronizationPoint<IOException> continueDecompression(IO.Readable.Buffered input, ISynchronizationPoint<IOException> waiting, Provider<ISynchronizationPoint<IOException>> continueProvider) {
		SynchronizationPoint<IOException> sp = new SynchronizationPoint<>();
    	waiting.listenAsync(decompressionTask(input, () -> {
    		continueProvider.provide().listenInline(sp);
    	}), sp);
    	return sp;
	}
	
	public static ISynchronizationPoint<IOException> checkRead(AsyncWork<Integer, IOException> read, int len) {
        if (read.isUnblocked()) {
        	if (read.isSuccessful()) {
        		if (read.getResult().intValue() != len)
        			return new SynchronizationPoint<>(new EOFException());
        	}
        	return read;
        }
        SynchronizationPoint<IOException> sp = new SynchronizationPoint<>();
        read.listenInline(() -> {
        	if (read.getResult().intValue() != len)
        		sp.error(new EOFException());
        	else
        		sp.unblock();
        }, sp);
        return sp;
	}
	
	public static Task.Cpu.FromRunnable compressionTask(IO.Writable output, Runnable r) {
		return new Task.Cpu.FromRunnable("LZMA Compression", output.getPriority(), r);
	}

	public static SynchronizationPoint<IOException> continueCompression(IO.Writable.Buffered output, ISynchronizationPoint<IOException> waiting, Provider<ISynchronizationPoint<IOException>> continueProvider) {
		SynchronizationPoint<IOException> sp = new SynchronizationPoint<>();
		waiting.listenAsync(compressionTask(output, () -> {
    		continueProvider.provide().listenInline(sp);
    	}), sp);
    	return sp;
	}
	

}
