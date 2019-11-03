package net.lecousin.compression.lzma;

import java.io.EOFException;
import java.io.IOException;
import java.util.function.Supplier;

import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.io.IO;

public class TaskUtil {
	
	public static Task.Cpu.FromRunnable decompressionTask(IO.Readable input, Runnable r) {
		return new Task.Cpu.FromRunnable("LZMA Decompression", input.getPriority(), r);
	}

	public static Async<IOException> continueDecompression(IO.Readable.Buffered input, IAsync<IOException> waiting, Supplier<IAsync<IOException>> continueProvider) {
		Async<IOException> sp = new Async<>();
    	waiting.thenStart(decompressionTask(input, () -> {
    		continueProvider.get().onDone(sp);
    	}), sp);
    	return sp;
	}
	
	public static IAsync<IOException> checkRead(AsyncSupplier<Integer, IOException> read, int len) {
        if (read.isDone()) {
        	if (read.isSuccessful()) {
        		if (read.getResult().intValue() != len)
        			return new Async<>(new EOFException());
        	}
        	return read;
        }
        Async<IOException> sp = new Async<>();
        read.onDone(() -> {
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

	public static Async<IOException> continueCompression(IO.Writable.Buffered output, IAsync<IOException> waiting, Supplier<IAsync<IOException>> continueProvider) {
		Async<IOException> sp = new Async<>();
		waiting.thenStart(compressionTask(output, () -> {
    		continueProvider.get().onDone(sp);
    	}), sp);
    	return sp;
	}
	

}
