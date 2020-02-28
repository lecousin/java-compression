package net.lecousin.compression.lzma;

import java.io.EOFException;
import java.io.IOException;
import java.util.function.Supplier;

import net.lecousin.framework.concurrent.async.AsyncSupplier;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.exception.NoException;
import net.lecousin.framework.concurrent.Executable;
import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.io.IO;

final class TaskUtil {
	
	private TaskUtil() {
		// no instance
	}
	
	public static Task<Void, NoException> decompressionTask(IO.Readable input, Runnable r, IAsync<IOException> onerror) {
		return Task.cpu("LZMA Decompression", input.getPriority(), () -> {
			try { r.run(); }
			catch (Exception e) {
				onerror.error(IO.error(e));
			}
			return null;
		});
	}

	public static Async<IOException> continueDecompression(IO.Readable.Buffered input, IAsync<IOException> waiting, Supplier<IAsync<IOException>> continueProvider) {
		Async<IOException> sp = new Async<>();
    	waiting.thenStart(decompressionTask(input, () -> continueProvider.get().onDone(sp), sp), sp);
    	return sp;
	}
	
	public static IAsync<IOException> checkRead(AsyncSupplier<Integer, IOException> read, int len) {
        if (read.isDone()) {
        	if (read.isSuccessful() && read.getResult().intValue() != len)
        		return new Async<>(new EOFException());
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
	
	public static Task<Void, NoException> compressionTask(IO.Writable output, Runnable r) {
		return Task.cpu("LZMA Compression", output.getPriority(), new Executable.FromRunnable(r));
	}

	public static Async<IOException> continueCompression(IO.Writable.Buffered output, IAsync<IOException> waiting, Supplier<IAsync<IOException>> continueProvider) {
		Async<IOException> sp = new Async<>();
		waiting.thenStart(compressionTask(output, () -> continueProvider.get().onDone(sp)), sp);
    	return sp;
	}
	

}
