package net.lecousin.compression.lzma;

import java.io.IOException;

import net.lecousin.framework.concurrent.async.Async;
import net.lecousin.framework.concurrent.async.IAsync;
import net.lecousin.framework.concurrent.threads.Task;
import net.lecousin.framework.concurrent.threads.TaskManager;
import net.lecousin.framework.concurrent.threads.Threading;
import net.lecousin.framework.concurrent.threads.Task.Priority;
import net.lecousin.framework.io.IO;
import net.lecousin.framework.memory.ByteArrayCache;
import net.lecousin.framework.memory.IntArrayCache;
import net.lecousin.framework.util.ConcurrentCloseable;

public abstract class AbstractLZMAWritable extends ConcurrentCloseable<IOException> implements IO.Writable {
	protected IO.Writable.Buffered output;

	protected final ByteArrayCache byteArrayCache;
	protected final IntArrayCache intArrayCache;

	protected LZEncoder lz;
	protected LZMAEncoder lzma;
	
	protected int props;
	
	protected boolean finished = false;
	
	protected AbstractLZMAWritable(IO.Writable.Buffered output, ByteArrayCache byteArrayCache, IntArrayCache intArrayCache) {
		this.output = output;
		this.byteArrayCache = byteArrayCache;
		this.intArrayCache = intArrayCache;
	}
	
	public abstract void finishSync() throws IOException;
	
	public abstract IAsync<IOException> finishAsync();
	
    @Override
    protected void closeResources(Async<IOException> ondone) {
    	output = null;
    	ondone.unblock();
    }

	@Override
    protected IAsync<IOException> closeUnderlyingResources() {
		if (output != null) {
    		if (!finished) {
    			Async<IOException> sp = new Async<>();
    			finishAsync().onDone(() -> output.closeAsync().onDone(sp), sp);
    			return sp;
    		}
    		return output.closeAsync();
    	}
    	return new Async<>(true);
	}

	@Override
	public IO getWrappedIO() {
		return output;
	}

	@Override
	public String getSourceDescription() {
		return getClass().getSimpleName() + (output != null ? "[" + output.getSourceDescription() + "]" : "");
	}

	@Override
	public Priority getPriority() {
		return output != null ? output.getPriority() : Task.Priority.NORMAL;
	}

	@Override
	public void setPriority(Priority priority) {
		if (output != null) output.setPriority(priority);
	}

	@Override
	public IAsync<IOException> canStartWriting() {
		return output.canStartWriting();
	}

	@Override
	public TaskManager getTaskManager() {
		return Threading.getCPUTaskManager();
	}

}
