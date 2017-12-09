package net.lecousin.compression.deflate;

import java.util.List;
import java.util.zip.Inflater;

import net.lecousin.framework.collections.TurnArray;
import net.lecousin.framework.concurrent.Task;
import net.lecousin.framework.concurrent.synch.AsyncWork;
import net.lecousin.framework.exception.NoException;
import net.lecousin.framework.memory.IMemoryManageable;
import net.lecousin.framework.memory.MemoryManager;

/**
 * Cache of inflaters, to reuse them.
 */
public class InflaterCache implements IMemoryManageable {

	private InflaterCache() { /* singleton */ }
	
	static {
		// instantiate singleton and register it
		MemoryManager.register(new InflaterCache());
	}
	
	private static TurnArray<Inflater> w = new TurnArray<Inflater>(5);
	private static TurnArray<Inflater> nw = new TurnArray<Inflater>(5);
	
	/** Get an inflater. */
	public static AsyncWork<Inflater,NoException> get(boolean nowrap) {
		if (nowrap) {
			synchronized (nw) {
				if (!nw.isEmpty())
					return new AsyncWork<>(nw.removeFirst(),null);
			}
			return new CreateInflater(true).getOutput();
		}
		synchronized (w) {
			if (!w.isEmpty())
				return new AsyncWork<>(w.removeFirst(),null);
		}
		return new CreateInflater(false).getOutput();
	}
	
	private static class CreateInflater extends Task.Cpu<Inflater, NoException> {
		public CreateInflater(boolean nowrap) {
			super("Creation of Inflater", Task.PRIORITY_IMPORTANT);
			this.nowrap = nowrap;
			start();
		}
		
		private boolean nowrap;
		
		@Override
		public Inflater run() {
			return new Inflater(nowrap);
		}
	}
	
	/** Release a deflater. The end method must not be called, else the inflater cannot be reused. */
	public static void free(Inflater inf, boolean nowrap) {
		inf.reset();
		if (nowrap) {
			synchronized (nw) {
				nw.addLast(inf);
			}
		} else {
			synchronized (w) {
				w.addLast(inf);
			}
		}
	}
	
	@Override
	public String getDescription() {
		return "Inflater cache (" + (w.size() + nw.size()) + " inflaters)";
	}
	
	@Override
	public List<String> getItemsDescription() {
		return null;
	}
	
	@Override
	public void freeMemory(FreeMemoryLevel level) {
		synchronized (w) {
			w.pollFirst();
		}
		synchronized (nw) {
			nw.pollFirst();
		}
	}
	
}
