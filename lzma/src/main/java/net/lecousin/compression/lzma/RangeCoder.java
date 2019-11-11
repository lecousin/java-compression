package net.lecousin.compression.lzma;

import java.util.Arrays;

abstract class RangeCoder {
	
	protected RangeCoder() {
		// nothing to do
	}
	
    static final int SHIFT_BITS = 8;
    static final int TOP_MASK = 0xFF000000;
    static final int BIT_MODEL_TOTAL_BITS = 11;
    static final int BIT_MODEL_TOTAL = 1 << BIT_MODEL_TOTAL_BITS;
    static final short PROB_INIT = (short)(BIT_MODEL_TOTAL / 2);
    static final int MOVE_BITS = 5;

    public static final void initProbs(short[] probs) {
        Arrays.fill(probs, PROB_INIT);
    }
}
