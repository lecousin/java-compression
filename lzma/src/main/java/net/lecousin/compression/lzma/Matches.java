package net.lecousin.compression.lzma;

public final class Matches {
    public final int[] len;
    public final int[] dist;
    public int count = 0;

    Matches(int countMax) {
        len = new int[countMax];
        dist = new int[countMax];
    }
}
