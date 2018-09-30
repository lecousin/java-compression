package net.lecousin.compression.lzma;

interface FilterEncoder extends FilterCoder {
    long getFilterID();
    byte[] getFilterProps();
    boolean supportsFlushing();
}
