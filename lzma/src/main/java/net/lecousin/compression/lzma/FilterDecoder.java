package net.lecousin.compression.lzma;

interface FilterDecoder extends FilterCoder {
    int getMemoryUsage();
}
