package net.lecousin.compression.lzma;

abstract class LZMA2Coder implements FilterCoder {
    public static final long FILTER_ID = 0x21;

    @Override
	public boolean changesSize() {
        return true;
    }

    @Override
	public boolean nonLastOK() {
        return false;
    }

    @Override
	public boolean lastOK() {
        return true;
    }
}
