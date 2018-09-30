package net.lecousin.compression.lzma;

class LZMA2Encoder extends LZMA2Coder implements FilterEncoder {
    private final byte[] props = new byte[1];

    LZMA2Encoder(LZMA2Options options) {
        if (options.getPresetDict() != null)
            throw new IllegalArgumentException(
                    "XZ doesn't support a preset dictionary for now");

        /*if (options.getMode() == LZMA2Options.MODE_UNCOMPRESSED) {
            props[0] = (byte)0;
        } else {*/
            int d = Math.max(options.getDictSize(),
                             LZMA2Options.DICT_SIZE_MIN);
            props[0] = (byte)(LZMAEncoder.getDistSlot(d - 1) - 23);
        /*}*/
    }

    @Override
	public long getFilterID() {
        return FILTER_ID;
    }

    @Override
	public byte[] getFilterProps() {
        return props;
    }

    @Override
	public boolean supportsFlushing() {
        return true;
    }
}
