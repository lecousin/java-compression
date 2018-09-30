package net.lecousin.compression.lzma;

class LZMA2Decoder extends LZMA2Coder implements FilterDecoder {
    private int dictSize;

    LZMA2Decoder(byte[] props) throws UnsupportedOptionsException {
        // Up to 1.5 GiB dictionary is supported. The bigger ones
        // are too big for int.
        if (props.length != 1 || (props[0] & 0xFF) > 37)
            throw new UnsupportedOptionsException(
                    "Unsupported LZMA2 properties");

        dictSize = 2 | (props[0] & 1);
        dictSize <<= (props[0] >>> 1) + 11;
    }

    @Override
	public int getMemoryUsage() {
        return LZMA2Readable.getMemoryUsage(dictSize);
    }
}
