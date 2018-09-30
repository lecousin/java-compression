package net.lecousin.compression.lzma;

/**
 * Base class for filter-specific options classes.
 */
public abstract class FilterOptions implements Cloneable {
    /**
     * Gets how much memory the encoder will need with
     * the given filter chain. This function simply calls
     * <code>getEncoderMemoryUsage()</code> for every filter
     * in the array and returns the sum of the returned values.
     */
    public static int getEncoderMemoryUsage(FilterOptions[] options) {
        int m = 0;

        for (int i = 0; i < options.length; ++i)
            m += options[i].getEncoderMemoryUsage();

        return m;
    }

    /**
     * Gets how much memory the decoder will need with
     * the given filter chain. This function simply calls
     * <code>getDecoderMemoryUsage()</code> for every filter
     * in the array and returns the sum of the returned values.
     */
    public static int getDecoderMemoryUsage(FilterOptions[] options) {
        int m = 0;

        for (int i = 0; i < options.length; ++i)
            m += options[i].getDecoderMemoryUsage();

        return m;
    }

    /**
     * Gets how much memory the encoder will need with these options.
     */
    public abstract int getEncoderMemoryUsage();

    /**
     * Gets how much memory the decoder will need to decompress the data
     * that was encoded with these options.
     */
    public abstract int getDecoderMemoryUsage();

    abstract FilterEncoder getFilterEncoder();

    FilterOptions() {}
}
