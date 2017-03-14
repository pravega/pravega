/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.common.util.BitConverter;

import java.nio.ByteBuffer;

/**
 * Utilities class to help with conversions from numerical types to byte[] and vice versa.
 */
public class Utilities {

    public static double toDouble(final byte[] b, int offset) {
        return Double.longBitsToDouble(BitConverter.readLong(b, offset));
    }

    public static byte[] toByteArray(final double value) {
        final byte[] bytes = new byte[Double.BYTES];

        ByteBuffer.wrap(bytes).putLong(Double.doubleToRawLongBits(value));
        return bytes;
    }

    public static byte[] toByteArray(final int value) {
        final byte[] bytes = new byte[Integer.BYTES];
        ByteBuffer.wrap(bytes).putInt(value);
        return bytes;
    }

    public static byte[] toByteArray(final long value) {
        final byte[] bytes = new byte[Long.BYTES];
        ByteBuffer.wrap(bytes).putLong(value);
        return bytes;
    }
}
