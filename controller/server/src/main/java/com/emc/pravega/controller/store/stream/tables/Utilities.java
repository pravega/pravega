/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream.tables;

import java.nio.ByteBuffer;

/**
 * Utilities class to help with conversions from numerical types to byte[] and vice versa.
 */
public class Utilities {

    public static int toInt(final byte[] b) {
        return ByteBuffer.wrap(b).getInt();
    }

    public static long toLong(final byte[] b) {
        return ByteBuffer.wrap(b).getLong();
    }

    public static double toDouble(final byte[] b) {
        return ByteBuffer.wrap(b).getDouble();
    }

    public static byte[] toByteArray(final double value) {
        final byte[] bytes = new byte[Double.SIZE / 8];
        ByteBuffer.wrap(bytes).putDouble(value);
        return bytes;
    }

    public static byte[] toByteArray(final int value) {
        final byte[] bytes = new byte[Integer.SIZE / 8];
        ByteBuffer.wrap(bytes).putInt(value);
        return bytes;
    }

    public static byte[] toByteArray(final long value) {
        final byte[] bytes = new byte[Long.SIZE / 8];
        ByteBuffer.wrap(bytes).putLong(value);
        return bytes;
    }
}
