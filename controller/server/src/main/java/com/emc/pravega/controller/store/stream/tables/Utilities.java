/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
