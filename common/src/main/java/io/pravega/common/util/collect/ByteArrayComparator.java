/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.collect;

import io.pravega.common.util.ByteArraySegment;
import java.util.Comparator;

/**
 * Compares two byte arrays of the same length.
 */
class ByteArrayComparator implements Comparator<byte[]> {
    @Override
    public int compare(byte[] b1, byte[] b2) {
        assert b1.length == b2.length;
        return compare(b1, 0, b2, 0, b1.length);
    }

    int compare(ByteArraySegment b1, ByteArraySegment b2) {
        assert b1.getLength() == b2.getLength();
        return compare(b1.array(), b1.arrayOffset(), b2.array(), b2.arrayOffset(), b1.getLength());
    }

    int compare(byte[] b1, int offset1, byte[] b2, int offset2, int length) {
        int r;
        for (int i = 0; i < length; i++) {
            r = Byte.compare(b1[offset1 + i], b2[offset2 + i]);
            if (r != 0) {
                return r;
            }
        }

        return 0;
    }
}