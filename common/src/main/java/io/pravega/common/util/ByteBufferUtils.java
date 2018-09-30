/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import java.nio.ByteBuffer;

public class ByteBufferUtils {

    public static ByteBuffer slice(ByteBuffer orig, int begin, int length) {
        int pos = orig.position();
        int limit = orig.limit();
        orig.limit(begin + length);
        orig.position(begin);
        ByteBuffer result = orig.slice();
        orig.limit(limit);
        orig.position(pos);
        return result;
    }
    
    /**
     * Copies data from the provided `from` buffer to the provided `to` buffer.
     * The data is copied starting from the {@link ByteBuffer#position())} of the buffer to its {@link ByteBuffer#limit())}.
     * As much data as possible will be copied without going beyond the {@link ByteBuffer#limit())} of the `to` buffer. 
     * So if the `to` buffer is too small to hold the data in the `from` buffer, only the portion that will fit is copied.
     * 
     * The position of the `from` buffer will be advanced by the amount of data copied. 
     * The `to` buffer's position will be unchanged, but it's limit will be set to the amount of the data copied.
     * (So the `to` buffer should be ready to read the data written.) 
     * 
     * @param from the buffer to copy from
     * @param to the buffer to copy into
     * @return the number of bytes copied
     */
    public static int copy(ByteBuffer from, ByteBuffer to) {
        int toCopy = Math.min(from.remaining(), to.remaining());
        int origionalFromLimit = from.limit();
        int origionalToPosition = to.position();
        from.limit(from.position() + toCopy);
        to.put(from);
        to.limit(to.position());
        to.position(origionalToPosition);
        from.limit(origionalFromLimit);
        return toCopy;
    }
    
}
