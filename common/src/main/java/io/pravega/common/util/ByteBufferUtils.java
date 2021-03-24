/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.util;

import java.nio.ByteBuffer;

public class ByteBufferUtils {
    
    public static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

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
     * The data is copied starting from the {@link ByteBuffer#position()} of the buffer to its {@link ByteBuffer#limit()}.
     * As much data as possible will be copied without going beyond the {@link ByteBuffer#limit()} of the `to` buffer.
     * So if the `to` buffer is too small to hold the data in the `from` buffer, only the portion that will fit is copied.
     * 
     * The position of the `from` and `to` buffers will be advanced by the amount of data copied. 
     * 
     * @param from the buffer to copy from
     * @param to the buffer to copy into
     * @return the number of bytes copied
     */
    public static int copy(ByteBuffer from, ByteBuffer to) {
        int toCopy = Math.min(from.remaining(), to.remaining());
        int originalFromLimit = from.limit();
        from.limit(from.position() + toCopy);
        to.put(from);
        from.limit(originalFromLimit);
        return toCopy;
    }
    
}
