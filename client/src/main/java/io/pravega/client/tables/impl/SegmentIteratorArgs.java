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
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.util.AsyncIterator;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Arguments to {@link TableSegment#keyIterator} and {@link TableSegment#entryIterator}.
 */
@Data
@Builder
class SegmentIteratorArgs {
    /**
     * The Table Segment Key to begin iteration at (inclusive).
     */
    @NonNull
    private final ByteBuf fromKey;
    /**
     * The Table Segment Key to end the iteration at (inclusive). NOTE that not all Keys/Entries between
     * {@link #getFromKey()} and {@link #getToKey()} may be returned - the result size may be capped at
     * {@link #getMaxItemsAtOnce()}.
     */
    @NonNull
    private final ByteBuf toKey;
    /**
     * The maximum number of items to return with each call to {@link AsyncIterator#getNext()}.
     */
    private final int maxItemsAtOnce;

    /**
     * Creates a new {@link SegmentIteratorArgs} that is identical to this instance, but has a {@link #getFromKey()}
     * which is the immediate successor of the given {@code lastKey}.
     *
     * @param lastKey The last returned key from an iteration of the {@link TableSegmentIterator}.
     * @return The next {@link SegmentIteratorArgs} to use, or null of {@code lastKey} is null or if, as a result of this
     * method's computations, the next {@link SegmentIteratorArgs} would have {@link #getFromKey()} exceed
     * {@link #getToKey()} (which means a subsequent call to {@link TableSegmentIterator#getNext()} cannot possibly yield
     * any result).
     */
    SegmentIteratorArgs next(@Nullable ByteBuf lastKey) {
        if (lastKey == null) {
            // End of iteration.
            return null;
        }

        final byte[] result = lastKey.copy().array();
        int index = result.length - 1;
        ByteBuf resultBuf = null;
        while (index >= 0) {
            int v = result[index] & 0xFF;
            if (v >= 0xFF) {
                // Carryover.
                result[index] = 0;
            } else {
                // Found one.
                result[index] = (byte) (v + 1);
                resultBuf = Unpooled.wrappedBuffer(result);
                break;
            }

            index--;
        }

        return resultBuf == null || resultBuf.compareTo(this.toKey) > 0
                ? null
                : new SegmentIteratorArgs(resultBuf, this.toKey.copy(), this.maxItemsAtOnce);
    }
}
