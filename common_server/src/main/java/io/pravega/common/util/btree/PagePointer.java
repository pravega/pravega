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
package io.pravega.common.util.btree;

import io.pravega.common.util.ByteArraySegment;
import lombok.Getter;

/**
 * Pointer to a BTreePage.
 */
@Getter
class PagePointer {
    /**
     * Reserved value to indicate an offset has not yet been assigned.
     */
    static final long NO_OFFSET = -1L;
    private final ByteArraySegment key;
    private final long offset;
    private final int length;
    private final long minOffset;

    /**
     * Creates a new instance of the PagePointer class.
     *
     * @param key    A ByteArraySegment representing the key. If supplied (may be null), a copy of this will be kept.
     * @param offset Desired Page's Offset.
     * @param length Desired Page's Length.
     */
    PagePointer(ByteArraySegment key, long offset, int length) {
        this(key, offset, length, NO_OFFSET);
    }

    /**
     * Creates a new instance of the PagePointer class.
     *
     * @param key       A ByteArraySegment representing the key. If supplied (may be null), a copy of this will be kept.
     * @param offset    Desired Page's Offset.
     * @param length    Desired Page's Length.
     * @param minOffset Desired page's MinOffset (including that of any of its descendants).
     */
    PagePointer(ByteArraySegment key, long offset, int length, long minOffset) {
        // Make a copy of the key. ByteArraySegments are views into another array (of the BTreePage). As such, if the
        // BTreePage is modified (in case the first key is deleted), this view may return a different set of bytes.
        this.key = key == null ? null : new ByteArraySegment(key.getCopy());
        this.offset = offset;
        this.length = length;
        this.minOffset = minOffset;
    }

    @Override
    public String toString() {
        return String.format("Offset = %s, Length = %s, MinOffset = %s", this.offset, this.length, this.minOffset);
    }
}
