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

import io.pravega.client.control.impl.SegmentCollection;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.common.hash.HashHelper;
import java.nio.ByteBuffer;
import java.util.NavigableMap;
import lombok.EqualsAndHashCode;

/**
 * The Segments within a KeyValueTable.
 */
@EqualsAndHashCode(callSuper = true)
public class KeyValueTableSegments extends SegmentCollection {
    private static final HashHelper HASHER = HashHelper.seededWith("KeyValueTableRouter"); // DO NOT change this string.

    /**
     * Creates a new instance of the KeyValueTableSegments class.
     *
     * @param segments        Segments keyed by the largest key in their key range.
     *                        i.e. If there are two segments split evenly, the first should have a value of 0.5 and the second 1.0.
     */
    public KeyValueTableSegments(NavigableMap<Double, SegmentWithRange> segments) {
        super(segments);
    }

    /**
     * Gets the {@link Segment} that the given Key hashes to. This should be used for those keys that do not use Key
     * Families for hashing.
     *
     * @param keySerialization A {@link ByteBuffer} representing the serialization of the key.
     * @return A {@link Segment}.
     */
    Segment getSegmentForKey(ByteBuffer keySerialization) {
        return getSegmentForKey(HASHER.hashToRange(keySerialization));
    }

    /**
     * Gets the total number of Segments in this collection.
     *
     * @return The total number of Segments.
     */
    int getSegmentCount() {
        return super.segments.size();
    }

    @Override
    protected double hashToRange(String key) {
        return HASHER.hashToRange(key);
    }
}
