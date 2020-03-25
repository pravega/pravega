/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.pravega.client.control.impl.SegmentCollection;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.common.hash.HashHelper;
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
     * @param delegationToken Delegation token to access the segments in the segmentstore
     */
    public KeyValueTableSegments(NavigableMap<Double, SegmentWithRange> segments, String delegationToken) {
        super(segments, delegationToken);
    }

    /**
     * Gets the {@link Segment} that the given Key hashes to. This should be used for those keys that do not use Key
     * Families for hashing.
     *
     * @param keySerialization A {@link ByteBuf} representing the serialization of the key.
     * @return A {@link Segment}.
     */
    Segment getSegmentForKey(ByteBuf keySerialization) {
        return getSegmentForKey(HASHER.hashToRange(keySerialization.nioBuffers()));
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
