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

import io.netty.buffer.Unpooled;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link KeyValueTableIteratorState} class.
 */
public class KeyValueTableIteratorStateTests {
    @Test
    public void testSerialization() {
        val baseNullState = new KeyValueTableIteratorState("Segment", 123L, null);
        val deserializedNullState = KeyValueTableIteratorState.fromBytes(baseNullState.toBytes());
        assertEqual(baseNullState, deserializedNullState);

        val baseNonNullState = new KeyValueTableIteratorState("Segment", 123L, Unpooled.wrappedBuffer("123".getBytes()));
        val deserializedNonNullState = KeyValueTableIteratorState.fromBytes(baseNonNullState.toBytes());
        assertEqual(baseNonNullState, deserializedNonNullState);
    }

    private void assertEqual(KeyValueTableIteratorState e, KeyValueTableIteratorState a) {
        Assert.assertEquals("KeyValueTableName", e.getKeyValueTableName(), a.getKeyValueTableName());
        Assert.assertEquals("SegmentId", e.getSegmentId(), a.getSegmentId());
        Assert.assertEquals("SegmentIteratorState", e.getSegmentIteratorState(), a.getSegmentIteratorState());
    }
}
