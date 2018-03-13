/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables.serializer;

import com.google.common.collect.Lists;
import io.pravega.controller.store.stream.StreamCutRecord;
import io.pravega.controller.store.stream.tables.RetentionRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ControllerMetadataSerializerTest {

    @Test
    public void streamCutRecordTest() throws IOException {
        Map<Integer, Long> streamcut = new HashMap<>();
        streamcut.put(0, 1L);
        streamcut.put(1, 1L);
        StreamCutRecord record = new StreamCutRecord(1L, 1L, streamcut);
        byte[] serialized = StreamCutRecord.SERIALIZER_V1.serialize(record).array();
        StreamCutRecord deserialized = StreamCutRecord.SERIALIZER_V1.deserialize(serialized);
        assertEquals(record, deserialized);
    }

    @Test
    public void retentionRecordTest() throws IOException {
        Map<Integer, Long> map = new HashMap<>();
        map.put(0, 1L);
        map.put(1, 1L);
        StreamCutRecord s1 = new StreamCutRecord(1L, 1L, map);
        StreamCutRecord s2 = new StreamCutRecord(1L, 1L, map);
        List<StreamCutRecord> streamCuts = Lists.newArrayList(s1, s2);
        RetentionRecord record = RetentionRecord.builder().streamCuts(streamCuts).build();
        byte[] serialized = RetentionRecord.SERIALIZER_V1.serialize(record).array();
        RetentionRecord deserialized = RetentionRecord.SERIALIZER_V1.deserialize(serialized);
        assertEquals(record, deserialized);
    }
}

