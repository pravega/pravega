/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables.serializers;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.SegmentRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StateRecord;
import io.pravega.controller.store.stream.tables.StreamConfigurationRecord;
import io.pravega.controller.store.stream.tables.StreamCutRecord;
import io.pravega.controller.store.stream.tables.RetentionRecord;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ControllerMetadataSerializerTest {

    @Test
    public void streamCutRecordTest() throws IOException {
        Map<Long, Long> streamcut = new HashMap<>();
        streamcut.put(0L, 1L);
        streamcut.put(1L, 1L);
        StreamCutRecord record = new StreamCutRecord(1L, 1L, streamcut);
        byte[] serialized = StreamCutRecord.SERIALIZER.serialize(record).array();
        StreamCutRecord deserialized = StreamCutRecord.SERIALIZER.deserialize(serialized);
        assertEquals(record, deserialized);
    }

    @Test
    public void retentionRecordTest() throws IOException {
        Map<Long, Long> map = new HashMap<>();
        map.put(0L, 1L);
        map.put(1L, 1L);
        StreamCutRecord s1 = new StreamCutRecord(1L, 1L, map);
        StreamCutRecord s2 = new StreamCutRecord(1L, 1L, map);
        List<StreamCutRecord> streamCuts = Lists.newArrayList(s1, s2);
        RetentionRecord record = RetentionRecord.builder().streamCuts(streamCuts).build();
        byte[] serialized = RetentionRecord.SERIALIZER.serialize(record).array();
        RetentionRecord deserialized = RetentionRecord.SERIALIZER.deserialize(serialized);
        assertEquals(record, deserialized);
    }

    @Test
    public void txnRecordTest() throws IOException {
        ActiveTxnRecord record = new ActiveTxnRecord(1L, 1L, 1L, TxnStatus.OPEN);
        byte[] serialized = ActiveTxnRecord.SERIALIZER.serialize(record).array();
        ActiveTxnRecord deserialized = ActiveTxnRecord.SERIALIZER.deserialize(serialized);
        assertEquals(record, deserialized);

        CompletedTxnRecord record2 = new CompletedTxnRecord(1L, TxnStatus.COMMITTED);
        byte[] serialized2 = CompletedTxnRecord.SERIALIZER.serialize(record2).array();
        CompletedTxnRecord deserialized2 = CompletedTxnRecord.SERIALIZER.deserialize(serialized2);
        assertEquals(record2, deserialized2);
    }

    @Test
    public void stateRecordTest() throws IOException {
        StateRecord record = new StateRecord(State.ACTIVE);
        byte[] serialized = StateRecord.SERIALIZER.serialize(record).array();
        StateRecord deserialized = StateRecord.SERIALIZER.deserialize(serialized);
        assertEquals(record, deserialized);
    }

    @Test
    public void truncationRecordTest() throws IOException {
        Map<Long, Long> streamCut = new HashMap<>();
        streamCut.put(0L, 1L);
        streamCut.put(2L, 1L);
        streamCut.put(3L, 1L);

        ImmutableMap<Long, Integer> epochMap = ImmutableMap.copyOf(new HashMap<>());
        ImmutableSet<Long> deleted = ImmutableSet.copyOf(new HashSet<>());
        StreamTruncationRecord record = StreamTruncationRecord.builder().cutEpochMap(epochMap)
                .streamCut(ImmutableMap.copyOf(streamCut))
                .deletedSegments(deleted)
                .toDelete(ImmutableSet.copyOf(new HashSet<>()))
                .updating(true).build();

        byte[] serialized = StreamTruncationRecord.SERIALIZER.serialize(record).array();
        StreamTruncationRecord deserialized = StreamTruncationRecord.SERIALIZER.deserialize(serialized);
        assertEquals(record, deserialized);
    }

    @Test
    public void configurationRecordTest() throws IOException {
        StreamConfiguration withScalingAndRetention = StreamConfiguration.builder().streamName("a").scope("a")
                .scalingPolicy(ScalingPolicy.fixed(1)).retentionPolicy(RetentionPolicy.bySizeBytes(1L)).build();
        StreamConfiguration withScalingOnly = StreamConfiguration.builder().streamName("a").scope("a")
                .retentionPolicy(RetentionPolicy.bySizeBytes(1L)).build();
        StreamConfiguration withRetentiononly = StreamConfiguration.builder().streamName("a").scope("a")
                .retentionPolicy(RetentionPolicy.bySizeBytes(1L)).build();

        StreamConfigurationRecord record = StreamConfigurationRecord.builder().streamConfiguration(withScalingAndRetention)
                .updating(true).build();
        byte[] serialized = StreamConfigurationRecord.SERIALIZER.serialize(record).array();
        StreamConfigurationRecord deserialized = StreamConfigurationRecord.SERIALIZER.deserialize(serialized);
        assertEquals(record, deserialized);

        record = StreamConfigurationRecord.builder().streamConfiguration(withScalingOnly)
                .updating(true).build();
        serialized = StreamConfigurationRecord.SERIALIZER.serialize(record).array();
        deserialized = StreamConfigurationRecord.SERIALIZER.deserialize(serialized);
        assertEquals(record, deserialized);

        record = StreamConfigurationRecord.builder().streamConfiguration(withRetentiononly)
                .updating(true).build();
        serialized = StreamConfigurationRecord.SERIALIZER.serialize(record).array();
        deserialized = StreamConfigurationRecord.SERIALIZER.deserialize(serialized);
        assertEquals(record, deserialized);
    }

    @Test
    public void segmentRecordTest() throws IOException {
        SegmentRecord record = SegmentRecord.builder().creationEpoch(0).routingKeyEnd(0.0).routingKeyEnd(0.1).startTime(1L).segmentNumber(1).build();
        byte[] serialized = SegmentRecord.SERIALIZER.serialize(record).array();
        SegmentRecord deserialized = SegmentRecord.SERIALIZER.deserialize(serialized);
        assertEquals(record, deserialized);
    }

    @Test
    public void historyRecordTest() throws IOException {
        List<Long> segments = Lists.newArrayList(1L, 2L, 3L);
        HistoryRecord record = HistoryRecord.builder().epoch(0).creationTime(System.currentTimeMillis()).segments(segments).build();
        byte[] serialized = HistoryRecord.SERIALIZER.serialize(record).array();
        HistoryRecord deserialized = HistoryRecord.SERIALIZER.deserialize(serialized);
        assertEquals(record, deserialized);
    }

    @Test
    public void epochTransitionRecordTest() throws IOException {
        Map<Long, AbstractMap.SimpleEntry<Double, Double>> map = new HashMap<>();
        map.put(0L, new AbstractMap.SimpleEntry<>(0.2, 1.0));
        map.put(1L, new AbstractMap.SimpleEntry<>(0.3, 3.0));
        map.put(2L, new AbstractMap.SimpleEntry<>(0.4, 1.0));
        map.put(3L, new AbstractMap.SimpleEntry<>(0.1, 2.0));
        Set<Long> set = new HashSet<>();
        set.add(0L);
        set.add(1L);
        set.add(2L);
        EpochTransitionRecord record = EpochTransitionRecord.builder().activeEpoch(0).time(1L)
                .newSegmentsWithRange(ImmutableMap.copyOf(map)).segmentsToSeal(ImmutableSet.copyOf(set)).build();
        byte[] serialized = EpochTransitionRecord.SERIALIZER.serialize(record).array();
        EpochTransitionRecord deserialized = EpochTransitionRecord.SERIALIZER.deserialize(serialized);
        assertEquals(record, deserialized);
    }
}

