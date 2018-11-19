/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import com.google.common.collect.Lists;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.State;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ControllerMetadataRecordSerializerTest {

    @Test
    public void commitTransactionsRecordTest() {
        List<UUID> list = Lists.newArrayList(UUID.randomUUID(), UUID.randomUUID());
        CommittingTransactionsRecord commitTransactionsRecord = CommittingTransactionsRecord.builder().epoch(0).transactionsToCommit(list).build();
        assertEquals(CommittingTransactionsRecord.fromBytes(commitTransactionsRecord.toBytes()), commitTransactionsRecord);
        CommittingTransactionsRecord updated = commitTransactionsRecord.createRollingTxnRecord(10);
        assertNotEquals(CommittingTransactionsRecord.fromBytes(updated.toBytes()), commitTransactionsRecord);
        assertEquals(CommittingTransactionsRecord.fromBytes(updated.toBytes()), updated);
    }

    @Test
    public void epochRecordTest() {
        List<StreamSegmentRecord> list = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(1, 0, 10L, 0.0, 1.0));
        EpochRecord record = EpochRecord.builder().epoch(10).referenceEpoch(0).creationTime(10L).segments(list).build();
        assertEquals(EpochRecord.fromBytes(record.toBytes()), record);
    }
    
    @Test
    public void historyTimeSeriesTest() {
        List<StreamSegmentRecord> sealedSegments = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0));
        List<StreamSegmentRecord> newSegments = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0));
        HistoryTimeSeriesRecord node = HistoryTimeSeriesRecord.builder().epoch(0).creationTime(0L).referenceEpoch(0)
                                                              .segmentsCreated(newSegments).segmentsSealed(sealedSegments)
                                                              .build();

        assertEquals(HistoryTimeSeriesRecord.fromBytes(node.toBytes()), node);

        HistoryTimeSeries timeSeries = HistoryTimeSeries.builder().historyRecords(Lists.newArrayList(node)).build();
        assertEquals(HistoryTimeSeries.fromBytes(timeSeries.toBytes()), timeSeries);

        HistoryTimeSeries newTimeSeries = HistoryTimeSeries.addHistoryRecord(timeSeries, node);
        assertEquals(newTimeSeries, timeSeries);

        AssertExtensions.assertThrows(IllegalArgumentException.class, 
                () -> HistoryTimeSeriesRecord.builder().epoch(1).creationTime(1L).referenceEpoch(0)
                               .segmentsCreated(newSegments).segmentsSealed(sealedSegments)
                               .build());
        
        HistoryTimeSeriesRecord node2 = HistoryTimeSeriesRecord.builder().epoch(1).creationTime(1L).referenceEpoch(0)
                                                               .build();

        newTimeSeries = HistoryTimeSeries.addHistoryRecord(timeSeries, node2);
        assertEquals(newTimeSeries.getLatestRecord(), node2);

        HistoryTimeSeriesRecord node3 = HistoryTimeSeriesRecord.builder().epoch(4).creationTime(1L).referenceEpoch(4)
                                                               .segmentsCreated(newSegments).segmentsSealed(sealedSegments)
                                                               .build();

        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> HistoryTimeSeries.addHistoryRecord(timeSeries, node3));
    }

    @Test
    public void retentionSetTest() {
        StreamCutReferenceRecord record = StreamCutReferenceRecord.builder().recordingSize(0L).recordingTime(10L).build();
        assertEquals(StreamCutReferenceRecord.fromBytes(record.toBytes()), record);

        RetentionSet set = RetentionSet.builder().retentionRecords(Lists.newArrayList(record)).build();
        assertEquals(RetentionSet.fromBytes(set.toBytes()), set);
    }

    @Test
    public void retentionStreamCutRecordTest() {
        Map<Long, Long> cut = new HashMap<>();
        cut.put(0L, 0L);
        StreamCutRecord record = StreamCutRecord.builder().recordingSize(100L).recordingTime(10L).streamCut(cut).build();
        assertEquals(StreamCutRecord.fromBytes(record.toBytes()), record);

        assertTrue(record.getReferenceRecord().getRecordingTime() == 10L && record.getReferenceRecord().getRecordingSize() == 100L);
    }

    @Test
    public void sealedSegmentSizesMapShardTest() {
        Map<Long, Long> map = new HashMap<>();
        map.put(0L, 0L);
        map.put(1L, 0L);
        map.put(2L, 0L);
        SealedSegmentsMapShard record = SealedSegmentsMapShard.builder().shardNumber(0).sealedSegmentsSizeMap(map).build();
        assertEquals(SealedSegmentsMapShard.fromBytes(record.toBytes()), record);

        record.addSealedSegmentSize(4L, 10L);
        assertTrue(record.getSealedSegmentsSizeMap().containsKey(4L));
        record.addSealedSegmentSize(1L, 10L);
    }

    @Test
    public void streamTruncationRecordTest() {
        Map<StreamSegmentRecord, Integer> span = new HashMap<>();
        span.put(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0), 0);
        Map<Long, Long> streamCut = new HashMap<>();
        streamCut.put(0L, 0L);
        Set<Long> set = new HashSet<>();
        set.add(0L);
        StreamTruncationRecord record = StreamTruncationRecord.builder().span(span).streamCut(streamCut).toDelete(set)
                                                              .deletedSegments(set).updating(true).build();
        assertEquals(StreamTruncationRecord.fromBytes(record.toBytes()), record);
        assertTrue(record.isUpdating());
        StreamTruncationRecord completed = StreamTruncationRecord.complete(record);
        assertEquals(StreamTruncationRecord.fromBytes(completed.toBytes()), completed);
        assertTrue(!completed.isUpdating());
    }

    @Test
    public void txnRecordTest() throws IOException {
        ActiveTxnRecord record = new ActiveTxnRecord(1L, 1L, 1L, TxnStatus.OPEN);
        byte[] serialized = record.toBytes();
        ActiveTxnRecord deserialized = ActiveTxnRecord.fromBytes(serialized);
        assertEquals(record, deserialized);

        CompletedTxnRecord record2 = new CompletedTxnRecord(1L, TxnStatus.COMMITTED);
        byte[] serialized2 = record2.toBytes();
        CompletedTxnRecord deserialized2 = CompletedTxnRecord.fromBytes(serialized2);
        assertEquals(record2, deserialized2);
    }

    @Test
    public void stateRecordTest() throws IOException {
        StateRecord record = new StateRecord(State.ACTIVE);
        byte[] serialized = record.toBytes();
        StateRecord deserialized = StateRecord.fromBytes(serialized);
        assertEquals(record, deserialized);
    }

    @Test
    public void configurationRecordTest() throws IOException {
        StreamConfiguration withScalingAndRetention = StreamConfiguration.builder()
                                                                         .scalingPolicy(ScalingPolicy.fixed(1)).retentionPolicy(RetentionPolicy.bySizeBytes(1L)).build();
        StreamConfiguration withScalingOnly = StreamConfiguration.builder()
                                                                 .retentionPolicy(RetentionPolicy.bySizeBytes(1L)).build();
        StreamConfiguration withRetentiononly = StreamConfiguration.builder()
                                                                   .retentionPolicy(RetentionPolicy.bySizeBytes(1L)).build();

        StreamConfigurationRecord record = StreamConfigurationRecord.builder().streamConfiguration(withScalingAndRetention)
                                                                    .updating(true).build();
        byte[] serialized = record.toBytes();
        StreamConfigurationRecord deserialized = StreamConfigurationRecord.fromBytes(serialized);
        assertEquals(record, deserialized);

        record = StreamConfigurationRecord.builder().streamConfiguration(withScalingOnly)
                                          .updating(true).build();
        serialized = record.toBytes();
        deserialized = StreamConfigurationRecord.fromBytes(serialized);
        assertEquals(record, deserialized);

        record = StreamConfigurationRecord.builder().streamConfiguration(withRetentiononly)
                                          .updating(true).build();
        serialized = record.toBytes();
        deserialized = StreamConfigurationRecord.fromBytes(serialized);
        assertEquals(record, deserialized);
    }


}

