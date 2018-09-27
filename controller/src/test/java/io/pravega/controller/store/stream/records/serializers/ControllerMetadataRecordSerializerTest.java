/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records.serializers;

import com.google.common.collect.Lists;
import io.pravega.controller.store.stream.records.CommitTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeIndexLeaf;
import io.pravega.controller.store.stream.records.HistoryTimeIndexRootNode;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.RetentionSetRecord;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.TruncationRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ControllerMetadataRecordSerializerTest {

    @Test
    public void commitTransactionsRecordTest() {
        List<UUID> list = Lists.newArrayList(UUID.randomUUID(), UUID.randomUUID());
        CommitTransactionsRecord commitTransactionsRecord = CommitTransactionsRecord.builder().epoch(0).transactionsToCommit(list).build();
        assertEquals(CommitTransactionsRecord.parse(commitTransactionsRecord.toByteArray()), commitTransactionsRecord);
        CommitTransactionsRecord updated = commitTransactionsRecord.getRollingTxnRecord(10);
        assertNotEquals(CommitTransactionsRecord.parse(updated.toByteArray()), commitTransactionsRecord);
        assertEquals(CommitTransactionsRecord.parse(updated.toByteArray()), updated);
    }

    @Test
    public void epochRecordTest() {
        List<StreamSegmentRecord> list = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(1, 0, 10L, 0.0, 1.0));
        EpochRecord record = EpochRecord.builder().epoch(10).referenceEpoch(0).creationTime(10L).segments(list).build();
        assertEquals(EpochRecord.parse(record.toByteArray()), record);
    }

    @Test
    public void historyTimeIndexTest() {
        List<Long> leaves = Lists.newArrayList(0L, 100L, 200L);
        HistoryTimeIndexRootNode rootNode = HistoryTimeIndexRootNode.builder().leaves(leaves).build();
        assertEquals(HistoryTimeIndexRootNode.parse(rootNode.toByteArray()), rootNode);

        List<Long> record = Lists.newArrayList(0L, 1L, 2L);

        HistoryTimeIndexLeaf leaf = HistoryTimeIndexLeaf.builder().records(record).build();
        assertEquals(HistoryTimeIndexLeaf.parse(leaf.toByteArray()), leaf);
    }

    @Test
    public void historyTimeSeriesTest() {
        List<StreamSegmentRecord> sealedSegments = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0));
        List<StreamSegmentRecord> newSegments = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0));
        HistoryTimeSeriesRecord node = HistoryTimeSeriesRecord.builder().epoch(0).creationTime(0L).referenceEpoch(0).segmentsCreated(newSegments).segmentsSealed(sealedSegments).build();

        assertEquals(HistoryTimeSeriesRecord.parse(node.toByteArray()), node);

        HistoryTimeSeries timeSeries = HistoryTimeSeries.builder().historyRecords(Lists.newArrayList(node)).build();
        assertEquals(HistoryTimeSeries.parse(timeSeries.toByteArray()), timeSeries);
    }

    @Test
    public void retentionSetTest() {
        RetentionSetRecord record = RetentionSetRecord.builder().recordingSize(0L).recordingTime(10L).build();
        assertEquals(RetentionSetRecord.parse(record.toByteArray()), record);

        RetentionSet set = RetentionSet.builder().retentionRecords(Lists.newArrayList(record)).build();
        assertEquals(RetentionSet.parse(set.toByteArray()), set);
    }

    @Test
    public void sealedSegmentSizesMapShardTest() {
        Map<Long, Long> map = new HashMap<>();
        map.put(0L, 0L);
        map.put(1L, 0L);
        map.put(2L, 0L);
        SealedSegmentsMapShard record = SealedSegmentsMapShard.builder().shardNumber(0).sealedSegmentsSizeMap(map).build();
        assertEquals(SealedSegmentsMapShard.parse(record.toByteArray()), record);
    }

    @Test
    public void streamTruncationRecordTest() {
        Map<StreamSegmentRecord, Integer> span = new HashMap<>();
        span.put(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0), 0);
        Map<Long, Long> streamCut = new HashMap<>();
        streamCut.put(0L, 0L);
        Set<Long> set = new HashSet<>();
        set.add(0L);
        TruncationRecord record = TruncationRecord.builder().span(span).streamCut(streamCut).toDelete(set)
                .deletedSegments(set).updating(false).build();
        assertEquals(TruncationRecord.parse(record.toByteArray()), record);
    }
}

