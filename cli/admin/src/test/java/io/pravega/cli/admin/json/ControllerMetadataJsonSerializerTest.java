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
package io.pravega.cli.admin.json;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.CompletedTxnRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StateRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamCutReferenceRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.StreamSubscriber;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.stream.records.Subscribers;
import io.pravega.controller.store.stream.records.WriterMark;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ControllerMetadataJsonSerializerTest {

    @Test
    public void testStreamConfigurationRecord() {
        StreamConfiguration withScalingAndRetention = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .retentionPolicy(RetentionPolicy.bySizeBytes(1L))
                .build();
        StreamConfigurationRecord record1 = StreamConfigurationRecord.builder()
                .streamConfiguration(withScalingAndRetention)
                .streamName("a")
                .scope("a")
                .updating(true)
                .build();
        testRecordSerialization(record1, StreamConfigurationRecord.class);

        StreamConfiguration withScalingOnly = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        StreamConfigurationRecord record2 = StreamConfigurationRecord.builder()
                .streamConfiguration(withScalingOnly)
                .streamName("a")
                .scope("a")
                .updating(true)
                .build();
        testRecordSerialization(record2, StreamConfigurationRecord.class);

        StreamConfiguration withRetentionOnly = StreamConfiguration.builder()
                .scalingPolicy(null)
                .retentionPolicy(RetentionPolicy.bySizeBytes(1L))
                .build();
        StreamConfigurationRecord record3 = StreamConfigurationRecord.builder()
                .streamConfiguration(withRetentionOnly)
                .streamName("a")
                .scope("a")
                .updating(true)
                .build();
        testRecordSerialization(record3, StreamConfigurationRecord.class);
    }

    @Test
    public void testStreamTruncationRecord() {
        Map<StreamSegmentRecord, Integer> span = new HashMap<>();
        span.put(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0), 0);
        span.put(StreamSegmentRecord.newSegmentRecord(1, 0, 0L, 0.0, 1.0), 0);
        Map<Long, Long> streamCut = new HashMap<>();
        streamCut.put(0L, 0L);
        Set<Long> set = new HashSet<>();
        set.add(0L);
        StreamTruncationRecord record = new StreamTruncationRecord(ImmutableMap.copyOf(streamCut),
                ImmutableMap.copyOf(span), ImmutableSet.copyOf(set), ImmutableSet.copyOf(set), 0L, true);
        testRecordSerialization(record, StreamTruncationRecord.class);
    }

    @Test
    public void testStateRecord() {
        StateRecord record = new StateRecord(State.ACTIVE);
        testRecordSerialization(record, StateRecord.class);
    }

    @Test
    public void testEpochTransitionRecord() {
        ImmutableSet<Long> segmentsToSeal = ImmutableSet.of(1L, 2L);
        ImmutableMap<Long, Map.Entry<Double, Double>> newSegmentsWithRange =
                ImmutableMap.of(3L, Map.entry(0.1, 0.2), 4L, Map.entry(0.3, 0.4));
        EpochTransitionRecord record = new EpochTransitionRecord(10, 100L,
                segmentsToSeal, newSegmentsWithRange);
        testRecordSerialization(record, EpochTransitionRecord.class);
    }

    @Test
    public void testRetentionSet() {
        StreamCutReferenceRecord refRecord1 = StreamCutReferenceRecord.builder().recordingSize(0L).recordingTime(10L).build();
        StreamCutReferenceRecord refRecord2 = StreamCutReferenceRecord.builder().recordingSize(1L).recordingTime(11L).build();
        RetentionSet record = new RetentionSet(ImmutableList.of(refRecord1, refRecord2));
        testRecordSerialization(record, RetentionSet.class);
    }

    @Test
    public void testStreamCutRecord() {
        StreamCutRecord record = new StreamCutRecord(10L, 10L, ImmutableMap.of(1L, 2L, 3L, 4L));
        testRecordSerialization(record, StreamCutRecord.class);
    }

    @Test
    public void testEpochRecord() {
        List<StreamSegmentRecord> list = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(1, 0, 10L, 0.0, 1.0));
        EpochRecord record = new EpochRecord(10, 0, ImmutableList.copyOf(list), 10L,
                0L, 0L);
        testRecordSerialization(record, EpochRecord.class);
    }

    @Test
    public void testHistoryTimeSeries() {
        List<StreamSegmentRecord> sealedSegments = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0));
        List<StreamSegmentRecord> newSegments = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0),
                StreamSegmentRecord.newSegmentRecord(1, 1, 0L, 0.1, 0.2));
        HistoryTimeSeriesRecord node = new HistoryTimeSeriesRecord(0, 0, ImmutableList.copyOf(sealedSegments),
                ImmutableList.copyOf(newSegments), 0L);
        HistoryTimeSeriesRecord node2 = new HistoryTimeSeriesRecord(1, 0,
                ImmutableList.of(), ImmutableList.of(), 1L);
        HistoryTimeSeriesRecord node3 = new HistoryTimeSeriesRecord(4, 4,
                ImmutableList.copyOf(sealedSegments), ImmutableList.copyOf(newSegments), 1L);
        HistoryTimeSeries record = new HistoryTimeSeries(ImmutableList.of(node, node2, node3));
        testRecordSerialization(record, HistoryTimeSeries.class);
    }

    @Test
    public void testSealedSegmentsMapShard() {
        Map<Long, Long> map = new HashMap<>();
        map.put(0L, 0L);
        map.put(1L, 0L);
        map.put(2L, 0L);
        SealedSegmentsMapShard record = SealedSegmentsMapShard.builder().sealedSegmentsSizeMap(map).build();
        testRecordSerialization(record, SealedSegmentsMapShard.class);
    }

    @Test
    public void testCommittingTransactionsRecord() {
        List<UUID> list = Lists.newArrayList(UUID.randomUUID(), UUID.randomUUID());
        CommittingTransactionsRecord record0 =
                new CommittingTransactionsRecord(0, ImmutableList.copyOf(list));
        testRecordSerialization(record0, CommittingTransactionsRecord.class);
        CommittingTransactionsRecord record = record0.createRollingTxnRecord(10);
        testRecordSerialization(record, CommittingTransactionsRecord.class);
    }

    @Test
    public void testStreamSubscriber() {
        StreamSubscriber record = new StreamSubscriber("a", 1L,
                ImmutableMap.of(1L, 2L, 3L, 4L), 10L);
        testRecordSerialization(record, StreamSubscriber.class);
    }

    @Test
    public void testSubscribers() {
        Subscribers record = new Subscribers(ImmutableSet.of("a", "b"));
        testRecordSerialization(record, Subscribers.class);
    }

    @Test
    public void testWriterMark() {
        Map<Long, Long> map = new HashMap<>();
        map.put(0L, 0L);
        map.put(1L, 0L);
        map.put(2L, 0L);
        WriterMark record = new WriterMark(100L, ImmutableMap.copyOf(map), true);
        testRecordSerialization(record, WriterMark.class);
    }

    @Test
    public void testActiveTxnRecord() {
        ActiveTxnRecord record = new ActiveTxnRecord(1L, 1L, 1L,
                TxnStatus.OPEN, "a", 1L, 1L, ImmutableMap.of(1L, 2L, 3L, 4L));
        testRecordSerialization(record, ActiveTxnRecord.class);
    }

    @Test
    public void testCompletedTxnRecord() {
        CompletedTxnRecord record = new CompletedTxnRecord(1L, TxnStatus.COMMITTED);
        testRecordSerialization(record, CompletedTxnRecord.class);
    }

    @Test
    public void testPrimitives() {
        int record1 = 1;
        testRecordSerialization(record1, Integer.class);
        long record2 = 2L;
        testRecordSerialization(record2, Long.class);
        String record3 = "testString";
        testRecordSerialization(record3, String.class);
    }

    private static <T> void testRecordSerialization(T record, Class<T> type) {
        ControllerMetadataJsonSerializer jsonSerializer = new ControllerMetadataJsonSerializer();
        String jsonString = jsonSerializer.toJson(record);
        Assert.assertEquals(record, jsonSerializer.fromJson(jsonString, type));
    }
}
