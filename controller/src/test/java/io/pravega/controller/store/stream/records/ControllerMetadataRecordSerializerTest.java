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
package io.pravega.controller.store.stream.records;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.State;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
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
        CommittingTransactionsRecord commitTransactionsRecord = 
                new CommittingTransactionsRecord(0, ImmutableList.copyOf(list));
        assertEquals(CommittingTransactionsRecord.fromBytes(commitTransactionsRecord.toBytes()), commitTransactionsRecord);
        CommittingTransactionsRecord updated = commitTransactionsRecord.createRollingTxnRecord(10);
        assertNotEquals(CommittingTransactionsRecord.fromBytes(updated.toBytes()), commitTransactionsRecord);
        assertEquals(CommittingTransactionsRecord.fromBytes(updated.toBytes()), updated);
    }

    @Test
    public void epochRecordTest() {
        List<StreamSegmentRecord> list = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(1, 0, 10L, 0.0, 1.0));
        EpochRecord record = new EpochRecord(10, 0, ImmutableList.copyOf(list), 10L,
                0L, 0L);
        assertEquals(EpochRecord.fromBytes(record.toBytes()), record);
    }
    
    @Test
    public void historyTimeSeriesTest() {
        List<StreamSegmentRecord> sealedSegments = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0));
        List<StreamSegmentRecord> newSegments = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0));
        HistoryTimeSeriesRecord node = new HistoryTimeSeriesRecord(0, 0, ImmutableList.copyOf(sealedSegments),
                ImmutableList.copyOf(newSegments), 0L);

        assertEquals(HistoryTimeSeriesRecord.fromBytes(node.toBytes()), node);

        HistoryTimeSeries timeSeries = new HistoryTimeSeries(ImmutableList.of(node));
        assertEquals(HistoryTimeSeries.fromBytes(timeSeries.toBytes()), timeSeries);

        HistoryTimeSeries newTimeSeries = HistoryTimeSeries.addHistoryRecord(timeSeries, node);
        assertEquals(newTimeSeries, timeSeries);

        AssertExtensions.assertThrows(IllegalArgumentException.class, 
                () -> new HistoryTimeSeriesRecord(1, 0, ImmutableList.copyOf(sealedSegments), 
                        ImmutableList.copyOf(newSegments), 1L));
        
        HistoryTimeSeriesRecord node2 = new HistoryTimeSeriesRecord(1, 0, 1L);

        newTimeSeries = HistoryTimeSeries.addHistoryRecord(timeSeries, node2);
        assertEquals(newTimeSeries.getLatestRecord(), node2);

        HistoryTimeSeriesRecord node3 = new HistoryTimeSeriesRecord(4, 4,
                ImmutableList.copyOf(sealedSegments), ImmutableList.copyOf(newSegments), 1L);

        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> HistoryTimeSeries.addHistoryRecord(timeSeries, node3));
    }

    @Test
    public void retentionSetTest() {
        StreamCutReferenceRecord record = StreamCutReferenceRecord.builder().recordingSize(0L).recordingTime(10L).build();
        assertEquals(StreamCutReferenceRecord.fromBytes(record.toBytes()), record);

        RetentionSet set = new RetentionSet(ImmutableList.of(record));
        assertEquals(RetentionSet.fromBytes(set.toBytes()), set);
    }

    @Test
    public void retentionStreamCutRecordTest() {
        Map<Long, Long> cut = new HashMap<>();
        cut.put(0L, 0L);
        StreamCutRecord record = new StreamCutRecord(10L, 100L, ImmutableMap.copyOf(cut));
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
        StreamTruncationRecord record = new StreamTruncationRecord(ImmutableMap.copyOf(streamCut),
                ImmutableMap.copyOf(span), ImmutableSet.copyOf(set), ImmutableSet.copyOf(set), 0L, true);
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
                                                                         .scalingPolicy(ScalingPolicy.fixed(1))
                                                                         .retentionPolicy(RetentionPolicy.bySizeBytes(1L))
                                                                         .build();
        StreamConfiguration withScalingOnly = StreamConfiguration.builder()
                                                                 .retentionPolicy(RetentionPolicy.bySizeBytes(1L))
                                                                 .build();
        StreamConfiguration withRetentiononly = StreamConfiguration.builder()
                                                                   .retentionPolicy(RetentionPolicy.bySizeBytes(1L))
                                                                   .build();

        StreamConfigurationRecord record = StreamConfigurationRecord.builder()
                                                                    .streamConfiguration(withScalingAndRetention)
                                                                    .streamName("a")
                                                                    .scope("a")
                                                                    .updating(true)
                                                                    .build();
        byte[] serialized = record.toBytes();
        StreamConfigurationRecord deserialized = StreamConfigurationRecord.fromBytes(serialized);
        assertEquals(record, deserialized);

        record = StreamConfigurationRecord.builder()
                                          .streamConfiguration(withScalingOnly)
                                          .streamName("a")
                                          .scope("a")
                                          .updating(true)
                                          .build();
        serialized = record.toBytes();
        deserialized = StreamConfigurationRecord.fromBytes(serialized);
        assertEquals(record, deserialized);

        record = StreamConfigurationRecord.builder()
                                          .streamConfiguration(withRetentiononly)
                                          .streamName("a")
                                          .scope("a")
                                          .updating(true)
                                          .build();
        serialized = record.toBytes();
        deserialized = StreamConfigurationRecord.fromBytes(serialized);
        assertEquals(record, deserialized);

        StreamConfiguration allWithTime = StreamConfiguration.builder()
                                                             .scalingPolicy(ScalingPolicy.fixed(1))
                                                             .retentionPolicy(RetentionPolicy.bySizeBytes(1L))
                                                             .timestampAggregationTimeout(1000L)
                                                             .build();

        record = StreamConfigurationRecord.builder()
                                          .streamConfiguration(allWithTime)
                                          .streamName("a")
                                          .scope("a")
                                          .updating(true)
                                          .build();
        serialized = record.toBytes();
        deserialized = StreamConfigurationRecord.fromBytes(serialized);
        assertEquals(record, deserialized);
    }
    
    @Test
    public void configurationRecordWithRetentionPolicyTest() throws IOException {
        StreamConfiguration sizeBasedRetention = StreamConfiguration.builder()
                                                                    .retentionPolicy(RetentionPolicy.bySizeBytes(1L))
                                                                    .build();
        StreamConfiguration timeBasedRetention = StreamConfiguration.builder()
                                                                    .retentionPolicy(RetentionPolicy.byTime(Duration.ofMinutes(1)))
                                                                    .build();
        StreamConfiguration cbrSize = StreamConfiguration.builder()
                                                         .retentionPolicy(RetentionPolicy.bySizeBytes(1L, 10L))
                                                         .build();
        StreamConfiguration cbrtime = StreamConfiguration.builder()
                                                         .retentionPolicy(RetentionPolicy.byTime(Duration.ofMillis(1L), Duration.ofMillis(10L)))
                                                         .build();

        StreamConfigurationRecord record = StreamConfigurationRecord.builder()
                                                                    .streamConfiguration(sizeBasedRetention)
                                                                    .streamName("a")
                                                                    .scope("a")
                                                                    .updating(true)
                                                                    .build();
        byte[] serialized = record.toBytes();
        StreamConfigurationRecord deserialized = StreamConfigurationRecord.fromBytes(serialized);
        assertEquals(record, deserialized);

        record = StreamConfigurationRecord.builder()
                                          .streamConfiguration(timeBasedRetention)
                                          .streamName("a")
                                          .scope("a")
                                          .updating(true)
                                          .build();
        serialized = record.toBytes();
        deserialized = StreamConfigurationRecord.fromBytes(serialized);
        assertEquals(record, deserialized);

        record = StreamConfigurationRecord.builder()
                                          .streamConfiguration(cbrSize)
                                          .streamName("a")
                                          .scope("a")
                                          .updating(true)
                                          .build();
        serialized = record.toBytes();
        deserialized = StreamConfigurationRecord.fromBytes(serialized);
        assertEquals(record, deserialized);
        
        record = StreamConfigurationRecord.builder()
                                          .streamConfiguration(cbrtime)
                                          .streamName("a")
                                          .scope("a")
                                          .updating(true)
                                          .build();
        serialized = record.toBytes();
        deserialized = StreamConfigurationRecord.fromBytes(serialized);
        assertEquals(record, deserialized);
    }
}

