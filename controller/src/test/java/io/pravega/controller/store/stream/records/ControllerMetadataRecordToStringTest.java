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
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.TxnStatus;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class ControllerMetadataRecordToStringTest {

    @Test
    public void txnRecordTest() {
        ActiveTxnRecord record = new ActiveTxnRecord(1L, 1L, 1L, TxnStatus.OPEN);
        assertTrue(record.toString().contains("txCreationTimestamp"));
        assertTrue(record.toString().contains("leaseExpiryTime"));
        assertTrue(record.toString().contains("maxExecutionExpiryTime"));
        assertTrue(record.toString().contains("txnStatus"));
        assertTrue(record.toString().contains("commitOffsets"));

        record = new ActiveTxnRecord(1L, 1L, 1L,
                TxnStatus.OPEN, "a", 1L, 1L, ImmutableMap.of(1L, 2L, 3L, 4L));
        assertTrue(record.toString().contains("txCreationTimestamp"));
        assertTrue(record.toString().contains("leaseExpiryTime"));
        assertTrue(record.toString().contains("maxExecutionExpiryTime"));
        assertTrue(record.toString().contains("txnStatus"));
        assertTrue(record.toString().contains("writerId"));
        assertTrue(record.toString().contains("commitTime"));
        assertTrue(record.toString().contains("commitOrder"));
        assertTrue(record.toString().contains("commitOffsets"));

        CompletedTxnRecord record2 = new CompletedTxnRecord(1L, TxnStatus.COMMITTED);
        assertTrue(record2.toString().contains("completeTime"));
        assertTrue(record2.toString().contains("completionStatus"));
    }

    @Test
    public void committingTransactionsRecordTest() {
        List<UUID> list = Lists.newArrayList(UUID.randomUUID(), UUID.randomUUID());
        CommittingTransactionsRecord commitTransactionsRecord =
                new CommittingTransactionsRecord(0, ImmutableList.copyOf(list));
        System.out.println(commitTransactionsRecord.toString());
        assertTrue(commitTransactionsRecord.toString().contains("epoch"));
        assertTrue(commitTransactionsRecord.toString().contains("transactionsToCommit"));

        CommittingTransactionsRecord recordWithActiveEpoch = commitTransactionsRecord.createRollingTxnRecord(10);
        System.out.println(recordWithActiveEpoch.toString());
        assertTrue(recordWithActiveEpoch.toString().contains("epoch"));
        assertTrue(recordWithActiveEpoch.toString().contains("transactionsToCommit"));
        assertTrue(recordWithActiveEpoch.toString().contains("activeEpoch"));
    }

    @Test
    public void epochRecordTest() {
        List<StreamSegmentRecord> list = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(1, 0, 10L, 0.0, 1.0));
        EpochRecord record = new EpochRecord(10, 0, ImmutableList.copyOf(list), 10L,
                0L, 0L);
        assertTrue(record.toString().contains("epoch"));
        assertTrue(record.toString().contains("referenceEpoch"));
        assertTrue(record.toString().contains("segments"));
        assertTrue(record.toString().contains("creationTime"));
        assertTrue(record.toString().contains("splits"));
        assertTrue(record.toString().contains("merges"));
    }

    @Test
    public void epochTransitionRecordTest() {
        ImmutableSet<Long> segmentsToSeal = ImmutableSet.of(1L, 2L);
        ImmutableMap<Long, Map.Entry<Double, Double>> newSegmentsWithRange =
                ImmutableMap.of(3L, Map.entry(0.1, 0.2), 4L, Map.entry(0.3, 0.4));
        EpochTransitionRecord record = new EpochTransitionRecord(10, 100L,
                segmentsToSeal, newSegmentsWithRange);
        System.out.println(record.toString());
        assertTrue(record.toString().contains("activeEpoch"));
        assertTrue(record.toString().contains("time"));
        assertTrue(record.toString().contains("segmentsToSeal"));
        assertTrue(record.toString().contains("newSegmentsWithRange"));
    }

    @Test
    public void historyTimeSeriesTest() {
        List<StreamSegmentRecord> sealedSegments = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0));
        List<StreamSegmentRecord> newSegments = Lists.newArrayList(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0),
                StreamSegmentRecord.newSegmentRecord(1, 1, 0L, 0.1, 0.2));

        HistoryTimeSeriesRecord node = new HistoryTimeSeriesRecord(0, 0, ImmutableList.copyOf(sealedSegments),
                ImmutableList.copyOf(newSegments), 0L);
        HistoryTimeSeriesRecord node2 = new HistoryTimeSeriesRecord(1, 0, 1L);
        HistoryTimeSeriesRecord node3 = new HistoryTimeSeriesRecord(4, 4,
                ImmutableList.copyOf(sealedSegments), ImmutableList.copyOf(newSegments), 1L);

        HistoryTimeSeries timeSeries = new HistoryTimeSeries(ImmutableList.of(node, node2, node3));
        assertTrue(timeSeries.toString().contains("historyRecords"));
        assertTrue(timeSeries.toString().contains("epoch"));
        assertTrue(timeSeries.toString().contains("referenceEpoch"));
        assertTrue(timeSeries.toString().contains("segmentsSealed"));
        assertTrue(timeSeries.toString().contains("segmentsCreated"));
        assertTrue(timeSeries.toString().contains("scaleTime"));
    }

    @Test
    public void retentionSetTest() {
        StreamCutReferenceRecord record = StreamCutReferenceRecord.builder().recordingSize(0L).recordingTime(10L).build();
        StreamCutReferenceRecord record2 = StreamCutReferenceRecord.builder().recordingSize(1L).recordingTime(11L).build();
        RetentionSet set = new RetentionSet(ImmutableList.of(record, record2));
        assertTrue(set.toString().contains("retentionRecords"));
        assertTrue(set.toString().contains("recordingTime"));
        assertTrue(set.toString().contains("recordingSize"));
    }

    @Test
    public void sealedSegmentsMapShardTest() {
        Map<Long, Long> map = new HashMap<>();
        map.put(0L, 0L);
        map.put(1L, 0L);
        map.put(2L, 0L);
        SealedSegmentsMapShard record = SealedSegmentsMapShard.builder().sealedSegmentsSizeMap(map).build();
        assertTrue(record.toString().contains("shardNumber"));
        assertTrue(record.toString().contains("sealedSegmentsSizeMap"));
    }

    @Test
    public void stateRecordTest() {
        StateRecord record = new StateRecord(State.ACTIVE);
        assertTrue(record.toString().contains("state"));
    }

    @Test
    public void streamConfigurationRecordTest() {
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
        assertTrue(record1.toString().contains("scope"));
        assertTrue(record1.toString().contains("streamName"));
        assertTrue(record1.toString().contains("streamConfiguration"));
        assertTrue(record1.toString().contains("scalingPolicy"));
        assertTrue(record1.toString().contains("retentionPolicy"));
        assertTrue(record1.toString().contains("timestampAggregationTimeout"));
        assertTrue(record1.toString().contains("tags"));
        assertTrue(record1.toString().contains("rolloverSizeBytes"));
        assertTrue(record1.toString().contains("updating"));
        assertTrue(record1.toString().contains("tagOnlyUpdate"));
        assertTrue(record1.toString().contains("removeTags"));

        StreamConfiguration withScalingOnly = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        StreamConfigurationRecord record2 = StreamConfigurationRecord.builder()
                .streamConfiguration(withScalingOnly)
                .streamName("a")
                .scope("a")
                .updating(true)
                .build();
        assertTrue(record2.toString().contains("retentionPolicy = null"));

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
        assertTrue(record3.toString().contains("scalingPolicy = null"));
    }

    @Test
    public void streamCutRecordTest() {
        StreamCutRecord record = new StreamCutRecord(10L, 10L, ImmutableMap.of(1L, 2L, 3L, 4L));
        assertTrue(record.toString().contains("recordingTime"));
        assertTrue(record.toString().contains("recordingSize"));
        assertTrue(record.toString().contains("streamCut"));
    }

    @Test
    public void streamCutReferenceRecordTest() {
        StreamCutReferenceRecord record = StreamCutReferenceRecord.builder().recordingSize(0L).recordingTime(10L).build();
        assertTrue(record.toString().contains("recordingTime"));
        assertTrue(record.toString().contains("recordingSize"));
    }

    @Test
    public void streamSegmentRecordTest() {
        StreamSegmentRecord record = StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0);
        assertTrue(record.toString().contains("segmentNumber"));
        assertTrue(record.toString().contains("creationEpoch"));
        assertTrue(record.toString().contains("creationTime"));
        assertTrue(record.toString().contains("keyStart"));
        assertTrue(record.toString().contains("keyEnd"));
    }

    @Test
    public void streamSubscriberTest() {
        StreamSubscriber record = new StreamSubscriber("a", 1L,
                ImmutableMap.of(1L, 2L, 3L, 4L), 10L);
        assertTrue(record.toString().contains("subscriber"));
        assertTrue(record.toString().contains("generation"));
        assertTrue(record.toString().contains("truncationStreamCut"));
        assertTrue(record.toString().contains("updateTime"));
    }

    @Test
    public void streamTruncationRecord() {
        Map<StreamSegmentRecord, Integer> span = new HashMap<>();
        span.put(StreamSegmentRecord.newSegmentRecord(0, 0, 0L, 0.0, 1.0), 0);
        span.put(StreamSegmentRecord.newSegmentRecord(1, 0, 0L, 0.0, 1.0), 0);
        Map<Long, Long> streamCut = new HashMap<>();
        streamCut.put(0L, 0L);
        Set<Long> set = new HashSet<>();
        set.add(0L);
        StreamTruncationRecord record = new StreamTruncationRecord(ImmutableMap.copyOf(streamCut),
                ImmutableMap.copyOf(span), ImmutableSet.copyOf(set), ImmutableSet.copyOf(set), 0L, true);
        assertTrue(record.toString().contains("streamCut"));
        assertTrue(record.toString().contains("span"));
        assertTrue(record.toString().contains("deletedSegments"));
        assertTrue(record.toString().contains("toDelete"));
        assertTrue(record.toString().contains("sizeTill"));
        assertTrue(record.toString().contains("updating"));
    }

    @Test
    public void subscribersTest() {
        Subscribers record = new Subscribers(ImmutableSet.of("a", "b"));
        assertTrue(record.toString().contains("subscribers"));
    }
}
