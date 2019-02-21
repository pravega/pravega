/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Zookeeper based stream metadata store tests.
 */
public class PravegaTablesStreamMetadataStoreTest extends StreamMetadataStoreTest {

    private TestingServer zkServer;
    private CuratorFramework cli;

    @Override
    public void setupStore() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
        SegmentHelper segmentHelperMockForTables = SegmentHelperMock.getSegmentHelperMockForTables();
        store = new PravegaTablesStreamMetadataStore(segmentHelperMockForTables, cli, executor, Duration.ofSeconds(1));
        bucketStore = StreamStoreFactory.createZKBucketStore(1, cli, executor);
    }

    @Override
    public void cleanupStore() throws Exception {
        store.close();
        cli.close();
        zkServer.close();
    }
    
    @Test
    public void testInvalidOperation() throws Exception {
        // Test operation when stream is not in active state
        store.createScope(scope).get();
        store.createStream(scope, stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream1, State.CREATING, null, executor).get();

        AssertExtensions.assertFutureThrows("Should throw IllegalStateException",
                store.getActiveSegments(scope, stream1, null, executor),
                (Throwable t) -> t instanceof StoreException.IllegalStateException);
    }
    
    @Test
    public void testScaleMetadata() throws Exception {
        String scope = "testScopeScale";
        String stream = "testStreamScale1";
        ScalingPolicy policy = ScalingPolicy.fixed(3);
        StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();
        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.5, 1.0);
        List<Map.Entry<Double, Double>> newRanges = Arrays.asList(segment1, segment2);

        store.createScope(scope).get();
        store.createStream(scope, stream, configuration, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        List<ScaleMetadata> scaleIncidents = store.getScaleMetadata(scope, stream, 0, Long.MAX_VALUE, null, executor).get();
        assertTrue(scaleIncidents.size() == 1);
        assertTrue(scaleIncidents.get(0).getSegments().size() == 3);
        // scale
        scale(scope, stream, scaleIncidents.get(0).getSegments(), newRanges);
        scaleIncidents = store.getScaleMetadata(scope, stream, 0, Long.MAX_VALUE, null, executor).get();
        assertTrue(scaleIncidents.size() == 2);
        assertTrue(scaleIncidents.get(0).getSegments().size() == 3);
        assertTrue(scaleIncidents.get(1).getSegments().size() == 2);

        // scale again
        scale(scope, stream, scaleIncidents.get(1).getSegments(), newRanges);
        scaleIncidents = store.getScaleMetadata(scope, stream, 0, Long.MAX_VALUE, null, executor).get();
        assertTrue(scaleIncidents.size() == 3);
        assertTrue(scaleIncidents.get(0).getSegments().size() == 3);
        assertTrue(scaleIncidents.get(1).getSegments().size() == 2);
        assertTrue(scaleIncidents.get(2).getSegments().size() == 2);

        // scale again
        scale(scope, stream, scaleIncidents.get(2).getSegments(), newRanges);
        scaleIncidents = store.getScaleMetadata(scope, stream, 0, Long.MAX_VALUE, null, executor).get();
        assertTrue(scaleIncidents.size() == 4);
        assertTrue(scaleIncidents.get(0).getSegments().size() == 3);
        assertTrue(scaleIncidents.get(1).getSegments().size() == 2);
        assertTrue(scaleIncidents.get(2).getSegments().size() == 2);
        assertTrue(scaleIncidents.get(3).getSegments().size() == 2);
    }

    @Test
    public void testSplitsMerges() throws Exception {
        String scope = "testScopeScale";
        String stream = "testStreamScale";
        ScalingPolicy policy = ScalingPolicy.fixed(2);
        StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        store.createScope(scope).get();
        store.createStream(scope, stream, configuration, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        // Case: Initial state, splits = 0, merges = 0
        // time t0, total segments 2, S0 {0.0 - 0.5} S1 {0.5 - 1.0}
        List<ScaleMetadata> scaleRecords = store.getScaleMetadata(scope, stream, 0, Long.MAX_VALUE, null, executor).get();
        assertTrue(scaleRecords.size() == 1);
        assertTrue(scaleRecords.get(0).getSegments().size() == 2);
        assertTrue(scaleRecords.get(0).getSplits() == 0L);
        assertTrue(scaleRecords.get(0).getMerges() == 0L);

        SimpleEntry<Long, Long> simpleEntrySplitsMerges = findSplitsAndMerges(scope, stream);

        assertEquals("Number of splits ", new Long(0), simpleEntrySplitsMerges.getKey());
        assertEquals("Number of merges", new Long(0), simpleEntrySplitsMerges.getValue());

        // Case: Only splits, S0 split into S2, S3, S4 and S1 split into S5, S6,
        //  total splits = 2, total merges = 3
        // time t1, total segments 5, S2 {0.0, 0.2}, S3 {0.2, 0.4}, S4 {0.4, 0.5}, S5 {0.5, 0.7}, S6 {0.7, 1.0}
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.0, 0.2);
        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.2, 0.4);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.4, 0.5);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.5, 0.7);
        SimpleEntry<Double, Double> segment6 = new SimpleEntry<>(0.7, 1.0);
        List<Map.Entry<Double, Double>> newRanges1 = Arrays.asList(segment2, segment3, segment4, segment5, segment6);
        scale(scope, stream, scaleRecords.get(0).getSegments(), newRanges1);
        scaleRecords = store.getScaleMetadata(scope, stream, 0, Long.MAX_VALUE, null, executor).get();
        assertEquals(scaleRecords.size(), 2);
        assertEquals(scaleRecords.get(1).getSegments().size(), 5);
        assertEquals(scaleRecords.get(1).getSplits(), 2L);
        assertEquals(scaleRecords.get(1).getMerges(), 0L);
        assertEquals(scaleRecords.size(), 2);
        SimpleEntry<Long, Long> simpleEntrySplitsMerges1 = findSplitsAndMerges(scope, stream);
        assertEquals("Number of splits ", new Long(2), simpleEntrySplitsMerges1.getKey());
        assertEquals("Number of merges", new Long(0), simpleEntrySplitsMerges1.getValue());

        // Case: Splits and merges both, S2 and S3 merged to S7,  S4 and S5 merged to S8,  S6 split to S9 and S10
        // total splits = 3, total merges = 2
        // time t2, total segments 4, S7 {0.0, 0.4}, S8 {0.4, 0.7}, S9 {0.7, 0.8}, S10 {0.8, 1.0}
        SimpleEntry<Double, Double> segment7 = new SimpleEntry<>(0.0, 0.4);
        SimpleEntry<Double, Double> segment8 = new SimpleEntry<>(0.4, 0.7);
        SimpleEntry<Double, Double> segment9 = new SimpleEntry<>(0.7, 0.8);
        SimpleEntry<Double, Double> segment10 = new SimpleEntry<>(0.8, 1.0);
        List<Map.Entry<Double, Double>> newRanges2 = Arrays.asList(segment7, segment8, segment9, segment10);
        scale(scope, stream, scaleRecords.get(1).getSegments(), newRanges2);
        scaleRecords = store.getScaleMetadata(scope, stream, 0, Long.MAX_VALUE, null, executor).get();
        assertEquals(scaleRecords.size(), 3);
        assertEquals(scaleRecords.get(2).getSegments().size(), 4);
        assertEquals(scaleRecords.get(2).getSplits(), 1L);
        assertEquals(scaleRecords.get(2).getMerges(), 2L);

        SimpleEntry<Long, Long> simpleEntrySplitsMerges2 = findSplitsAndMerges(scope, stream);
        assertEquals("Number of splits ", new Long(3), simpleEntrySplitsMerges2.getKey());
        assertEquals("Number of merges", new Long(2), simpleEntrySplitsMerges2.getValue());

        // Case: Only merges , S7 and S8 merged to S11,  S9 and S10 merged to S12
        // total splits = 3, total merges = 4
        // time t3, total segments 2, S11 {0.0, 0.7}, S12 {0.7, 1.0}
        SimpleEntry<Double, Double> segment11 = new SimpleEntry<>(0.0, 0.7);
        SimpleEntry<Double, Double> segment12 = new SimpleEntry<>(0.7, 1.0);
        List<Map.Entry<Double, Double>> newRanges3 = Arrays.asList(segment11, segment12);
        scale(scope, stream, scaleRecords.get(2).getSegments(), newRanges3);
        scaleRecords = store.getScaleMetadata(scope, stream, 0, Long.MAX_VALUE, null, executor).get();
        assertEquals(scaleRecords.size(), 4);
        assertEquals(scaleRecords.get(3).getSegments().size(), 2);
        assertEquals(scaleRecords.get(3).getSplits(), 0L);
        assertEquals(scaleRecords.get(3).getMerges(), 2L);

        SimpleEntry<Long, Long> simpleEntrySplitsMerges3 = findSplitsAndMerges(scope, stream);
        assertEquals("Number of splits ", new Long(3), simpleEntrySplitsMerges3.getKey());
        assertEquals("Number of merges", new Long(4), simpleEntrySplitsMerges3.getValue());
    }
    
    private SimpleEntry<Long, Long> findSplitsAndMerges(String scope, String stream) throws InterruptedException, java.util.concurrent.ExecutionException {
        return store.getScaleMetadata(scope, stream, 0, Long.MAX_VALUE, null, executor).get()
                .stream().reduce(new SimpleEntry<>(0L, 0L),
                        (x, y) -> new SimpleEntry<>(x.getKey() + y.getSplits(), x.getValue() + y.getMerges()),
                        (x, y) -> new SimpleEntry<>(x.getKey() + y.getKey(), x.getValue() + y.getValue()));
    }

    private void scale(String scope, String stream, List<Segment> segments, List<Map.Entry<Double, Double>> newRanges) {

        long scaleTimestamp = System.currentTimeMillis();
        List<Long> existingSegments = segments.stream().map(Segment::segmentId).collect(Collectors.toList());
        VersionedMetadata<EpochTransitionRecord> versioned = store.submitScale(scope, stream, existingSegments, newRanges,
                scaleTimestamp, null, null, executor).join();
        VersionedMetadata<State> state = store.getVersionedState(scope, stream, null, executor).join();
        state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).join();
        store.startScale(scope, stream, false, versioned, state, null, executor).join();
        store.scaleCreateNewEpochs(scope, stream, versioned, null, executor).join();
        store.scaleSegmentsSealed(scope, stream, existingSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned,
                null, executor).join();
        store.completeScale(scope, stream, versioned, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();
    }
}
