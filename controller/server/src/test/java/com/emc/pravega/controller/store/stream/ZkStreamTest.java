/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.TxnStatus;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ZkStreamTest {
    private static final String SCOPE = "scope";
    private TestingServer zkTestServer;
    private CuratorFramework cli;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer();
        cli = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
        PersistentStreamBase.setCreationState(State.ACTIVE);
    }

    @After
    public void stopZookeeper() throws Exception {
        cli.close();
        zkTestServer.close();
    }

    @Test
    public void testZkConnectionLoss() throws Exception {
        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 5);

        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "testfail";

        StreamConfigurationImpl streamConfig = new StreamConfigurationImpl(streamName, streamName, policy);

        CompletableFuture<Boolean> createStream = store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor);
        zkTestServer.stop();

        try {
            createStream.get();
        } catch (ExecutionException e) {
            assert e.getCause() instanceof StoreConnectionException;
        }
        zkTestServer.start();
    }

    @Test
    public void testCreateStreamState() throws Exception {
        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 5);

        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "testfail";

        StreamConfigurationImpl streamConfig = new StreamConfigurationImpl(streamName, streamName, policy);

        PersistentStreamBase.setCreationState(State.CREATING);

        store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();
        try {
            store.getConfiguration(SCOPE, streamName, null, executor).get();
        } catch (Exception e) {
            assert e.getCause() != null && e.getCause() instanceof IllegalStateException;
        }

        PersistentStreamBase.setCreationState(State.ACTIVE);
    }

    @Test
    public void testZkStream() throws Exception {
        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 5);

        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "test";

        StreamConfigurationImpl streamConfig = new StreamConfigurationImpl(streamName, streamName, policy);
        store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();

        OperationContext context = store.createContext(SCOPE, streamName);

        List<Segment> segments = store.getActiveSegments(SCOPE, streamName, context, executor).get();
        assertEquals(segments.size(), 5);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0, 1, 2, 3, 4).contains(x.getNumber())));

        long start = segments.get(0).getStart();

        assertEquals(store.getConfiguration(SCOPE, streamName, context, executor).get(), streamConfig);

        List<AbstractMap.SimpleEntry<Double, Double>> newRanges;

        // existing range 0 = 0 - .2, 1 = .2 - .4, 2 = .4 - .6, 3 = .6 - .8, 4 = .8 - 1.0

        // 3, 4 -> 5 = .6 - 1.0
        newRanges = Collections.singletonList(
                new AbstractMap.SimpleEntry<>(0.6, 1.0));

        long scale1 = start + 10;
        store.scale(SCOPE, streamName, Lists.newArrayList(3, 4), newRanges, scale1, context, executor).get();

        segments = store.getActiveSegments(SCOPE, streamName, context, executor).get();
        assertEquals(segments.size(), 4);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0, 1, 2, 5).contains(x.getNumber())));

        // 1 -> 6 = 0.2 -.3, 7 = .3 - .4
        // 2,5 -> 8 = .4 - 1.0
        newRanges = Arrays.asList(
                new AbstractMap.SimpleEntry<>(0.2, 0.3),
                new AbstractMap.SimpleEntry<>(0.3, 0.4),
                new AbstractMap.SimpleEntry<>(0.4, 1.0));

        long scale2 = scale1 + 10;
        store.scale(SCOPE, streamName, Lists.newArrayList(1, 2, 5), newRanges, scale2, context, executor).get();

        segments = store.getActiveSegments(SCOPE, streamName, context, executor).get();
        assertEquals(segments.size(), 4);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0, 6, 7, 8).contains(x.getNumber())));

        // 7 -> 9 = .3 - .35, 10 = .35 - .6
        // 8 -> 10 = .35 - .6, 11 = .6 - 1.0
        newRanges = Arrays.asList(
                new AbstractMap.SimpleEntry<>(0.3, 0.35),
                new AbstractMap.SimpleEntry<>(0.35, 0.6),
                new AbstractMap.SimpleEntry<>(0.6, 1.0));

        long scale3 = scale2 + 10;
        store.scale(SCOPE, streamName, Lists.newArrayList(7, 8), newRanges, scale3, context, executor).get();

        segments = store.getActiveSegments(SCOPE, streamName, context, executor).get();
        assertEquals(segments.size(), 5);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0, 6, 9, 10, 11).contains(x.getNumber())));

        // start -1
        SegmentFutures segmentFutures = store.getActiveSegments(SCOPE, streamName, start - 1, context, executor).get();
        assertEquals(segmentFutures.getCurrent().size(), 5);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 1, 2, 3, 4)));

        // start + 1
        segmentFutures = store.getActiveSegments(SCOPE, streamName, start + 1, context, executor).get();
        assertEquals(segmentFutures.getCurrent().size(), 5);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 1, 2, 3, 4)));

        // scale1 + 1
        segmentFutures = store.getActiveSegments(SCOPE, streamName, scale1 + 1, context, executor).get();
        assertEquals(segmentFutures.getCurrent().size(), 4);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 1, 2, 5)));

        // scale2 + 1
        segmentFutures = store.getActiveSegments(SCOPE, streamName, scale2 + 1, context, executor).get();
        assertEquals(segmentFutures.getCurrent().size(), 4);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 6, 7, 8)));

        // scale3 + 1
        segmentFutures = store.getActiveSegments(SCOPE, streamName, scale3 + 1, context, executor).get();
        assertEquals(segmentFutures.getCurrent().size(), 5);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 6, 9, 10, 11)));

        // scale 3 + 100
        segmentFutures = store.getActiveSegments(SCOPE, streamName, scale3 + 100, context, executor).get();
        assertEquals(segmentFutures.getCurrent().size(), 5);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 6, 9, 10, 11)));

        assertFalse(store.isSealed(SCOPE, streamName, context, executor).get());
        assertNotEquals(0, store.getActiveSegments(SCOPE, streamName, context, executor).get().size());
        Boolean sealOperationStatus = store.setSealed(SCOPE, streamName, context, executor).get();
        assertTrue(sealOperationStatus);
        assertTrue(store.isSealed(SCOPE, streamName, context, executor).get());
        assertEquals(0, store.getActiveSegments(SCOPE, streamName, context, executor).get().size());

        //seal an already sealed stream.
        Boolean sealOperationStatus1 = store.setSealed(SCOPE, streamName, context, executor).get();
        assertTrue(sealOperationStatus1);
        assertTrue(store.isSealed(SCOPE, streamName, context, executor).get());
        assertEquals(0, store.getActiveSegments(SCOPE, streamName, context, executor).get().size());

        //seal a non existing stream.
        try {
            store.setSealed(SCOPE, "nonExistentStream", null, executor).get();
        } catch (Exception e) {
            assertEquals(DataNotFoundException.class, e.getCause().getClass());
        }
    }

    @Ignore("run manually")
    //    @Test
    public void testZkStreamChukning() throws Exception {
        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 6);

        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "test2";

        StreamConfigurationImpl streamConfig = new StreamConfigurationImpl(streamName, streamName, policy);
        store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();

        OperationContext context = store.createContext(SCOPE, streamName);

        List<Segment> initial = store.getActiveSegments(SCOPE, streamName, context, executor).get();
        assertEquals(initial.size(), 6);
        assertTrue(initial.stream().allMatch(x -> Lists.newArrayList(0, 1, 2, 3, 4, 5).contains(x.getNumber())));

        long start = initial.get(0).getStart();

        assertEquals(store.getConfiguration(SCOPE, streamName, context, executor).get(), streamConfig);

        IntStream.range(0, SegmentRecord.SEGMENT_CHUNK_SIZE + 2).forEach(x -> {
            List<AbstractMap.SimpleEntry<Double, Double>> newRanges = Arrays.asList(
                    new AbstractMap.SimpleEntry<>(0.0, 0.2),
                    new AbstractMap.SimpleEntry<>(0.2, 0.4),
                    new AbstractMap.SimpleEntry<>(0.4, 0.6),
                    new AbstractMap.SimpleEntry<>(0.6, 0.8),
                    new AbstractMap.SimpleEntry<>(0.8, 0.9),
                    new AbstractMap.SimpleEntry<>(0.9, 1.0));

            long scaleTs = start + 10 * (x + 1);

            try {

                List<Integer> list = IntStream.range(x * 6, (x + 1) * 6).boxed().collect(Collectors.toList());
                store.scale(SCOPE, streamName, list, newRanges, scaleTs, context, executor).get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(1000);

        List<Segment> segments = store.getActiveSegments(SCOPE, streamName, context, executor).get();
        assertEquals(segments.size(), 6);

    }

    @Test
    public void testTransaction() throws Exception {
        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 5);

        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "testTx";

        StreamConfigurationImpl streamConfig = new StreamConfigurationImpl(streamName, streamName, policy);
        store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();

        OperationContext context = store.createContext(SCOPE, streamName);

        UUID tx = store.createTransaction(SCOPE, streamName, context, executor).get();

        UUID tx2 = store.createTransaction(SCOPE, streamName, context, executor).get();

        store.sealTransaction(SCOPE, streamName, tx, context, executor).get();
        assert store.transactionStatus(SCOPE, streamName, tx, context, executor).get().equals(TxnStatus.SEALED);

        CompletableFuture<TxnStatus> f1 = store.commitTransaction(SCOPE, streamName, tx, null, executor);
        CompletableFuture<TxnStatus> f2 = store.abortTransaction(SCOPE, streamName, tx2, null, executor);

        CompletableFuture.allOf(f1, f2).get();

        assert store.transactionStatus(SCOPE, streamName, tx, context, executor).get().equals(TxnStatus.COMMITTED);
        assert store.transactionStatus(SCOPE, streamName, tx2, context, executor).get().equals(TxnStatus.ABORTED);

        assert store.commitTransaction(SCOPE, streamName, UUID.randomUUID(), context, executor)
                .handle((ok, ex) -> {
                    if (ex.getCause() instanceof TransactionNotFoundException) {
                        return true;
                    } else {
                        throw new RuntimeException("assert failed");
                    }
                }).get();

        assert store.abortTransaction(SCOPE, streamName, UUID.randomUUID(), null, executor)
                .handle((ok, ex) -> {
                    if (ex.getCause() instanceof TransactionNotFoundException) {
                        return true;
                    } else {
                        throw new RuntimeException("assert failed");
                    }
                }).get();

        assert store.transactionStatus(SCOPE, streamName, UUID.randomUUID(), context, executor).get().equals(TxnStatus.UNKNOWN);

        store.blockTransactions(SCOPE, streamName, null, executor).get();
        store.createTransaction(SCOPE, streamName, null, executor)
                .handle((r, ex) -> {
                    assert ex != null && ex.getCause() instanceof TransactionBlockedException;
                    return null;
                }).get();

        store.setMarker(SCOPE, streamName, 0, 1L, null, executor).get();

        assert store.getMarker(SCOPE, streamName, 0, null, executor).get().isPresent();

        store.removeMarker(SCOPE, streamName, 0, null, executor).get();

        assert !store.getMarker(SCOPE, streamName, 0, null, executor).get().isPresent();

    }
}
