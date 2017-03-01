/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
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
import java.util.Optional;
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
import static org.mockito.Mockito.spy;

public class ZkStreamTest {
    private static final String SCOPE = "scope";
    private TestingServer zkTestServer;
    private CuratorFramework cli;
    private StreamMetadataStore storePartialMock;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer();
        cli = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(2000));
        cli.start();

        storePartialMock = spy(new ZKStreamMetadataStore(cli, executor));
    }

    @After
    public void stopZookeeper() throws Exception {
        cli.close();
        zkTestServer.close();
    }

    @Test
    public void testZkConnectionLoss() throws Exception {
        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 5);

        final String streamName = "testfail";

        final StreamConfiguration streamConfig = StreamConfiguration.builder().scope(streamName).streamName(streamName).scalingPolicy(policy).build();

        zkTestServer.stop();

        try {
            storePartialMock.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();
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

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scope(streamName)
                .streamName(streamName)
                .scalingPolicy(policy)
                .build();

        store.createScope(SCOPE).get();
        store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();

        try {
            store.getConfiguration(SCOPE, streamName, null, executor).get();
        } catch (Exception e) {
            assert e.getCause() != null && e.getCause() instanceof IllegalStateException;
        }
        store.deleteScope(SCOPE);
    }

    @Test
    public void testZkCreateScope() throws Exception {

        // create new scope test
        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String scopeName = "Scope1";
        CompletableFuture<CreateScopeStatus> createScopeStatus = store.createScope(scopeName);

        // createScope returns null on success, and exception on failure
        assertEquals("Create new scope :", CreateScopeStatus.Status.SUCCESS, createScopeStatus.get().getStatus());

        // create duplicate scope test
        createScopeStatus = store.createScope(scopeName);
        assertEquals("Create new scope :", CreateScopeStatus.Status.SCOPE_EXISTS, createScopeStatus.get().getStatus());

        //listStreamsInScope test
        final String streamName1 = "Stream1";
        final String streamName2 = "Stream2";
        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 5);
        StreamConfiguration streamConfig =
                StreamConfiguration.builder().scope(scopeName).streamName(streamName1).scalingPolicy(policy).build();

        StreamConfiguration streamConfig2 =
                StreamConfiguration.builder().scope(scopeName).streamName(streamName2).scalingPolicy(policy).build();

        store.createStream(scopeName, streamName1, streamConfig, System.currentTimeMillis(), null, executor).get();
        store.setState(scopeName, streamName1, State.ACTIVE, null, executor).get();
        store.createStream(scopeName, streamName2, streamConfig2, System.currentTimeMillis(), null, executor).get();
        store.setState(scopeName, streamName2, State.ACTIVE, null, executor).get();

        List<StreamConfiguration> listOfStreams = store.listStreamsInScope(scopeName).get();
        assertEquals("Size of list", 2, listOfStreams.size());
        assertEquals("Name of stream at index zero", "Stream1", listOfStreams.get(0).getStreamName());
        assertEquals("Name of stream at index one", "Stream2", listOfStreams.get(1).getStreamName());
    }

    @Test
    public void testZkDeleteScope() throws Exception {
        // create new scope
        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String scopeName = "Scope1";
        store.createScope(scopeName).get();

        // Delete empty scope Scope1
        CompletableFuture<DeleteScopeStatus> deleteScopeStatus = store.deleteScope(scopeName);
        assertEquals("Delete Empty Scope", DeleteScopeStatus.Status.SUCCESS, deleteScopeStatus.get().getStatus());

        // Delete non-existent scope Scope2
        CompletableFuture<DeleteScopeStatus> deleteScopeStatus2 = store.deleteScope("Scope2");
        assertEquals("Delete non-existent Scope", DeleteScopeStatus.Status.SCOPE_NOT_FOUND, deleteScopeStatus2.get().getStatus());

        // Delete non-empty scope Scope3
        store.createScope("Scope3").get();
        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 5);
        final StreamConfiguration streamConfig =
                StreamConfiguration.builder().scope("Scope3").streamName("Stream3").scalingPolicy(policy).build();

        store.createStream("Scope3", "Stream3", streamConfig, System.currentTimeMillis(), null, executor).get();
        store.setState("Scope3", "Stream3", State.ACTIVE, null, executor).get();

        CompletableFuture<DeleteScopeStatus> deleteScopeStatus3 = store.deleteScope("Scope3");
        assertEquals("Delete non-empty Scope", DeleteScopeStatus.Status.SCOPE_NOT_EMPTY,
                     deleteScopeStatus3.get().getStatus());
    }

    @Test
    public void testZkListScope() throws Exception {
        // list scope test
        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        store.createScope("Scope1").get();
        store.createScope("Scope2").get();
        store.createScope("Scope3").get();

        List<String> listScopes = store.listScopes().get();
        assertEquals("List Scopes ", 3, listScopes.size());

        store.deleteScope("Scope3").get();
        listScopes = store.listScopes().get();
        assertEquals("List Scopes ", 2, listScopes.size());
    }

    @Test
    public void testZkStream() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(5);

        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "test";
        store.createScope(SCOPE).get();

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scope(streamName)
                .streamName(streamName)
                .scalingPolicy(policy)
                .build();

        store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();
        store.setState(SCOPE, streamName, State.ACTIVE, null, executor).get();
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

        store.markCold(SCOPE, streamName, 0, System.currentTimeMillis() + 1000, null, executor).get();
        assertTrue(store.isCold(SCOPE, streamName, 0, null, executor).get());
        Thread.sleep(1000);
        assertFalse(store.isCold(SCOPE, streamName, 0, null, executor).get());

        store.markCold(SCOPE, streamName, 0, System.currentTimeMillis() + 1000, null, executor).get();
        store.removeMarker(SCOPE, streamName, 0, null, executor).get();

        assertFalse(store.isCold(SCOPE, streamName, 0, null, executor).get());
    }

    @Ignore("run manually")
    //    @Test
    public void testZkStreamChunking() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(6);

        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "test2";
        final String scopeName = "test2";
        store.createScope(scopeName);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scope(streamName)
                .streamName(streamName)
                .scalingPolicy(policy)
                .build();

        store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();
        store.setState(SCOPE, streamName, State.ACTIVE, null, executor).get();
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
        final ScalingPolicy policy = ScalingPolicy.fixed(5);

        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "testTx";
        store.createScope(SCOPE).get();

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(streamName)
                .scalingPolicy(policy)
                .build();

        store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();
        store.setState(SCOPE, streamName, State.ACTIVE, null, executor).get();

        OperationContext context = store.createContext(ZkStreamTest.SCOPE, streamName);

        VersionedTransactionData tx = store.createTransaction(SCOPE, streamName, 10000, 600000, 30000,
                context, executor).get();

        VersionedTransactionData tx2 = store.createTransaction(SCOPE, streamName, 10000, 600000, 30000,
                context, executor).get();

        store.sealTransaction(SCOPE, streamName, tx.getId(), true, Optional.<Integer>empty(),
                context, executor).get();
        assert store.transactionStatus(SCOPE, streamName, tx.getId(), context, executor)
                .get().equals(TxnStatus.COMMITTING);

        CompletableFuture<TxnStatus> f1 = store.commitTransaction(SCOPE, streamName, tx.getId(), context, executor);

        store.sealTransaction(SCOPE, streamName, tx2.getId(), false, Optional.<Integer>empty(),
                context, executor).get();
        assert store.transactionStatus(SCOPE, streamName, tx2.getId(), context, executor)
                .get().equals(TxnStatus.ABORTING);

        CompletableFuture<TxnStatus> f2 = store.abortTransaction(SCOPE, streamName, tx2.getId(), context, executor);

        CompletableFuture.allOf(f1, f2).get();

        assert store.transactionStatus(SCOPE, streamName, tx.getId(), context, executor)
                .get().equals(TxnStatus.COMMITTED);
        assert store.transactionStatus(SCOPE, streamName, tx2.getId(), context, executor)
                .get().equals(TxnStatus.ABORTED);

        assert store.commitTransaction(ZkStreamTest.SCOPE, streamName, UUID.randomUUID(), null, executor)
                .handle((ok, ex) -> {
                    if (ex.getCause() instanceof TransactionNotFoundException) {
                        return true;
                    } else {
                        throw new RuntimeException("assert failed");
                    }
                }).get();

        assert store.abortTransaction(ZkStreamTest.SCOPE, streamName, UUID.randomUUID(), null, executor)
                .handle((ok, ex) -> {
                    if (ex.getCause() instanceof TransactionNotFoundException) {
                        return true;
                    } else {
                        throw new RuntimeException("assert failed");
                    }
                }).get();

        assert store.transactionStatus(ZkStreamTest.SCOPE, streamName, UUID.randomUUID(), context, executor)
                .get().equals(TxnStatus.UNKNOWN);
    }
}
