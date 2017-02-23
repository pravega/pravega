/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecordWithStream;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.DeleteScopeStatus;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;
import com.google.common.collect.Lists;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ZkStreamTest {

    private TestingServer zkTestServer;
    private CuratorFramework cli;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer();
        cli = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
    }

    @After
    public void stopZookeeper() throws Exception {
        cli.close();
        zkTestServer.close();
    }

    @Test
    public void testZkCreateScope() throws Exception {

        // create new scope test
        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String scopeName = "Scope1";
        CompletableFuture<CreateScopeStatus> createScopeStatus = store.createScope(scopeName);

        // createScope returns null on success, and exception on failure
        assertEquals("Create new scope :", CreateScopeStatus.SUCCESS, createScopeStatus.get());

        // create duplicate scope test
        createScopeStatus = store.createScope(scopeName);
        assertEquals("Create new scope :", CreateScopeStatus.SCOPE_EXISTS, createScopeStatus.get());

        //listStreamsInScope test
        final String streamName1 = "Stream1";
        final String streamName2 = "Stream2";
        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 5);
        StreamConfiguration streamConfig =
                StreamConfiguration.builder().scope(scopeName).streamName(streamName1).scalingPolicy(policy).build();

        StreamConfiguration streamConfig2 =
                StreamConfiguration.builder().scope(scopeName).streamName(streamName2).scalingPolicy(policy).build();

        store.createStream(scopeName, streamName1, streamConfig, System.currentTimeMillis()).get();
        store.createStream(scopeName, streamName2, streamConfig2, System.currentTimeMillis()).get();

        List<String> listOfStreams = store.listStreamsInScope(scopeName).get();
        assertEquals("Size of list", 2, listOfStreams.size());
        assertEquals("Name of stream at index zero", "Stream1", listOfStreams.get(0));
        assertEquals("Name of stream at index one", "Stream2", listOfStreams.get(1));
    }

    @Test
    public void testZkDeleteScope() throws Exception {
        // create new scope
        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String scopeName = "Scope1";
        store.createScope(scopeName).get();

        // Delete empty scope Scope1
        CompletableFuture<DeleteScopeStatus> deleteScopeStatus = store.deleteScope(scopeName);
        assertEquals("Delete Empty Scope", DeleteScopeStatus.SUCCESS, deleteScopeStatus.get());

        // Delete non-existent scope Scope2
        CompletableFuture<DeleteScopeStatus> deleteScopeStatus2 = store.deleteScope("Scope2");
        assertEquals("Delete non-existent Scope", DeleteScopeStatus.SCOPE_NOT_FOUND, deleteScopeStatus2.get());

        // Delete non-empty scope Scope3
        store.createScope("Scope3").get();
        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 5);
        final StreamConfiguration streamConfig =
                StreamConfiguration.builder().scope("Scope3").streamName("Stream3").scalingPolicy(policy).build();

        store.createStream("Scope3", "Stream3", streamConfig, System.currentTimeMillis()).get();
        CompletableFuture<DeleteScopeStatus> deleteScopeStatus3 = store.deleteScope("Scope3");
        assertEquals("Delete non-empty Scope", DeleteScopeStatus.SCOPE_NOT_EMPTY, deleteScopeStatus3.get());
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
        final String scopeName = "test";
        store.createScope(scopeName).get();

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                              .scope(streamName)
                                                              .streamName(streamName)
                                                              .scalingPolicy(policy)
                                                              .build();
        store.createStream(scopeName, streamName, streamConfig, System.currentTimeMillis()).get();

        List<Segment> segments = store.getActiveSegments(scopeName, streamName).get();
        assertEquals(segments.size(), 5);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0, 1, 2, 3, 4).contains(x.getNumber())));

        long start = segments.get(0).getStart();

        assertEquals(store.getConfiguration(scopeName, streamName).get(), streamConfig);

        List<AbstractMap.SimpleEntry<Double, Double>> newRanges;

        // existing range 0 = 0 - .2, 1 = .2 - .4, 2 = .4 - .6, 3 = .6 - .8, 4 = .8 - 1.0

        // 3, 4 -> 5 = .6 - 1.0
        newRanges = Collections.singletonList(
                new AbstractMap.SimpleEntry<>(0.6, 1.0));

        long scale1 = start + 10;
        store.scale(scopeName, streamName, Lists.newArrayList(3, 4), newRanges, scale1).get();

        segments = store.getActiveSegments(scopeName, streamName).get();
        assertEquals(segments.size(), 4);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0, 1, 2, 5).contains(x.getNumber())));

        // 1 -> 6 = 0.2 -.3, 7 = .3 - .4
        // 2,5 -> 8 = .4 - 1.0
        newRanges = Arrays.asList(
                new AbstractMap.SimpleEntry<>(0.2, 0.3),
                new AbstractMap.SimpleEntry<>(0.3, 0.4),
                new AbstractMap.SimpleEntry<>(0.4, 1.0));

        long scale2 = scale1 + 10;
        store.scale(scopeName, streamName, Lists.newArrayList(1, 2, 5), newRanges, scale2).get();

        segments = store.getActiveSegments(scopeName, streamName).get();
        assertEquals(segments.size(), 4);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0, 6, 7, 8).contains(x.getNumber())));

        // 7 -> 9 = .3 - .35, 10 = .35 - .6
        // 8 -> 10 = .35 - .6, 11 = .6 - 1.0
        newRanges = Arrays.asList(
                new AbstractMap.SimpleEntry<>(0.3, 0.35),
                new AbstractMap.SimpleEntry<>(0.35, 0.6),
                new AbstractMap.SimpleEntry<>(0.6, 1.0));

        long scale3 = scale2 + 10;
        store.scale(scopeName, streamName, Lists.newArrayList(7, 8), newRanges, scale3).get();

        segments = store.getActiveSegments(scopeName, streamName).get();
        assertEquals(segments.size(), 5);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0, 6, 9, 10, 11).contains(x.getNumber())));

        // start -1
        SegmentFutures segmentFutures = store.getActiveSegments(scopeName, streamName, start - 1).get();
        assertEquals(segmentFutures.getCurrent().size(), 5);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 1, 2, 3, 4)));

        // start + 1
        segmentFutures = store.getActiveSegments(scopeName, streamName, start + 1).get();
        assertEquals(segmentFutures.getCurrent().size(), 5);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 1, 2, 3, 4)));

        // scale1 + 1
        segmentFutures = store.getActiveSegments(scopeName, streamName, scale1 + 1).get();
        assertEquals(segmentFutures.getCurrent().size(), 4);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 1, 2, 5)));

        // scale2 + 1
        segmentFutures = store.getActiveSegments(scopeName, streamName, scale2 + 1).get();
        assertEquals(segmentFutures.getCurrent().size(), 4);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 6, 7, 8)));

        // scale3 + 1
        segmentFutures = store.getActiveSegments(scopeName, streamName, scale3 + 1).get();
        assertEquals(segmentFutures.getCurrent().size(), 5);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 6, 9, 10, 11)));

        // scale 3 + 100
        segmentFutures = store.getActiveSegments(scopeName, streamName, scale3 + 100).get();
        assertEquals(segmentFutures.getCurrent().size(), 5);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 6, 9, 10, 11)));

        assertFalse(store.isSealed(scopeName, streamName).get());
        assertNotEquals(0, store.getActiveSegments(scopeName, streamName).get().size());
        Boolean sealOperationStatus = store.setSealed(scopeName, streamName).get();
        assertTrue(sealOperationStatus);
        assertTrue(store.isSealed(scopeName, streamName).get());
        assertEquals(0, store.getActiveSegments(scopeName, streamName).get().size());

        //seal an already sealed stream.
        Boolean sealOperationStatus1 = store.setSealed(scopeName, streamName).get();
        assertTrue(sealOperationStatus1);
        assertTrue(store.isSealed(scopeName, streamName).get());
        assertEquals(0, store.getActiveSegments(scopeName, streamName).get().size());

        //seal a non existing stream.
        try {
            Boolean sealOperationStatus2 = store.setSealed(scopeName, "nonExistentStream").get();
        } catch (Exception e) {
            assertEquals(StreamNotFoundException.class, e.getCause().getClass());
        }
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
        store.createStream(scopeName, streamName, streamConfig, System.currentTimeMillis()).get();

        List<Segment> initial = store.getActiveSegments(scopeName, streamName).get();
        assertEquals(initial.size(), 6);
        assertTrue(initial.stream().allMatch(x -> Lists.newArrayList(0, 1, 2, 3, 4, 5).contains(x.getNumber())));

        long start = initial.get(0).getStart();

        assertEquals(store.getConfiguration(scopeName, streamName).get(), streamConfig);

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
                store.scale(scopeName, streamName, list, newRanges, scaleTs).get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(1000);

        List<Segment> segments = store.getActiveSegments(scopeName, streamName).get();
        assertEquals(segments.size(), 6);

    }

    @Test
    public void testTransaction() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(5);

        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "testTx";
        final String scopeName = "testTx";
        store.createScope(scopeName).get();

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                              .scope(streamName)
                                                              .streamName(streamName)
                                                              .scalingPolicy(policy)
                                                              .build();
        store.createStream(scopeName, streamName, streamConfig, System.currentTimeMillis()).get();

        UUID tx = store.createTransaction(scopeName, streamName).get();

        List<ActiveTxRecordWithStream> y = store.getAllActiveTx().get();
        ActiveTxRecordWithStream z = y.get(0);
        assert z.getTxid().equals(tx) && z.getTxRecord().getTxnStatus() == TxnStatus.OPEN;

        UUID tx2 = store.createTransaction(scopeName, streamName).get();
        y = store.getAllActiveTx().get();

        assert y.size() == 2;

        store.sealTransaction(scopeName, streamName, tx).get();
        assert store.transactionStatus(scopeName, streamName, tx).get().equals(TxnStatus.SEALED);

        CompletableFuture<TxnStatus> f1 = store.commitTransaction(scopeName, streamName, tx);
        CompletableFuture<TxnStatus> f2 = store.abortTransaction(scopeName, streamName, tx2);

        CompletableFuture.allOf(f1, f2).get();

        assert store.transactionStatus(scopeName, streamName, tx).get().equals(TxnStatus.COMMITTED);
        assert store.transactionStatus(scopeName, streamName, tx2).get().equals(TxnStatus.ABORTED);

        assert store.commitTransaction(scopeName, streamName, UUID.randomUUID())
                .handle((ok, ex) -> {
                    if (ex.getCause() instanceof TransactionNotFoundException) {
                        return true;
                    } else {
                        throw new RuntimeException("assert failed");
                    }
                }).get();

        assert store.abortTransaction(scopeName, streamName, UUID.randomUUID())
                .handle((ok, ex) -> {
                    if (ex.getCause() instanceof TransactionNotFoundException) {
                        return true;
                    } else {
                        throw new RuntimeException("assert failed");
                    }
                }).get();

        assert store.transactionStatus(scopeName, streamName, UUID.randomUUID()).get().equals(TxnStatus.UNKNOWN);
    }
}
