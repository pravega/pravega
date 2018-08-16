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

import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.Int96;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.test.common.AssertExtensions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.pravega.controller.store.stream.ZKStreamMetadataStore.COUNTER_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Zookeeper based stream metadata store tests.
 */
public class ZKStreamMetadataStoreTest extends StreamMetadataStoreTest {

    private TestingServer zkServer;
    private CuratorFramework cli;

    @Override
    public void setupTaskStore() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
        store = new ZKStreamMetadataStore(cli, 1, executor, Duration.ofSeconds(1));
    }

    @Override
    public void cleanupTaskStore() throws IOException {
        cli.close();
        zkServer.close();
    }

    @Test
    public void testCounter() throws Exception {
        ZKStoreHelper storeHelper = spy(new ZKStoreHelper(cli, executor));
        storeHelper.createZNodeIfNotExist("/store/scope").join();

        ZKStreamMetadataStore zkStore = spy((ZKStreamMetadataStore) this.store);
        zkStore.setStoreHelperForTesting(storeHelper);

        // first call should get the new range from store
        Int96 counter = zkStore.getNextCounter().join();

        // verify that the generated counter is from new range
        assertEquals(0, counter.getMsb());
        assertEquals(1L, counter.getLsb());
        assertEquals(zkStore.getCounterForTesting(), counter);
        Int96 limit = zkStore.getLimitForTesting();
        assertEquals(ZKStreamMetadataStore.COUNTER_RANGE, limit.getLsb());

        // update the local counter to the end of the current range (limit - 1)
        zkStore.setCounterAndLimitForTesting(limit.getMsb(), limit.getLsb() - 1, limit.getMsb(), limit.getLsb());
        // now call three getNextCounters concurrently.. first one to execute should increment the counter to limit.
        // other two will result in refresh being called.
        CompletableFuture<Int96> future1 = zkStore.getNextCounter();
        CompletableFuture<Int96> future2 = zkStore.getNextCounter();
        CompletableFuture<Int96> future3 = zkStore.getNextCounter();

        List<Int96> values = Futures.allOfWithResults(Arrays.asList(future1, future2, future3)).join();

        // second and third should result in refresh being called. Verify method call count is 3, twice for now and
        // once for first time when counter is set
        verify(zkStore, times(3)).refreshRangeIfNeeded();

        verify(zkStore, times(2)).getRefreshFuture();

        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(limit.getMsb(), limit.getLsb())) == 0));
        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(0, limit.getLsb() + 1)) == 0));
        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(0, limit.getLsb() + 2)) == 0));

        // verify that counter and limits are increased
        Int96 newCounter = zkStore.getCounterForTesting();
        Int96 newLimit = zkStore.getLimitForTesting();
        assertEquals(ZKStreamMetadataStore.COUNTER_RANGE * 2, newLimit.getLsb());
        assertEquals(0, newLimit.getMsb());
        assertEquals(ZKStreamMetadataStore.COUNTER_RANGE + 2, newCounter.getLsb());
        assertEquals(0, newCounter.getMsb());

        // set range in store to have lsb = Long.Max - 100
        Data<Integer> data = new Data<>(new Int96(0, Long.MAX_VALUE - 100).toBytes(), null);
        doReturn(CompletableFuture.completedFuture(data)).when(storeHelper).getData(COUNTER_PATH);
        // set local limit to {msb, Long.Max - 100}
        zkStore.setCounterAndLimitForTesting(0, Long.MAX_VALUE - 100, 0, Long.MAX_VALUE - 100);
        // now the call to getNextCounter should result in another refresh
        zkStore.getNextCounter().join();
        // verify that post refresh counter and limit have different msb
        Int96 newCounter2 = zkStore.getCounterForTesting();
        Int96 newLimit2 = zkStore.getLimitForTesting();

        assertEquals(1, newLimit2.getMsb());
        assertEquals(ZKStreamMetadataStore.COUNTER_RANGE - 100, newLimit2.getLsb());
        assertEquals(0, newCounter2.getMsb());
        assertEquals(Long.MAX_VALUE - 99, newCounter2.getLsb());
    }

    @Test
    public void testCounterConcurrentUpdates() {
        ZKStoreHelper storeHelper = spy(new ZKStoreHelper(cli, executor));
        storeHelper.createZNodeIfNotExist("/store/scope").join();

        ZKStreamMetadataStore zkStore = spy((ZKStreamMetadataStore) this.store);
        ZKStreamMetadataStore zkStore2 = spy((ZKStreamMetadataStore) this.store);
        ZKStreamMetadataStore zkStore3 = spy((ZKStreamMetadataStore) this.store);
        zkStore.setStoreHelperForTesting(storeHelper);

        // first call should get the new range from store
        Int96 counter = zkStore.getNextCounter().join();

        // verify that the generated counter is from new range
        assertEquals(0, counter.getMsb());
        assertEquals(1L, counter.getLsb());
        assertEquals(zkStore.getCounterForTesting(), counter);
        Int96 limit = zkStore.getLimitForTesting();
        assertEquals(ZKStreamMetadataStore.COUNTER_RANGE, limit.getLsb());

        zkStore3.getRefreshFuture().join();
        assertEquals(ZKStreamMetadataStore.COUNTER_RANGE, zkStore3.getCounterForTesting().getLsb());
        assertEquals(ZKStreamMetadataStore.COUNTER_RANGE * 2, zkStore3.getLimitForTesting().getLsb());

        zkStore2.getRefreshFuture().join();
        assertEquals(ZKStreamMetadataStore.COUNTER_RANGE * 2, zkStore2.getCounterForTesting().getLsb());
        assertEquals(ZKStreamMetadataStore.COUNTER_RANGE * 3, zkStore2.getLimitForTesting().getLsb());

        zkStore.getRefreshFuture().join();
        assertEquals(ZKStreamMetadataStore.COUNTER_RANGE * 3, zkStore.getCounterForTesting().getLsb());
        assertEquals(ZKStreamMetadataStore.COUNTER_RANGE * 4, zkStore.getLimitForTesting().getLsb());
    }

    @Test
    public void listStreamsWithInactiveStream() throws Exception {
        // list stream in scope
        store.createScope("Scope").get();
        store.createStream("Scope", stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState("Scope", stream1, State.ACTIVE, null, executor).get();

        store.createStream("Scope", stream2, configuration2, System.currentTimeMillis(), null, executor).get();

        List<StreamConfiguration> streamInScope = store.listStreamsInScope("Scope").get();
        assertEquals("List streams in scope", 2, streamInScope.size());
        assertEquals("List streams in scope", stream1, streamInScope.get(0).getStreamName());
        assertEquals("List streams in scope", stream2, streamInScope.get(1).getStreamName());
    }

    @Test
    public void testInvalidOperation() throws Exception {
        // Test operation when stream is not in active state
        store.createScope(scope).get();
        store.createStream(scope, stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream1, State.CREATING, null, executor).get();

        AssertExtensions.assertThrows("Should throw IllegalStateException",
                store.getActiveSegments(scope, stream1, null, executor),
                (Throwable t) -> t instanceof StoreException.IllegalStateException);
    }

    @Test(timeout = 5000)
    public void testError() throws Exception {
        String host = "host";
        TxnResource txn = new TxnResource("SCOPE", "STREAM1", UUID.randomUUID());
        Predicate<Throwable> checker = (Throwable ex) -> ex instanceof StoreException.UnknownException;

        cli.close();
        testFailure(host, txn, checker);
    }

    @Test
    public void testConnectionLoss() throws Exception {
        String host = "host";
        TxnResource txn = new TxnResource("SCOPE", "STREAM1", UUID.randomUUID());
        Predicate<Throwable> checker = (Throwable ex) -> ex instanceof StoreException.StoreConnectionException;

        zkServer.close();
        AssertExtensions.assertThrows("Add txn to index fails", store.addTxnToIndex(host, txn, 0), checker);
    }

    private void testFailure(String host, TxnResource txn, Predicate<Throwable> checker) {
        AssertExtensions.assertThrows("Add txn to index fails", store.addTxnToIndex(host, txn, 0), checker);
        AssertExtensions.assertThrows("Remove txn fails", store.removeTxnFromIndex(host, txn, true), checker);
        AssertExtensions.assertThrows("Remove host fails", store.removeHostFromIndex(host), checker);
        AssertExtensions.assertThrows("Get txn version fails", store.getTxnVersionFromIndex(host, txn), checker);
        AssertExtensions.assertThrows("Get random txn fails", store.getRandomTxnFromIndex(host), checker);
        AssertExtensions.assertThrows("List hosts fails", store.listHostsOwningTxn(), checker);
    }

    @Test
    public void testScaleMetadata() throws Exception {
        String scope = "testScopeScale";
        String stream = "testStreamScale";
        ScalingPolicy policy = ScalingPolicy.fixed(3);
        StreamConfiguration configuration = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(policy).build();
        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.5, 1.0);
        List<SimpleEntry<Double, Double>> newRanges = Arrays.asList(segment1, segment2);

        store.createScope(scope).get();
        store.createStream(scope, stream, configuration, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        List<ScaleMetadata> scaleIncidents = store.getScaleMetadata(scope, stream, null, executor).get();
        assertTrue(scaleIncidents.size() == 1);
        assertTrue(scaleIncidents.get(0).getSegments().size() == 3);
        // scale
        scale(scope, stream, scaleIncidents.get(0).getSegments(), newRanges);
        scaleIncidents = store.getScaleMetadata(scope, stream, null, executor).get();
        assertTrue(scaleIncidents.size() == 2);
        assertTrue(scaleIncidents.get(0).getSegments().size() == 3);
        assertTrue(scaleIncidents.get(1).getSegments().size() == 2);

        // scale again
        scale(scope, stream, scaleIncidents.get(1).getSegments(), newRanges);
        scaleIncidents = store.getScaleMetadata(scope, stream, null, executor).get();
        assertTrue(scaleIncidents.size() == 3);
        assertTrue(scaleIncidents.get(0).getSegments().size() == 3);
        assertTrue(scaleIncidents.get(1).getSegments().size() == 2);
        assertTrue(scaleIncidents.get(2).getSegments().size() == 2);

        // scale again
        scale(scope, stream, scaleIncidents.get(2).getSegments(), newRanges);
        scaleIncidents = store.getScaleMetadata(scope, stream, null, executor).get();
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
        StreamConfiguration configuration = StreamConfiguration.builder().
                scope(scope).streamName(stream).scalingPolicy(policy).build();

        store.createScope(scope).get();
        store.createStream(scope, stream, configuration, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        // Case: Initial state, splits = 0, merges = 0
        // time t0, total segments 2, S0 {0.0 - 0.5} S1 {0.5 - 1.0}
        List<ScaleMetadata> scaleRecords = store.getScaleMetadata(scope, stream, null, executor).get();
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
        List<SimpleEntry<Double, Double>> newRanges1 = Arrays.asList(segment2, segment3, segment4, segment5, segment6);
        scale(scope, stream, scaleRecords.get(0).getSegments(), newRanges1);
        scaleRecords = store.getScaleMetadata(scope, stream, null, executor).get();
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
        List<SimpleEntry<Double, Double>> newRanges2 = Arrays.asList(segment7, segment8, segment9, segment10);
        scale(scope, stream, scaleRecords.get(1).getSegments(), newRanges2);
        scaleRecords = store.getScaleMetadata(scope, stream, null, executor).get();
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
        List<SimpleEntry<Double, Double>> newRanges3 = Arrays.asList(segment11, segment12);
        scale(scope, stream, scaleRecords.get(2).getSegments(), newRanges3);
        scaleRecords = store.getScaleMetadata(scope, stream, null, executor).get();
        assertEquals(scaleRecords.size(), 4);
        assertEquals(scaleRecords.get(3).getSegments().size(), 2);
        assertEquals(scaleRecords.get(3).getSplits(), 0L);
        assertEquals(scaleRecords.get(3).getMerges(), 2L);

        SimpleEntry<Long, Long> simpleEntrySplitsMerges3 = findSplitsAndMerges(scope, stream);
        assertEquals("Number of splits ", new Long(3), simpleEntrySplitsMerges3.getKey());
        assertEquals("Number of merges", new Long(4), simpleEntrySplitsMerges3.getValue());
    }

    @Test
    public void testCommittedTxnGc() {
        String scope = "scopeGC";
        String stream = "stream";
        store.createScope(scope).join();
        StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        store.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();

        ZKStreamMetadataStore zkStore = (ZKStreamMetadataStore) store;
        ZKStoreHelper storeHelper = zkStore.getStoreHelper();
        
        UUID txnId = new UUID(0L, 0L);
        createAndCommitTxn(txnId, scope, stream).join();
        
        // getTxnStatus
        TxnStatus status = store.transactionStatus(scope, stream, txnId, null, executor).join();
        assertEquals(TxnStatus.COMMITTED, status);
        // verify txns are created in a new batch
        List<Integer> batches = storeHelper.getChildren(ZKStreamMetadataStore.COMPLETED_TX_BATCH_ROOT_PATH).join()
                                        .stream().map(Integer::parseInt).collect(Collectors.toList());
        assertEquals(1, batches.size());
        int firstBatch = batches.get(0);

        // region Backward Compatibility
        // TODO 2755 retire code in this region
        String streamOldPath = ZKPaths.makePath(ZKPaths.makePath(ZKStreamMetadataStore.COMPLETED_TX_ROOT_PATH, scope), stream);
        List<UUID> oldSchemeTxns = storeHelper.getChildren(streamOldPath).join()
                                              .stream().map(UUID::fromString).sorted().collect(Collectors.toList());
        assertEquals(1, oldSchemeTxns.size());
        assertEquals(txnId, oldSchemeTxns.get(0));
        String txnOldPath = ZKPaths.makePath(streamOldPath, txnId.toString());
        Data<Integer> oldSchemeTxnData = storeHelper.getData(txnOldPath).join();
        assertEquals(TxnStatus.COMMITTED, CompletedTxnRecord.parse(oldSchemeTxnData.getData()).getCompletionStatus());
        // explicitly delete from old scheme
        storeHelper.deletePath(txnOldPath, false).join();
        status = store.transactionStatus(scope, stream, txnId, null, executor).join();
        assertEquals(TxnStatus.COMMITTED, status);
        // endregion
        
        // create another transaction after introducing a delay greater than gcperiod so that it gets created in a new batch 
        Futures.delayedFuture(() -> createAndCommitTxn(new UUID(0L, 1L), scope, stream), 
                Duration.ofSeconds(2).toMillis(), executor).join();
        // verify that this gets created in a new batch
        batches = storeHelper.getChildren(ZKStreamMetadataStore.COMPLETED_TX_BATCH_ROOT_PATH).join()
                             .stream().map(Integer::parseInt).sorted().collect(Collectors.toList());
        assertEquals(2, batches.size());
        assertTrue(batches.contains(firstBatch));
        int secondBatch = batches.stream().max(Integer::compare).get();
        assertTrue(secondBatch > firstBatch);
        
        // region Backward Compatibility
        // TODO: 2755 retire code in this region 
        oldSchemeTxns = storeHelper.getChildren(streamOldPath).join()
                                   .stream().map(UUID::fromString).sorted().collect(Collectors.toList());
        // verify it is created in old path
        assertEquals(1, oldSchemeTxns.size());
        // endregion
        
        // let one more gc cycle run and verify that these two batches are not cleaned up. 
        batches = Futures.delayedFuture(() -> storeHelper.getChildren(ZKStreamMetadataStore.COMPLETED_TX_BATCH_ROOT_PATH),
                Duration.ofSeconds(2).toMillis(), executor).join()
                             .stream().map(Integer::parseInt).sorted().collect(Collectors.toList());
        assertEquals(2, batches.size());

        // create third transaction after introducing a delay greater than gcperiod so that it gets created in a new batch
        Futures.delayedFuture(() -> createAndCommitTxn(new UUID(0L, 2L), scope, stream), 
                Duration.ofSeconds(2).toMillis(), executor).join();
        // Verify that a new batch is created here. 
        batches = storeHelper.getChildren(ZKStreamMetadataStore.COMPLETED_TX_BATCH_ROOT_PATH).join()
                             .stream().map(Integer::parseInt).sorted().collect(Collectors.toList());

        int thirdBatch = batches.stream().max(Long::compare).get();
        assertTrue(thirdBatch > secondBatch);
        
        // wait for more than TTL, then do another getTxnStatus
        status = Futures.delayedFuture(() -> store.transactionStatus(scope, stream, txnId, null, executor), 
                Duration.ofSeconds(2).toMillis(), executor).join();
        assertEquals(TxnStatus.UNKNOWN, status);

        // verify that only 2 latest batches remain and that firstBatch has been GC'd
        batches = storeHelper.getChildren(ZKStreamMetadataStore.COMPLETED_TX_BATCH_ROOT_PATH).join()
                             .stream().map(Integer::parseInt).sorted().collect(Collectors.toList());
        assertEquals(2, batches.size());
        assertFalse(batches.contains(firstBatch));
        assertTrue(batches.contains(secondBatch));
        assertTrue(batches.contains(thirdBatch));

        // region Backward Compatibility
        // TODO: 2755 retire code in this region
        // verify that for transactions with data only on old path, getTransactionStatus returns correct value
        storeHelper.createZNode(txnOldPath, oldSchemeTxnData.getData()).join();
        status = store.transactionStatus(scope, stream, txnId, null, executor).join();
        assertEquals(TxnStatus.COMMITTED, status);
        // endregion
    }

    private CompletableFuture<TxnStatus> createAndCommitTxn(UUID txnId, String scope, String stream) {
        return store.createTransaction(scope, stream, txnId, 100, 100, null, executor)
             .thenCompose(x -> store.setState(scope, stream, State.COMMITTING_TXN, null, executor))
             .thenCompose(x -> store.sealTransaction(scope, stream, txnId, true, Optional.empty(), null, executor))
             .thenCompose(x -> store.commitTransaction(scope, stream, txnId, null, executor));
    }

    private SimpleEntry<Long, Long> findSplitsAndMerges(String scope, String stream) throws InterruptedException, java.util.concurrent.ExecutionException {
        return store.getScaleMetadata(scope, stream, null, executor).get()
                .stream().reduce(new SimpleEntry<>(0L, 0L),
                        (x, y) -> new SimpleEntry<>(x.getKey() + y.getSplits(), x.getValue() + y.getMerges()),
                        (x, y) -> new SimpleEntry<>(x.getKey() + y.getKey(), x.getValue() + y.getValue()));
    }

    private void scale(String scope, String stream, List<Segment> segments,
                       List<SimpleEntry<Double, Double>> newRanges) {

        long scaleTimestamp = System.currentTimeMillis();
        List<Long> existingSegments = segments.stream().map(Segment::segmentId).collect(Collectors.toList());
        EpochTransitionRecord response = store.startScale(scope, stream, existingSegments, newRanges,
                scaleTimestamp, false, null, executor).join();
        ImmutableMap<Long, SimpleEntry<Double, Double>> segmentsCreated = response.getNewSegmentsWithRange();
        store.setState(scope, stream, State.SCALING, null, executor).join();
        store.scaleCreateNewSegments(scope, stream, false, null, executor).join();
        store.scaleNewSegmentsCreated(scope, stream, null, executor).join();
        store.scaleSegmentsSealed(scope, stream, existingSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)),
                null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();
    }
}
