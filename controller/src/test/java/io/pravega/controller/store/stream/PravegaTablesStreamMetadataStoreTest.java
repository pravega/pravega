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
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.CompletedTxnRecord;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.COMPLETED_TRANSACTIONS_BATCHES_TABLE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Zookeeper based stream metadata store tests.
 */
public class PravegaTablesStreamMetadataStoreTest extends StreamMetadataStoreTest {

    private TestingServer zkServer;
    private CuratorFramework cli;
    private SegmentHelper segmentHelperMockForTables;

    @Override
    public void setupStore() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
        segmentHelperMockForTables = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        store = new PravegaTablesStreamMetadataStore(segmentHelperMockForTables, cli, executor, Duration.ofSeconds(100), AuthHelper.getDisabledAuthHelper());
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
    
    @Test
    public void testGarbageCollection() {
        try (PravegaTablesStreamMetadataStore testStore = new PravegaTablesStreamMetadataStore(
                segmentHelperMockForTables, cli, executor, Duration.ofSeconds(100), AuthHelper.getDisabledAuthHelper())) {
            AtomicInteger currentBatch = new AtomicInteger(0);
            Supplier<Integer> supplier = currentBatch::get;
            ZKGarbageCollector gc = mock(ZKGarbageCollector.class);
            doAnswer(x -> supplier.get()).when(gc).getLatestBatch();
            testStore.setCompletedTxnGCRef(gc);

            String scope = "scopeGC";
            String stream = "streamGC";
            testStore.createScope(scope).join();

            StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
            testStore.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();

            // batch 0
            UUID txnId0 = testStore.generateTransactionId(scope, stream, null, executor).join();
            createAndCommitTransaction(scope, stream, txnId0, testStore);
            UUID txnId1 = testStore.generateTransactionId(scope, stream, null, executor).join();
            createAndCommitTransaction(scope, stream, txnId1, testStore);
            
            // verify that the completed txn record is created in batch 0
            Set<Integer> batches = getAllBatches(testStore);
            assertEquals(batches.size(), 1);
            assertTrue(batches.contains(0));
            Map<String, CompletedTxnRecord> transactions = getAllTransactionsInBatch(testStore, 0);
            // verify that transaction is present in batch 0
            assertTrue(transactions.containsKey(PravegaTablesStream.getCompletedTransactionKey(scope, stream, txnId1.toString())));
            
            // run gc. There should be no purge. 
            testStore.gcCompletedTxn().join();
            batches = getAllBatches(testStore);
            // verify no purge of batch
            assertEquals(batches.size(), 1);
            assertTrue(batches.contains(0));

            TxnStatus status = testStore.transactionStatus(scope, stream, txnId1, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);

            // create batch 1
            currentBatch.incrementAndGet();
            UUID txnId2 = testStore.generateTransactionId(scope, stream, null, executor).join();
            createAndCommitTransaction(scope, stream, txnId2, testStore);
            // verify that the completed txn record is created in batch 1
            batches = getAllBatches(testStore);
            assertEquals(batches.size(), 2);
            transactions = getAllTransactionsInBatch(testStore, 1);
            // verify that transaction is present in batch 1
            assertTrue(transactions.containsKey(PravegaTablesStream.getCompletedTransactionKey(scope, stream, txnId2.toString())));

            // run gc. There should be no purge. 
            testStore.gcCompletedTxn().join();
            batches = getAllBatches(testStore);
            // verify no purge of batch
            assertEquals(batches.size(), 2);
            assertTrue(batches.contains(0));
            assertTrue(batches.contains(1));

            status = testStore.transactionStatus(scope, stream, txnId1, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
            status = testStore.transactionStatus(scope, stream, txnId2, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);

            // create batch 2
            currentBatch.incrementAndGet();
            UUID txnId3 = testStore.generateTransactionId(scope, stream, null, executor).join();
            createAndCommitTransaction(scope, stream, txnId3, testStore);
            // verify that the completed txn record is created in batch 2
            batches = getAllBatches(testStore);
            assertEquals(batches.size(), 3);
            assertTrue(batches.contains(0));
            assertTrue(batches.contains(1));
            assertTrue(batches.contains(2));
            status = testStore.transactionStatus(scope, stream, txnId1, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
            status = testStore.transactionStatus(scope, stream, txnId2, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
            status = testStore.transactionStatus(scope, stream, txnId3, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);

            // dont run gc. let batches get accumulated.
            transactions = getAllTransactionsInBatch(testStore, 2);
            // verify that transaction is present in batch 2
            assertTrue(transactions.containsKey(PravegaTablesStream.getCompletedTransactionKey(scope, stream, txnId3.toString())));
             
            // create batch 3
            currentBatch.incrementAndGet();
            UUID txnId4 = testStore.generateTransactionId(scope, stream, null, executor).join();
            createAndCommitTransaction(scope, stream, txnId4, testStore);
            // verify that the completed txn record is created in batch 3
            batches = getAllBatches(testStore);
            assertEquals(batches.size(), 4);
            assertTrue(batches.contains(0));
            assertTrue(batches.contains(1));
            assertTrue(batches.contains(2));
            assertTrue(batches.contains(3));
            status = testStore.transactionStatus(scope, stream, txnId1, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
            status = testStore.transactionStatus(scope, stream, txnId2, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
            status = testStore.transactionStatus(scope, stream, txnId3, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
            status = testStore.transactionStatus(scope, stream, txnId4, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);

            transactions = getAllTransactionsInBatch(testStore, 3);
            // verify that transaction is present in batch 3
            assertTrue(transactions.containsKey(PravegaTablesStream.getCompletedTransactionKey(scope, stream, txnId4.toString())));

            // check that we are able to get status for all 4 transactions.
            status = testStore.transactionStatus(scope, stream, txnId1, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
            status = testStore.transactionStatus(scope, stream, txnId2, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
            status = testStore.transactionStatus(scope, stream, txnId3, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
            status = testStore.transactionStatus(scope, stream, txnId4, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
            
            // run gc. There should be two purges. 
            testStore.gcCompletedTxn().join();
            batches = getAllBatches(testStore);
            assertEquals(batches.size(), 2);
            assertTrue(batches.contains(2));
            assertTrue(batches.contains(3));

            // we should be able to get txn status for txn3 and txn4 but should get unknown for txn1 and txn2
            status = testStore.transactionStatus(scope, stream, txnId1, null, executor).join();
            assertEquals(status, TxnStatus.UNKNOWN);
            status = testStore.transactionStatus(scope, stream, txnId2, null, executor).join();
            assertEquals(status, TxnStatus.UNKNOWN);
            status = testStore.transactionStatus(scope, stream, txnId3, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
            status = testStore.transactionStatus(scope, stream, txnId4, null, executor).join();
            assertEquals(status, TxnStatus.COMMITTED);
        }
    }

    @Test
    public void testFindStaleBatches() {
        PravegaTablesStreamMetadataStore store = (PravegaTablesStreamMetadataStore) this.store;

        List<String> batches = Arrays.asList("5", "1", "6", "2", "7", "3", "8", "4", "0");
        List<String> stale = store.findStaleBatches(batches);
        assertEquals(batches.size() - 2, stale.size());
        assertFalse(stale.contains("8"));
        assertFalse(stale.contains("7"));

        batches = stale;
        stale = store.findStaleBatches(batches);
        assertEquals(batches.size() - 2, stale.size());
        assertFalse(stale.contains("6"));
        assertFalse(stale.contains("5"));

        batches = stale;
        stale = store.findStaleBatches(batches);
        assertEquals(batches.size() - 2, stale.size());
        assertFalse(stale.contains("4"));
        assertFalse(stale.contains("3"));

        batches = stale;
        stale = store.findStaleBatches(batches);
        assertEquals(batches.size() - 2, stale.size());
        assertFalse(stale.contains("2"));
        assertFalse(stale.contains("1"));

        batches = stale;
        stale = store.findStaleBatches(batches);
        assertTrue(stale.isEmpty());
    }

    private Set<Integer> getAllBatches(PravegaTablesStreamMetadataStore testStore) {
        Set<Integer> batches = new ConcurrentSkipListSet<>();
        testStore.getStoreHelper().getAllKeys(COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                 .collectRemaining(x -> {
                     batches.add(Integer.parseInt(x));
                     return true;
                 }).join();
        return batches;
    }

    private Map<String, CompletedTxnRecord> getAllTransactionsInBatch(PravegaTablesStreamMetadataStore testStore, int batch) {
        Map<String, CompletedTxnRecord> transactions = new ConcurrentHashMap<>();
        testStore.getStoreHelper().getAllEntries(PravegaTablesStream.getCompletedTransactionsBatchTableName(batch), 
                CompletedTxnRecord::fromBytes)
                 .collectRemaining(x -> {
                     transactions.put(x.getKey(), x.getValue().getObject());
                     return true;
                 }).join();
        return transactions;
    }

    private void createAndCommitTransaction(String scope, String stream, UUID txnId, PravegaTablesStreamMetadataStore testStore) {
        testStore.createTransaction(scope, stream, txnId, 10000L, 10000L, null, executor).join();
        testStore.sealTransaction(scope, stream, txnId, true, Optional.empty(), null, executor).join();
        VersionedMetadata<CommittingTransactionsRecord> record = testStore.startCommitTransactions(scope, stream, null, executor).join();
        testStore.completeCommitTransactions(scope, stream, record, null, executor).join();
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
