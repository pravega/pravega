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
package io.pravega.controller.store.stream;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.ZKScope;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.test.common.AssertExtensions;
import lombok.Synchronized;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Zookeeper based stream metadata store tests.
 */
public class ZKStreamMetadataStoreTest extends StreamMetadataStoreTest {

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
        store = new TestZkStore(cli, executor, Duration.ofSeconds(1));
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, 1,
                BucketStore.ServiceType.WatermarkingService, 1);

        bucketStore = StreamStoreFactory.createZKBucketStore(map, cli, executor);
    }

    @Override
    public void cleanupStore() throws Exception {
        store.close();
        cli.close();
        zkServer.close();
    }

    @Test
    public void listStreamsWithInactiveStream() throws Exception {
        // list stream in scope
        store.createScope("Scope", null, executor).get();
        store.createStream("Scope", stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState("Scope", stream1, State.ACTIVE, null, executor).get();

        store.createStream("Scope", stream2, configuration2, System.currentTimeMillis(), null, executor).get();

        Map<String, StreamConfiguration> streamInScope = store.listStreamsInScope("Scope", null, executor).get();
        assertEquals("List streams in scope", 1, streamInScope.size());
        assertTrue("List streams in scope", streamInScope.containsKey(stream1));
        assertFalse("List streams in scope", streamInScope.containsKey(stream2));
    }

    @Test
    public void listStreamsInScopes() throws Exception {
        // list stream in scope
        String scope = "scopeList";
        ZKStreamMetadataStore zkStore = spy((ZKStreamMetadataStore) store);
        
        store.createScope(scope, null, executor).get();

        LinkedBlockingQueue<Integer> nextPositionList = new LinkedBlockingQueue<>();
        nextPositionList.put(0);
        nextPositionList.put(2);
        nextPositionList.put(10000);
        nextPositionList.put((int) Math.pow(10, 8));
        nextPositionList.put((int) Math.pow(10, 9));
        ZKScope myScope = spy((ZKScope) zkStore.getScope(scope, null));
        doAnswer(x -> CompletableFuture.completedFuture(nextPositionList.poll())).when(myScope).getNextStreamPosition();
        doAnswer(x -> myScope).when(zkStore).getScope(scope, null);
        
        String stream1 = "stream1";
        String stream2 = "stream2";
        String stream3 = "stream3";
        String stream4 = "stream4";
        String stream5 = "stream5";

        // add three streams and then list them. We should get 2 + 1 + 0
        zkStore.createStream(scope, stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        zkStore.setState(scope, stream1, State.ACTIVE, null, executor).get();
        zkStore.createStream(scope, stream2, configuration2, System.currentTimeMillis(), null, executor).get();
        zkStore.setState(scope, stream2, State.ACTIVE, null, executor).get();
        zkStore.createStream(scope, stream3, configuration2, System.currentTimeMillis(), null, executor).get();
        zkStore.setState(scope, stream3, State.ACTIVE, null, executor).get();
        
        Pair<List<String>, String> streamInScope = store.listStream(scope, "", 2, executor, null).get();
        assertEquals("List streams in scope", 2, streamInScope.getKey().size());
        assertTrue(streamInScope.getKey().contains(stream1));
        assertTrue(streamInScope.getKey().contains(stream2));
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));

        streamInScope = store.listStream(scope, streamInScope.getValue(), 2, executor, null).get();
        assertEquals("List streams in scope", 1, streamInScope.getKey().size());
        assertTrue(streamInScope.getKey().contains(stream3));
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));

        streamInScope = store.listStream(scope, streamInScope.getValue(), 2, executor, null).get();
        assertEquals("List streams in scope", 0, streamInScope.getKey().size());
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));

        // add 4th stream
        zkStore.createStream(scope, stream4, configuration2, System.currentTimeMillis(), null, executor).get();
        zkStore.setState(scope, stream4, State.ACTIVE, null, executor).get();

        // list on previous token we should get 1 entry
        streamInScope = store.listStream(scope, streamInScope.getValue(), 2, executor, null).get();
        assertEquals("List streams in scope", 1, streamInScope.getKey().size());
        assertTrue(streamInScope.getKey().contains(stream4));
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));
 
        // add 5th stream
        zkStore.createStream(scope, stream5, configuration2, System.currentTimeMillis(), null, executor).get();
        zkStore.setState(scope, stream5, State.ACTIVE, null, executor).get();

        // delete stream 1
        store.deleteStream(scope, stream1, null, executor).join();

        // start listing with empty/default continuation token
        streamInScope = store.listStream(scope, "", 2, executor, null).get();
        assertEquals("List streams in scope", 2, streamInScope.getKey().size());
        assertTrue(streamInScope.getKey().contains(stream2));
        assertTrue(streamInScope.getKey().contains(stream3));
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));

        streamInScope = store.listStream(scope, streamInScope.getValue(), 2, executor, null).get();
        assertEquals("List streams in scope", 2, streamInScope.getKey().size());
        assertTrue(streamInScope.getKey().contains(stream4));
        assertTrue(streamInScope.getKey().contains(stream5));
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));

        // delete stream 3
        store.deleteStream(scope, stream3, null, executor).join();
        
        // start listing with empty/default continuation token
        streamInScope = store.listStream(scope, "", 2, executor, null).get();
        assertEquals("List streams in scope", 2, streamInScope.getKey().size());
        assertTrue(streamInScope.getKey().contains(stream2));
        assertTrue(streamInScope.getKey().contains(stream4));
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));

        streamInScope = store.listStream(scope, streamInScope.getValue(), 2, executor, null).get();
        assertEquals("List streams in scope", 1, streamInScope.getKey().size());
        assertTrue(streamInScope.getKey().contains(stream5));
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));
    }

    @Test
    public void testInvalidOperation() throws Exception {
        // Test operation when stream is not in active state
        store.createScope(scope, null, executor).get();
        store.createStream(scope, stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream1, State.CREATING, null, executor).get();

        AssertExtensions.assertFutureThrows("Should throw IllegalStateException",
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
        AssertExtensions.assertFutureThrows("Add txn to index fails", store.addTxnToIndex(host, txn, new Version.IntVersion(0)), checker);
    }

    private void testFailure(String host, TxnResource txn, Predicate<Throwable> checker) {
        AssertExtensions.assertFutureThrows("Add txn to index fails", store.addTxnToIndex(host, txn, new Version.IntVersion(0)), checker);
        AssertExtensions.assertFutureThrows("Remove txn fails", store.removeTxnFromIndex(host, txn, true), checker);
        AssertExtensions.assertFutureThrows("Remove host fails", store.removeHostFromIndex(host), checker);
        AssertExtensions.assertFutureThrows("Get txn version fails", store.getTxnVersionFromIndex(host, txn), checker);
    }

    @Test
    public void testScaleMetadata() throws Exception {
        String scope = "testScopeScale";
        String stream = "testStreamScale";
        ScalingPolicy policy = ScalingPolicy.fixed(3);
        StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();
        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.5, 1.0);
        List<Map.Entry<Double, Double>> newRanges = Arrays.asList(segment1, segment2);

        store.createScope(scope, null, executor).get();
        store.createStream(scope, stream, configuration, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        // set minimum number of segments to 1 so that we can also test scale downs
        configuration = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        store.startUpdateConfiguration(scope, stream, configuration, null, executor).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = store.getConfigurationRecord(scope, stream, null, executor).join();
        store.completeUpdateConfiguration(scope, stream, configRecord, null, executor).join();

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

        store.createScope(scope, null, executor).get();
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

        assertEquals("Number of splits ", 0L, simpleEntrySplitsMerges.getKey().longValue());
        assertEquals("Number of merges", 0L, simpleEntrySplitsMerges.getValue().longValue());

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
        assertEquals("Number of splits ", 2L, simpleEntrySplitsMerges1.getKey().longValue());
        assertEquals("Number of merges", 0L, simpleEntrySplitsMerges1.getValue().longValue());

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
        assertEquals("Number of splits ", 3L, simpleEntrySplitsMerges2.getKey().longValue());
        assertEquals("Number of merges", 2L, simpleEntrySplitsMerges2.getValue().longValue());

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
        assertEquals("Number of splits ", 3, simpleEntrySplitsMerges3.getKey().longValue());
        assertEquals("Number of merges", 4, simpleEntrySplitsMerges3.getValue().longValue());
    }

    @Test
    public void testCommittedTxnGc() {
        String scope = "scopeGC";
        String stream = "stream";
        store.createScope(scope, null, executor).join();
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1))
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
    }

    @Test
    @Override
    public void listScopesPaginated() throws Exception {
        AssertExtensions.assertThrows("", () -> store.listScopes("", 1, executor, 0L),
                e -> Exceptions.unwrap(e) instanceof UnsupportedOperationException);
    }

    @Test
    @Override
    public void testReaderGroups() {
        AssertExtensions.assertThrows(UnsupportedOperationException.class,
                () -> store.getReaderGroupId("scope", "readergroup", null, executor).get());
        AssertExtensions.assertThrows(UnsupportedOperationException.class,
                () -> store.checkReaderGroupExists("scope", "readergroup", null, executor).get());
    }
    
    private CompletableFuture<TxnStatus> createAndCommitTxn(UUID txnId, String scope, String stream) {
        return store.createTransaction(scope, stream, txnId, 100, 100, null, executor)
             .thenCompose(x -> store.setState(scope, stream, State.COMMITTING_TXN, null, executor))
             .thenCompose(x -> store.sealTransaction(scope, stream, txnId, true, Optional.empty(), "", 
                     Long.MIN_VALUE, null, executor))
             .thenCompose(x -> ((AbstractStreamMetadataStore) store).commitTransaction(scope, stream, txnId, null, executor));
    }

    private SimpleEntry<Long, Long> findSplitsAndMerges(String scope, String stream) throws InterruptedException, 
            java.util.concurrent.ExecutionException {
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

    static class TestZkStore extends ZKStreamMetadataStore implements TestStore {
        HashMap<String, ZKStream> map = new HashMap<>();

        TestZkStore(CuratorFramework curatorClient, ScheduledExecutorService executor, Duration gcPeriod) {
            super(curatorClient, executor, gcPeriod);
        }

        @Override
        @Synchronized
        ZKStream newStream(String scope, String name) {
            String scopedStreamName = NameUtils.getScopedStreamName(scope, name);
            if (map.containsKey(scopedStreamName)) {
                return map.get(scopedStreamName);
            } else {
                return super.newStream(scope, name);
            }
        }

        @Override
        @Synchronized
        public void setStream(Stream stream) {
            String scopedStreamName = NameUtils.getScopedStreamName(stream.getScope(), stream.getName());
            map.put(scopedStreamName, (ZKStream) stream);
        }

        @Override
        public void close() {
            map.clear();
            super.close();
        }
    }
}
