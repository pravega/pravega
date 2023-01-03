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

import com.google.common.collect.Lists;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.TestOperationContext;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

public class ZkStreamTest {
    private static final String SCOPE = "scope";
    private TestingServer zkTestServer;
    private CuratorFramework cli;
    private StreamMetadataStore storePartialMock;

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServerStarter().start();
        cli = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(2000));
        cli.start();

        storePartialMock = Mockito.spy(new ZKStreamMetadataStore(cli, executor));
    }

    @After
    public void stopZookeeper() throws Exception {
        cli.close();
        zkTestServer.close();
        ExecutorServiceHelpers.shutdown(executor);
        storePartialMock.close();
    }

    @Test(timeout = 30000)
    public void testZkConnectionLoss() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(5);

        final String streamName = "testfail";

        final StreamConfiguration streamConfig = StreamConfiguration.builder().scalingPolicy(policy).build();

        zkTestServer.stop();

        try {
            storePartialMock.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();
        } catch (ExecutionException e) {
            assert e.getCause() instanceof StoreException.StoreConnectionException;
        }
        zkTestServer.start();
    }

    @Test(timeout = 30000)
    public void testCreateStreamState() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(5);

        @Cleanup
        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "testfail";

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(policy)
                .build();

        store.createScope(SCOPE, null, executor).get();
        store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();

        try {
            store.getConfiguration(SCOPE, streamName, null, executor).get();
        } catch (Exception e) {
            assert e.getCause() != null && e.getCause() instanceof IllegalStateException;
        }
        store.deleteScope(SCOPE, null, executor);
    }

    @Test(timeout = 30000)
    public void testZkCreateScope() throws Exception {

        // create new scope test
        @Cleanup
        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String scopeName = "Scope1";
        CompletableFuture<CreateScopeStatus> createScopeStatus = store.createScope(scopeName, null, executor);

        // createScope returns null on success, and exception on failure
        assertEquals("Create new scope :", CreateScopeStatus.Status.SUCCESS, createScopeStatus.get().getStatus());

        // create duplicate scope test
        createScopeStatus = store.createScope(scopeName, null, executor);
        assertEquals("Create new scope :", CreateScopeStatus.Status.SCOPE_EXISTS, createScopeStatus.get().getStatus());

        //listStreams test
        final String streamName1 = "Stream1";
        final String streamName2 = "Stream2";
        final ScalingPolicy policy = ScalingPolicy.fixed(5);
        StreamConfiguration streamConfig = StreamConfiguration.builder().scalingPolicy(policy).build();

        StreamConfiguration streamConfig2 = StreamConfiguration.builder().scalingPolicy(policy).build();

        store.createStream(scopeName, streamName1, streamConfig, System.currentTimeMillis(), null, executor).get();
        store.setState(scopeName, streamName1, State.ACTIVE, null, executor).get();
        store.createStream(scopeName, streamName2, streamConfig2, System.currentTimeMillis(), null, executor).get();
        store.setState(scopeName, streamName2, State.ACTIVE, null, executor).get();

        Map<String, StreamConfiguration> listOfStreams = store.listStreamsInScope(scopeName, null, executor).get();
        assertEquals("Size of list", 2, listOfStreams.size());
        assertTrue("Name of stream at index zero", listOfStreams.containsKey("Stream1"));
        assertTrue("Name of stream at index one", listOfStreams.containsKey("Stream2"));
    }

    @Test(timeout = 30000)
    public void testZkDeleteScope() throws Exception {
        // create new scope
        @Cleanup
        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String scopeName = "Scope1";
        store.createScope(scopeName, null, executor).get();

        // Delete empty scope Scope1
        CompletableFuture<DeleteScopeStatus> deleteScopeStatus = store.deleteScope(scopeName, null, executor);
        assertEquals("Delete Empty Scope", DeleteScopeStatus.Status.SUCCESS, deleteScopeStatus.get().getStatus());

        // Delete non-existent scope Scope2
        CompletableFuture<DeleteScopeStatus> deleteScopeStatus2 = store.deleteScope("Scope2", null, executor);
        assertEquals("Delete non-existent Scope", DeleteScopeStatus.Status.SCOPE_NOT_FOUND, deleteScopeStatus2.get().getStatus());

        // Delete non-empty scope Scope3
        store.createScope("Scope3", null, executor).get();
        final ScalingPolicy policy = ScalingPolicy.fixed(5);
        final StreamConfiguration streamConfig =
                StreamConfiguration.builder().scalingPolicy(policy).build();

        store.createStream("Scope3", "Stream3", streamConfig, System.currentTimeMillis(), 
                null, executor).get();
        store.setState("Scope3", "Stream3", State.ACTIVE, null, executor).get();

        CompletableFuture<DeleteScopeStatus> deleteScopeStatus3 = store.deleteScope("Scope3", null, executor);
        assertEquals("Delete non-empty Scope", DeleteScopeStatus.Status.SCOPE_NOT_EMPTY,
                deleteScopeStatus3.get().getStatus());
    }

    @Test(timeout = 30000)
    public void testGetScope() throws Exception {
        @Cleanup
        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String scope1 = "Scope1";
        final String scope2 = "Scope2";
        String scopeName;

        // get existent scope
        store.createScope(scope1, null, executor).get();
        scopeName = store.getScopeConfiguration(scope1, null, executor).get();
        assertEquals("Get existent scope", scope1, scopeName);

        // get non-existent scope
        try {
            store.getScopeConfiguration(scope2, null, executor).get();
        } catch (ExecutionException e) {
            assertTrue("Get non existent scope", e.getCause() instanceof StoreException.DataNotFoundException);
        }
    }

    @Test(timeout = 30000)
    public void testZkListScope() throws Exception {
        // list scope test
        @Cleanup
        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        store.createScope("Scope1", null, executor).get();
        store.createScope("Scope2", null, executor).get();
        store.createScope("Scope3", null, executor).get();

        List<String> listScopes = store.listScopes(executor, 0L).get();
        assertEquals("List Scopes ", 3, listScopes.size());

        store.deleteScope("Scope3", null, executor).get();
        listScopes = store.listScopes(executor, 0L).get();
        assertEquals("List Scopes ", 2, listScopes.size());
    }

    @Test(timeout = 30000)
    public void testZkStream() throws Exception {
        double keyChunk = 1.0 / 5;
        final ScalingPolicy policy = ScalingPolicy.fixed(5);

        @Cleanup
        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "test";
        store.createScope(SCOPE, null, executor).get();

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(policy)
                .build();

        store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();
        store.setState(SCOPE, streamName, State.ACTIVE, null, executor).get();
        OperationContext context = store.createStreamContext(SCOPE, streamName, 0L);

        // set minimum number of segments to 1 so that we can also test scale downs
        streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        store.startUpdateConfiguration(SCOPE, streamName, streamConfig, null, executor).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = store.getConfigurationRecord(SCOPE, streamName, null, executor).join();
        store.completeUpdateConfiguration(SCOPE, streamName, configRecord, null, executor).join();

        List<StreamSegmentRecord> segments = store.getActiveSegments(SCOPE, streamName, context, executor).get();
        assertEquals(segments.size(), 5);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0L, 1L, 2L, 3L, 4L).contains(x.segmentId())));

        long start = segments.get(0).getCreationTime();

        assertEquals(store.getConfiguration(SCOPE, streamName, context, executor).get(), streamConfig);

        List<Map.Entry<Double, Double>> newRanges;

        // existing range 0 = 0 - .2, 1 = .2 - .4, 2 = .4 - .6, 3 = .6 - .8, 4 = .8 - 1.0

        // 3, 4 -> 5 = .6 - 1.0
        newRanges = Collections.singletonList(
                new AbstractMap.SimpleEntry<>(3 * keyChunk, 1.0));

        long scale1 = start + 10000;
        ArrayList<Long> sealedSegments = Lists.newArrayList(3L, 4L);
        long five = computeSegmentId(5, 1);
        VersionedMetadata<EpochTransitionRecord> versioned = store.submitScale(SCOPE, streamName, sealedSegments, newRanges, scale1, null, context, executor).get();
        VersionedMetadata<State> state = store.getVersionedState(SCOPE, streamName, null, executor).join();
        state = store.updateVersionedState(SCOPE, streamName, State.SCALING, state, null, executor).join();
        versioned = store.startScale(SCOPE, streamName, false, versioned, state, null, executor).join();
        store.scaleCreateNewEpochs(SCOPE, streamName, versioned, context, executor).get();
        store.scaleSegmentsSealed(SCOPE, streamName, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned,
                context, executor).get();
        store.completeScale(SCOPE, streamName, versioned, null, executor).join();
        store.setState(SCOPE, streamName, State.ACTIVE, null, executor).join();
        segments = store.getActiveSegments(SCOPE, streamName, context, executor).get();
        assertEquals(segments.size(), 4);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0L, 1L, 2L, five).contains(x.segmentId())));

        // 1 -> 6 = 0.2 -.3, 7 = .3 - .4
        // 2,5 -> 8 = .4 - 1.0
        newRanges = Arrays.asList(
                new AbstractMap.SimpleEntry<>(keyChunk, 0.3),
                new AbstractMap.SimpleEntry<>(0.3, 2 * keyChunk),
                new AbstractMap.SimpleEntry<>(2 * keyChunk, 1.0));

        long scale2 = scale1 + 10000;
        ArrayList<Long> sealedSegments1 = Lists.newArrayList(1L, 2L, five);
        long six = computeSegmentId(6, 2);
        long seven = computeSegmentId(7, 2);
        long eight = computeSegmentId(8, 2);
        versioned = store.submitScale(SCOPE, streamName, sealedSegments1, newRanges, scale2, null, context, executor).get();
        EpochTransitionRecord response = versioned.getObject();
        state = store.getVersionedState(SCOPE, streamName, null, executor).join();
        state = store.updateVersionedState(SCOPE, streamName, State.SCALING, state, null, executor).join();
        versioned = store.startScale(SCOPE, streamName, false, versioned, state, null, executor).join();
        store.scaleCreateNewEpochs(SCOPE, streamName, versioned, context, executor).get();
        store.scaleSegmentsSealed(SCOPE, streamName, sealedSegments1.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned,
                context, executor).get();
        store.completeScale(SCOPE, streamName, versioned, null, executor).join();
        store.setState(SCOPE, streamName, State.ACTIVE, null, executor).join();

        segments = store.getActiveSegments(SCOPE, streamName, context, executor).get();
        assertEquals(segments.size(), 4);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0L, six, seven, eight).contains(x.segmentId())));

        // 7 -> 9 = .3 - .35, 10 = .35 - .6
        // 8 -> 10 = .35 - .6, 11 = .6 - 1.0
        newRanges = Arrays.asList(
                new AbstractMap.SimpleEntry<>(0.3, 0.35),
                new AbstractMap.SimpleEntry<>(0.35, 3 * keyChunk),
                new AbstractMap.SimpleEntry<>(3 * keyChunk, 1.0));

        long scale3 = scale2 + 10000;
        long nine = computeSegmentId(9, 3);
        long ten = computeSegmentId(10, 3);
        long eleven = computeSegmentId(11, 3);
        ArrayList<Long> sealedSegments2 = Lists.newArrayList(seven, eight);
        versioned = store.submitScale(SCOPE, streamName, sealedSegments2, newRanges, scale3, null, context, executor).get();
        response = versioned.getObject();
        state = store.getVersionedState(SCOPE, streamName, null, executor).join();
        state = store.updateVersionedState(SCOPE, streamName, State.SCALING, state, null, executor).join();
        store.startScale(SCOPE, streamName, false, versioned, state, null, executor).join();
        store.scaleCreateNewEpochs(SCOPE, streamName, versioned, context, executor).get();
        store.scaleSegmentsSealed(SCOPE, streamName, sealedSegments2.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned,
                context, executor).get();
        store.completeScale(SCOPE, streamName, versioned, null, executor).join();
        store.setState(SCOPE, streamName, State.ACTIVE, null, executor).join();

        segments = store.getActiveSegments(SCOPE, streamName, context, executor).get();
        assertEquals(segments.size(), 5);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0L, six, nine, ten, eleven).contains(x.segmentId())));

        Map<Long, List<Long>> successors = store.getSuccessors(SCOPE, streamName, 0L, context, executor).get()
                .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.isEmpty());
        successors = store.getSuccessors(SCOPE, streamName, 1L, context, executor).get()
                          .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.size() == 2 &&
                successors.containsKey(six) && successors.get(six).containsAll(Collections.singleton(1L)) &&
                successors.containsKey(seven) && successors.get(seven).containsAll(Collections.singleton(1L)));

        successors = store.getSuccessors(SCOPE, streamName, 2L, context, executor).get()
                          .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.size() == 1 &&
                successors.containsKey(eight) && successors.get(eight).containsAll(Lists.newArrayList(2L, five)));

        successors = store.getSuccessors(SCOPE, streamName, 3L, context, executor).get()
                          .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.size() == 1 &&
                successors.containsKey(five) && successors.get(five).containsAll(Lists.newArrayList(3L, 4L)));

        successors = store.getSuccessors(SCOPE, streamName, 4L, context, executor).get()
                          .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.size() == 1 &&
                successors.containsKey(five) && successors.get(five).containsAll(Lists.newArrayList(3L, 4L)));

        successors = store.getSuccessors(SCOPE, streamName, five, context, executor).get()
                          .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.size() == 1 &&
                successors.containsKey(eight) && successors.get(eight).containsAll(Lists.newArrayList(2L, five)));

        successors = store.getSuccessors(SCOPE, streamName, six, context, executor).get()
                          .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.isEmpty());
        successors = store.getSuccessors(SCOPE, streamName, seven, context, executor).get()
                          .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.size() == 2 &&
                successors.containsKey(nine) && successors.get(nine).containsAll(Collections.singleton(seven)) &&
                successors.containsKey(ten) && successors.get(ten).containsAll(Lists.newArrayList(seven, eight)));
        successors = store.getSuccessors(SCOPE, streamName, eight, context, executor).get()
                          .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.size() == 2 &&
                successors.containsKey(eleven) && successors.get(eleven).containsAll(Collections.singleton(eight)) &&
                successors.containsKey(ten) && successors.get(ten).containsAll(Lists.newArrayList(seven, eight)));
        successors = store.getSuccessors(SCOPE, streamName, nine, context, executor).get()
                          .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.isEmpty());
        successors = store.getSuccessors(SCOPE, streamName, ten, context, executor).get()
                          .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.isEmpty());
        successors = store.getSuccessors(SCOPE, streamName, eleven, context, executor).get()
                          .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(successors.isEmpty());
        // start -1
        Map<Long, Long> historicalSegments = store.getSegmentsAtHead(SCOPE, streamName, context, executor).get()
                                                  .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertEquals(historicalSegments.size(), 5);
        assertTrue(historicalSegments.keySet().containsAll(Lists.newArrayList(0L, 1L, 2L, 3L, 4L)));

        // start + 1
        List<Long> segmentsInEpoch = store.getSegmentsInEpoch(SCOPE, streamName, 0, context, executor).get()
                                             .stream().map(x -> x.segmentId()).collect(Collectors.toList());
        assertEquals(segmentsInEpoch.size(), 5);
        assertTrue(segmentsInEpoch.containsAll(Lists.newArrayList(0L, 1L, 2L, 3L, 4L)));

        // scale1
        segmentsInEpoch = store.getSegmentsInEpoch(SCOPE, streamName, 1, context, executor).get()
                               .stream().map(x -> x.segmentId()).collect(Collectors.toList());
        assertEquals(segmentsInEpoch.size(), 4);
        assertTrue(segmentsInEpoch.containsAll(Lists.newArrayList(0L, 1L, 2L, five)));

        // scale2
        segmentsInEpoch = store.getSegmentsInEpoch(SCOPE, streamName, 2, context, executor).get()
                               .stream().map(x -> x.segmentId()).collect(Collectors.toList());
        assertEquals(segmentsInEpoch.size(), 4);
        assertTrue(segmentsInEpoch.containsAll(Lists.newArrayList(0L, six, seven, eight)));

        // scale3
        segmentsInEpoch = store.getSegmentsInEpoch(SCOPE, streamName, 3, context, executor).get()
                               .stream().map(x -> x.segmentId()).collect(Collectors.toList());
        assertEquals(segmentsInEpoch.size(), 5);
        assertTrue(segmentsInEpoch.containsAll(Lists.newArrayList(0L, six, nine, ten, eleven)));

        assertFalse(store.isSealed(SCOPE, streamName, context, executor).get());
        assertNotEquals(0, store.getActiveSegments(SCOPE, streamName, context, executor).get().size());
        store.setSealed(SCOPE, streamName, context, executor).get();
        assertTrue(store.isSealed(SCOPE, streamName, context, executor).get());
        assertEquals(0, store.getActiveSegments(SCOPE, streamName, context, executor).get().size());

        //seal an already sealed stream.
        store.setSealed(SCOPE, streamName, context, executor).get();
        assertTrue(store.isSealed(SCOPE, streamName, context, executor).get());
        assertEquals(0, store.getActiveSegments(SCOPE, streamName, context, executor).get().size());

        //seal a non existing stream.
        AssertExtensions.assertFutureThrows("", store.setSealed(SCOPE, "nonExistentStream", null, executor), 
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);

        store.markCold(SCOPE, streamName, 0L, System.currentTimeMillis() + 1000, null, executor).get();
        assertTrue(store.isCold(SCOPE, streamName, 0L, null, executor).get());
        Thread.sleep(1000);
        assertFalse(store.isCold(SCOPE, streamName, 0L, null, executor).get());

        store.markCold(SCOPE, streamName, 0L, System.currentTimeMillis() + 1000, null, executor).get();
        store.removeMarker(SCOPE, streamName, 0L, null, executor).get();

        assertFalse(store.isCold(SCOPE, streamName, 0L, null, executor).get());
    }

    @Test(timeout = 10000)
    public void testTransaction() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(5);

        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "testTx";
        store.createScope(SCOPE, null, executor).get();
        final Predicate<Throwable> operationNotAllowedPredicate =
                ex -> Exceptions.unwrap(ex) instanceof StoreException.IllegalStateException;

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(policy)
                .build();

        store.createStream(SCOPE, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();
        store.setState(SCOPE, streamName, State.ACTIVE, null, executor).get();

        OperationContext context = store.createStreamContext(ZkStreamTest.SCOPE, streamName, 0L);

        Map<UUID, TxnStatus> listAborted = store.listCompletedTxns(SCOPE, streamName, context, executor).join();
        assertEquals(0, listAborted.size());

        UUID txnId1 = store.generateTransactionId(SCOPE, streamName, null, executor).join();
        VersionedTransactionData tx = store.createTransaction(SCOPE, streamName, txnId1, 10000, 600000,
                context, executor).get();
        Assert.assertEquals(txnId1, tx.getId());

        UUID txnId2 = store.generateTransactionId(SCOPE, streamName, null, executor).join();
        VersionedTransactionData tx2 = store.createTransaction(SCOPE, streamName, txnId2, 10000, 600000,
                context, executor).get();
        Assert.assertEquals(txnId2, tx2.getId());

        store.sealTransaction(SCOPE, streamName, tx.getId(), true, Optional.empty(), "", Long.MIN_VALUE,
                context, executor).get();
        assert store.transactionStatus(SCOPE, streamName, tx.getId(), context, executor)
                .get().equals(TxnStatus.COMMITTING);

        // Test to ensure that sealTransaction is idempotent.
        Assert.assertEquals(TxnStatus.COMMITTING, store.sealTransaction(SCOPE, streamName, tx.getId(), true,
                Optional.empty(), "", Long.MIN_VALUE, context, executor).join().getKey());

        // Test to ensure that COMMITTING_TXN transaction cannot be aborted.
        testAbortFailure(store, SCOPE, streamName, tx.getEpoch(), tx.getId(), context, operationNotAllowedPredicate);

        store.setState(SCOPE, streamName, State.COMMITTING_TXN, context, executor).join();
        CompletableFuture<TxnStatus> f1 = ((AbstractStreamMetadataStore) store).commitTransaction(SCOPE, streamName, tx.getId(), context, executor);
        store.setState(SCOPE, streamName, State.ACTIVE, context, executor).join();

        store.sealTransaction(SCOPE, streamName, tx2.getId(), false, Optional.empty(),
                "", Long.MIN_VALUE, context, executor).get();
        assert store.transactionStatus(SCOPE, streamName, tx2.getId(), context, executor)
                .get().equals(TxnStatus.ABORTING);

        // Test to ensure that sealTransaction is idempotent.
        Assert.assertEquals(TxnStatus.ABORTING, store.sealTransaction(SCOPE, streamName, tx2.getId(), false,
                Optional.empty(), "", Long.MIN_VALUE, context, executor).join().getKey());

        // Test to ensure that ABORTING transaction cannot be committed.
        testCommitFailure(store, SCOPE, streamName, tx2.getEpoch(), tx2.getId(), context, operationNotAllowedPredicate);

        CompletableFuture<TxnStatus> f2 = store.abortTransaction(SCOPE, streamName, tx2.getId(), context, executor);

        CompletableFuture.allOf(f1, f2).get();

        assert store.transactionStatus(SCOPE, streamName, tx.getId(), context, executor)
                .get().equals(TxnStatus.COMMITTED);
        assert store.transactionStatus(SCOPE, streamName, tx2.getId(), context, executor)
                .get().equals(TxnStatus.ABORTED);

        // Test to ensure that sealTransaction, to commit it, on committed transaction does not throw an error.
        Assert.assertEquals(TxnStatus.COMMITTED, store.sealTransaction(SCOPE, streamName, tx.getId(), true,
                Optional.empty(), "", Long.MIN_VALUE, context, executor).join().getKey());

        // Test to ensure that commitTransaction is idempotent.
        store.setState(SCOPE, streamName, State.COMMITTING_TXN, context, executor).join();
        Assert.assertEquals(TxnStatus.COMMITTED,
                ((AbstractStreamMetadataStore) store).commitTransaction(SCOPE, streamName, tx.getId(), context, executor).join());
        store.setState(SCOPE, streamName, State.ACTIVE, context, executor).join();

        // Test to ensure that sealTransaction, to abort it, and abortTransaction on committed transaction throws error.
        testAbortFailure(store, SCOPE, streamName, tx.getEpoch(), tx.getId(), context, operationNotAllowedPredicate);

        // Test to ensure that sealTransaction, to abort it, on aborted transaction does not throw an error.
        Assert.assertEquals(TxnStatus.ABORTED, store.sealTransaction(SCOPE, streamName, tx2.getId(), false,
                Optional.empty(), "", Long.MIN_VALUE, context, executor).join().getKey());

        // Test to ensure that abortTransaction is idempotent.
        Assert.assertEquals(TxnStatus.ABORTED,
                store.abortTransaction(SCOPE, streamName, tx2.getId(), context, executor).join());

        listAborted = store.listCompletedTxns(SCOPE, streamName, context, executor).join();
        assertEquals(2, listAborted.size());

        // Test to ensure that sealTransaction, to abort it, and abortTransaction on committed transaction throws error.
        testCommitFailure(store, SCOPE, streamName, tx2.getEpoch(), tx2.getId(), context, operationNotAllowedPredicate);

        store.setState(SCOPE, streamName, State.COMMITTING_TXN, context, executor).join();
        assert ((AbstractStreamMetadataStore) store).commitTransaction(ZkStreamTest.SCOPE, streamName, UUID.randomUUID(), null, executor)
                .handle((ok, ex) -> {
                    if (ex.getCause() instanceof StoreException.DataNotFoundException) {
                        return true;
                    } else {
                        throw new RuntimeException("assert failed");
                    }
                }).get();
        store.setState(SCOPE, streamName, State.ACTIVE, context, executor).join();

        assert store.abortTransaction(ZkStreamTest.SCOPE, streamName, UUID.randomUUID(), null, executor)
                .handle((ok, ex) -> {
                    if (ex.getCause() instanceof StoreException.DataNotFoundException) {
                        return true;
                    } else {
                        throw new RuntimeException("assert failed");
                    }
                }).get();

        assert store.transactionStatus(ZkStreamTest.SCOPE, streamName, UUID.randomUUID(), context, executor)
                .get().equals(TxnStatus.UNKNOWN);
    }

    @Test(timeout = 30000)
    public void testGetActiveTxn() throws Exception {
        ZKStoreHelper storeHelper = spy(new ZKStoreHelper(cli, executor));
        ZkOrderedStore orderer = new ZkOrderedStore("txn", storeHelper, executor);
        ZKStream stream = new ZKStream("scope", "stream", storeHelper, executor, orderer);
        final int startingSegmentNumber = 0;
        storeHelper.createZNodeIfNotExist("/store/scope").join();
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        OperationContext context = new TestOperationContext();
        stream.create(configuration1, System.currentTimeMillis(), startingSegmentNumber, context).join();
        stream.updateState(State.ACTIVE, context).join();
        UUID txId = stream.generateNewTxnId(0, 0L, context).join();
        stream.createTransaction(txId, 1000L, 1000L, context).join();

        String activeTxPath = stream.getActiveTxPath(0, txId.toString());
        // throw DataNotFoundException for txn path
        doReturn(Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "txn data not found")))
                .when(storeHelper).getData(eq(activeTxPath), any());

        Map<UUID, ActiveTxnRecord> result = stream.getActiveTxns(context).join();
        // verify that call succeeds and no active txns were found
        assertTrue(result.isEmpty());

        // throw generic exception for txn path
        doReturn(Futures.failedFuture(new RuntimeException())).when(storeHelper).getData(eq(activeTxPath), any());

        ZKStream stream2 = new ZKStream("scope", "stream", storeHelper, executor, orderer);
        // verify that the call fails
        AssertExtensions.assertFutureThrows("", stream2.getActiveTxns(context), 
                e -> Exceptions.unwrap(e) instanceof RuntimeException);

        reset(storeHelper);
        ZKStream stream3 = new ZKStream("scope", "stream", storeHelper, executor, orderer);
        result = stream3.getActiveTxns(context).join();
        assertEquals(1, result.size());
    }

    @Test(timeout = 30000)
    public void testStreamRecreation() {
        // We will first create stream. Verify that its metadata is present in the cache.  
        ZKStoreHelper storeHelper = new ZKStoreHelper(cli, executor);
        ZkOrderedStore orderer = new ZkOrderedStore("txn", storeHelper, executor);

        String scope = "scope";
        String stream1 = "streamToDelete";
        ZKStream stream = new ZKStream(scope, stream1, storeHelper, executor, orderer);
        final int startingSegmentNumber = 0;
        storeHelper.createZNodeIfNotExist("/store/scope").join();
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        OperationContext context = new TestOperationContext();
        stream.create(configuration1, System.currentTimeMillis(), startingSegmentNumber, context).join();
        stream.createStreamPositionNodeIfAbsent(0).join();
        stream.updateState(State.ACTIVE, context).join();
        Long creationTime = stream.getCreationTime(context).join();
        Integer position = stream.getStreamPosition().join();
        assertEquals(0, position.intValue());
        ZKStoreHelper.ZkCacheKey<Integer> key = new ZKStoreHelper.ZkCacheKey<>(stream.getCreationPath(),
                position.toString(), x -> BitConverter.readInt(x, 0));
        VersionedMetadata<?> cachedCreationTime = storeHelper.getCache().getCachedData(key);
        // verify that both timestamps are same
        assertEquals(creationTime, cachedCreationTime.getObject());
        // delete stream.
        stream.updateState(State.SEALING, context).join();
        stream.updateState(State.SEALED, context).join();
        stream.deleteStream(context).join();

        // refresh the stream object to indicate new request context
        stream.refresh();
        AssertExtensions.assertFutureThrows("should throw data not found for stream", stream.getEpochRecord(0, context), 
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
        
        // refresh the stream object to indicate new request context
        stream.refresh();

        // verify that metadata doesn't exist in the store.
        AssertExtensions.assertFutureThrows("Stream deleted", stream.getCreationTime(context), 
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
        
        // verify that cached entries still exist. 
        VersionedMetadata<?> cachedCreationTimeExists = storeHelper.getCache().getCachedData(key);
        assertEquals(cachedCreationTime.getObject(), cachedCreationTimeExists.getObject());
        
        // create stream again.
        stream.create(configuration1, System.currentTimeMillis(), startingSegmentNumber, context).join();
        stream.createStreamPositionNodeIfAbsent(1).join();
        stream.updateState(State.ACTIVE, context).join();

        Long creationTimeNew = stream.getCreationTime(context).join();
        Integer positionNew = stream.getStreamPosition().join();
        assertEquals(1, positionNew.intValue());

        ZKStoreHelper.ZkCacheKey<Integer> keyNew = new ZKStoreHelper.ZkCacheKey<>(stream.getCreationPath(), 
                positionNew.toString(), x -> BitConverter.readInt(x, 0));
        VersionedMetadata<?> cachedCreationTimeNew = storeHelper.getCache().getCachedData(keyNew);
        // verify that both times are different
        assertNotEquals(creationTime, creationTimeNew);
        assertNotEquals(cachedCreationTime.getObject(), cachedCreationTimeNew.getObject());
    }
    
    private void testCommitFailure(StreamMetadataStore store, String scope, String stream, int epoch, UUID txnId,
                                   OperationContext context,
                                   Predicate<Throwable> checker) {
        AssertExtensions.assertSuppliedFutureThrows("Seal txn to commit it failure",
                () -> store.sealTransaction(scope, stream, txnId, true, Optional.empty(), "", Long.MIN_VALUE, context, executor),
                checker);

        AssertExtensions.assertSuppliedFutureThrows("Commit txn failure",
                () -> ((AbstractStreamMetadataStore) store).commitTransaction(scope, stream, txnId, context, executor),
                checker);
    }

    private void testAbortFailure(StreamMetadataStore store, String scope, String stream, int epoch, UUID txnId,
                                  OperationContext context,
                                  Predicate<Throwable> checker) {
        AssertExtensions.assertSuppliedFutureThrows("Seal txn to abort it failure",
                () -> store.sealTransaction(scope, stream, txnId, false, Optional.empty(), "", Long.MIN_VALUE, context, executor),
                checker);

        AssertExtensions.assertSuppliedFutureThrows("Abort txn failure",
                () -> store.abortTransaction(scope, stream, txnId, context, executor),
                checker);
    }
}
