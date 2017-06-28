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

import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.ExceptionHelpers;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Stream metadata test.
 */
public abstract class StreamMetadataStoreTest {

    //Ensure each test completes within 10 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    protected StreamMetadataStore store;
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    protected final String scope = "scope";
    protected final String stream1 = "stream1";
    protected final String stream2 = "stream2";
    protected final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
    protected final ScalingPolicy policy2 = ScalingPolicy.fixed(3);
    protected final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(scope).streamName(stream1).scalingPolicy(policy1).build();
    protected final StreamConfiguration configuration2 = StreamConfiguration.builder().scope(scope).streamName(stream2).scalingPolicy(policy2).build();

    @Before
    public abstract void setupTaskStore() throws Exception;

    @After
    public abstract void cleanupTaskStore() throws IOException;

    @After
    public void tearDown() {
        executor.shutdown();
    }

    @Test
    public void testStreamMetadataStore() throws InterruptedException, ExecutionException {

        // region createStream
        store.createScope(scope).get();

        long start = System.currentTimeMillis();
        store.createStream(scope, stream1, configuration1, start, null, executor).get();
        store.setState(scope, stream1, State.ACTIVE, null, executor).get();
        store.createStream(scope, stream2, configuration2, start, null, executor).get();
        store.setState(scope, stream2, State.ACTIVE, null, executor).get();

        assertEquals(stream1, store.getConfiguration(scope, stream1, null, executor).get().getStreamName());
        // endregion

        // region checkSegments
        List<Segment> segments = store.getActiveSegments(scope, stream1, null, executor).get();
        assertEquals(2, segments.size());

        List<Integer> historicalSegments = store.getActiveSegments(scope, stream1, 10L, null, executor).get();
        assertEquals(2, historicalSegments.size());

        segments = store.getActiveSegments(scope, stream2, null, executor).get();
        assertEquals(3, segments.size());

        historicalSegments = store.getActiveSegments(scope, stream2, 10L, null, executor).get();
        assertEquals(3, historicalSegments.size());

        // endregion

        // region scaleSegments
        long scaleTs = System.currentTimeMillis();
        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        List<Integer> sealedSegments = Collections.singletonList(1);
        StartScaleResponse response = store.startScale(scope, stream1, sealedSegments, Arrays.asList(segment1, segment2), scaleTs, false, null, executor).join();
        List<Segment> segmentsCreated = response.getSegmentsCreated();
        store.scaleNewSegmentsCreated(scope, stream1, sealedSegments, segmentsCreated, response.getActiveEpoch(), scaleTs, null, executor).join();
        store.scaleSegmentsSealed(scope, stream1, sealedSegments, segmentsCreated, response.getActiveEpoch(), scaleTs, null, executor).join();

        segments = store.getActiveSegments(scope, stream1, null, executor).get();
        assertEquals(3, segments.size());

        historicalSegments = store.getActiveSegments(scope, stream1, System.currentTimeMillis(), null, executor).get();
        assertEquals(3, historicalSegments.size());

        historicalSegments = store.getActiveSegments(scope, stream1, scaleTs - 1, null, executor).get();
        assertEquals(2, historicalSegments.size());

        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.75, 1.0);
        sealedSegments = Arrays.asList(0, 1, 2);
        long scaleTs2 = System.currentTimeMillis();
        response = store.startScale(scope, stream2, sealedSegments, Arrays.asList(segment3, segment4, segment5), scaleTs2, false, null, executor).get();
        segmentsCreated = response.getSegmentsCreated();

        store.scaleNewSegmentsCreated(scope, stream2, sealedSegments, segmentsCreated, response.getActiveEpoch(), scaleTs2, null, executor).get();
        store.scaleSegmentsSealed(scope, stream2, sealedSegments, segmentsCreated, response.getActiveEpoch(), scaleTs2, null, executor).get();

        segments = store.getActiveSegments(scope, stream1, null, executor).get();
        assertEquals(3, segments.size());

        historicalSegments = store.getActiveSegments(scope, stream2, System.currentTimeMillis(), null, executor).get();
        assertEquals(3, historicalSegments.size());

        // endregion

        // region seal stream

        assertFalse(store.isSealed(scope, stream1, null, executor).get());
        assertNotEquals(0, store.getActiveSegments(scope, stream1, null, executor).get().size());
        Boolean sealOperationStatus = store.setSealed(scope, stream1, null, executor).get();
        assertTrue(sealOperationStatus);
        assertTrue(store.isSealed(scope, stream1, null, executor).get());
        assertEquals(0, store.getActiveSegments(scope, stream1, null, executor).get().size());

        //Sealing an already seal stream should return success.
        Boolean sealOperationStatus1 = store.setSealed(scope, stream1, null, executor).get();
        assertTrue(sealOperationStatus1);
        assertTrue(store.isSealed(scope, stream1, null, executor).get());
        assertEquals(0, store.getActiveSegments(scope, stream1, null, executor).get().size());

        // seal a non-existent stream.
        try {
            store.setSealed(scope, "streamNonExistent", null, executor).join();
        } catch (CompletionException e) {
            assertEquals(StoreException.DataNotFoundException.class, e.getCause().getClass());
        }
        // endregion

        // region delete scope and stream
        assertEquals(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY, store.deleteScope(scope).join().getStatus());

        // Deleting a stream should succeed.
        assertNull(store.deleteStream(scope, stream1, null, executor).join());

        // Delete a deleted stream, should fail with node not found error.
        AssertExtensions.assertThrows("Should throw StoreException",
                store.deleteStream(scope, stream1, null, executor),
                (Throwable t) -> checkStoreExceptionType(t, StoreException.Type.DATA_NOT_FOUND));

        // Delete other stream from the scope.
        assertNull(store.deleteStream(scope, stream2, null, executor).join());

        // Delete scope should succeed now.
        assertEquals(DeleteScopeStatus.Status.SUCCESS, store.deleteScope(scope).join().getStatus());

        // Deleting deleted scope should return Scope_Not_Found.
        assertEquals(DeleteScopeStatus.Status.SCOPE_NOT_FOUND, store.deleteScope(scope).join().getStatus());

        // Deleting non-existing stream should return null.
        AssertExtensions.assertThrows("Should throw StoreException",
                store.deleteStream(scope, "nonExistent", null, executor),
                (Throwable t) -> checkStoreExceptionType(t, StoreException.Type.DATA_NOT_FOUND));
        // endregion
    }

    @Test
    public void listStreamsInScope() throws Exception {
        // list stream in scope
        store.createScope("Scope").get();
        store.createStream("Scope", stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState("Scope", stream1, State.ACTIVE, null, executor).get();
        store.createStream("Scope", stream2, configuration2, System.currentTimeMillis(), null, executor).get();
        store.setState("Scope", stream2, State.ACTIVE, null, executor).get();
        List<StreamConfiguration> streamInScope = store.listStreamsInScope("Scope").get();
        assertEquals("List streams in scope", 2, streamInScope.size());
        assertEquals("List streams in scope", stream1, streamInScope.get(0).getStreamName());
        assertEquals("List streams in scope", stream2, streamInScope.get(1).getStreamName());

        // List streams in non-existent scope 'Scope1'
        try {
            store.listStreamsInScope("Scope1").join();
        } catch (StoreException se) {
            assertTrue("List streams in non-existent scope Scope1",
                    se.getType() == StoreException.Type.DATA_NOT_FOUND);
        } catch (CompletionException ce) {
            checkStoreExceptionType(ce.getCause(), StoreException.Type.DATA_NOT_FOUND);
        }
    }

    @Test
    public void listScopes() throws Exception {
        // list scopes test
        List<String> list = store.listScopes().get();
        assertEquals("List Scopes size", 0, list.size());

        store.createScope("Scope1").get();
        store.createScope("Scope2").get();
        store.createScope("Scope3").get();
        store.createScope("Scope4").get();

        list = store.listScopes().get();
        assertEquals("List Scopes size", 4, list.size());

        store.deleteScope("Scope1").get();
        store.deleteScope("Scope2").get();
        list = store.listScopes().get();
        assertEquals("List Scopes size", 2, list.size());
    }

    @Test
    public void getScopeTest() throws Exception {
        final String scope1 = "Scope1";
        final String scope2 = "Scope2";
        String scopeName;

        // get existent scope
        store.createScope(scope1).get();
        scopeName = store.getScopeConfiguration(scope1).get();
        assertEquals("Get existent scope", scope1, scopeName);

        // get non-existent scope
        AssertExtensions.assertThrows("Should throw StoreException",
                store.getScopeConfiguration(scope2),
                (Throwable t) -> checkStoreExceptionType(t, StoreException.Type.DATA_NOT_FOUND));
    }

    @Test
    public void txnHostIndexTest() {
        String host1 = "host1";
        String host2 = "host2";

        TxnResource txn1 = new TxnResource(scope, stream1, UUID.randomUUID());
        TxnResource txn2 = new TxnResource(scope, stream1, UUID.randomUUID());

        addTxnToHost(host1, txn1, 0);
        Assert.assertEquals(1, store.listHostsOwningTxn().join().size());
        Optional<TxnResource> txn = store.getRandomTxnFromIndex(host1).join();
        Assert.assertTrue(txn.isPresent());
        Assert.assertEquals(txn1.getTxnId().toString(), txn.get().getTxnId().toString());

        // Adding a txn again should not fail.
        addTxnToHost(host1, txn1, 0);
        addTxnToHost(host1, txn2, 5);
        Assert.assertEquals(1, store.listHostsOwningTxn().join().size());

        // Fetching version of txn not existing in the index should return null.
        Assert.assertNull(store.getTxnVersionFromIndex(host1, new TxnResource(scope, stream1, UUID.randomUUID())).join());

        txn = store.getRandomTxnFromIndex(host1).join();
        Assert.assertTrue(txn.isPresent());
        UUID randomTxnId = txn.get().getTxnId();
        Assert.assertTrue(randomTxnId.equals(txn1.getTxnId()) || randomTxnId.equals(txn2.getTxnId()));
        Assert.assertEquals(scope, txn.get().getScope());
        Assert.assertEquals(stream1, txn.get().getStream());

        // Test remove txn from index.
        store.removeTxnFromIndex(host1, txn1, true).join();
        // Test remove is idempotent operation.
        store.removeTxnFromIndex(host1, txn1, true).join();
        // Test remove last txn from the index.
        store.removeTxnFromIndex(host1, txn2, false).join();
        Assert.assertEquals(1, store.listHostsOwningTxn().join().size());
        // Test remove is idempotent operation.
        store.removeTxnFromIndex(host1, txn2, true).join();
        Assert.assertEquals(0, store.listHostsOwningTxn().join().size());
        // Test removal of txn that was never added.
        store.removeTxnFromIndex(host1, new TxnResource(scope, stream1, UUID.randomUUID()), true).join();

        // Test host removal.
        store.removeHostFromIndex(host1).join();
        Assert.assertEquals(0, store.listHostsOwningTxn().join().size());
        // Test host removal is idempotent.
        store.removeHostFromIndex(host1).join();
        Assert.assertEquals(0, store.listHostsOwningTxn().join().size());
        // Test removal of host that was never added.
        store.removeHostFromIndex(host2).join();
        Assert.assertEquals(0, store.listHostsOwningTxn().join().size());
    }

    private void addTxnToHost(String host, TxnResource txnResource, int version) {
        store.addTxnToIndex(host, txnResource, version).join();
        Assert.assertEquals(version, store.getTxnVersionFromIndex(host, txnResource).join().intValue());
    }

    @Test
    public void scaleTest() throws Exception {
        final String scope = "ScopeScale";
        final String stream = "StreamScale";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        // region idempotent

        long scaleTs = System.currentTimeMillis();
        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        List<Integer> scale1SealedSegments = Collections.singletonList(1);

        // test run only if started
        AssertExtensions.assertThrows("", () ->
                store.startScale(scope, stream, scale1SealedSegments,
                        Arrays.asList(segment1, segment2), scaleTs, true, null, executor).join(),
                e -> ExceptionHelpers.getRealException(e) instanceof ScaleOperationExceptions.ScaleStartException);

        // 1. start scale
        StartScaleResponse response = store.startScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, false, null, executor).join();
        final List<Segment> scale1SegmentsCreated = response.getSegmentsCreated();
        final int scale1ActiveEpoch = response.getActiveEpoch();

        // rerun start scale
        response = store.startScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, false, null, executor).join();
        assertEquals(response.getSegmentsCreated(), scale1SegmentsCreated);

        // 2. scale new segments created
        store.scaleNewSegmentsCreated(scope, stream, scale1SealedSegments, scale1SegmentsCreated,
                response.getActiveEpoch(), scaleTs, null, executor).join();

        // rerun start scale and new segments created
        response = store.startScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, false, null, executor).join();
        assertEquals(response.getSegmentsCreated(), scale1SegmentsCreated);

        store.scaleNewSegmentsCreated(scope, stream, scale1SealedSegments, scale1SegmentsCreated,
                response.getActiveEpoch(), scaleTs, null, executor).join();

        // 3. scale segments sealed -- this will complete scale
        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments, scale1SegmentsCreated, response.getActiveEpoch(), scaleTs, null, executor).join();

        // rerun -- illegal state exception
        AssertExtensions.assertThrows("", () ->
                        store.scaleNewSegmentsCreated(scope, stream, scale1SealedSegments, scale1SegmentsCreated,
                                scale1ActiveEpoch, scaleTs, null, executor).join(),
                e -> ExceptionHelpers.getRealException(e) instanceof IllegalStateException);

        // rerun  -- illegal state exception
        AssertExtensions.assertThrows("", () ->
                        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments, scale1SegmentsCreated,
                                scale1ActiveEpoch, scaleTs, null, executor).join(),
                e -> ExceptionHelpers.getRealException(e) instanceof IllegalStateException);

        // rerun start scale -- should fail with precondition failure
        AssertExtensions.assertThrows("", () ->
                store.startScale(scope, stream, scale1SealedSegments,
                        Arrays.asList(segment1, segment2), scaleTs, false, null, executor).join(),
                e -> ExceptionHelpers.getRealException(e) instanceof ScaleOperationExceptions.ScalePreConditionFailureException);

        // endregion

        // 2 different conflicting scale operations
        // region run concurrent conflicting scale
        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.75, 1.0);
        List<Integer> scale2SealedSegments = Arrays.asList(0, 2, 3);
        long scaleTs2 = System.currentTimeMillis();
        response = store.startScale(scope, stream, scale2SealedSegments, Arrays.asList(segment3, segment4, segment5), scaleTs2, false, null, executor).get();
        final List<Segment> scale2SegmentsCreated = response.getSegmentsCreated();
        final int scale2ActiveEpoch = response.getActiveEpoch();

        // rerun of scale 1 -- should fail with precondition failure
        AssertExtensions.assertThrows("", () ->
                store.startScale(scope, stream, scale1SealedSegments,
                        Arrays.asList(segment1, segment2), scaleTs, false, null, executor).join(),
                e -> ExceptionHelpers.getRealException(e) instanceof ScaleOperationExceptions.ScaleStartException);

        // rerun of scale 1's new segments created method
        AssertExtensions.assertThrows("", () ->
                store.scaleNewSegmentsCreated(scope, stream, scale1SealedSegments, scale1SegmentsCreated,
                        scale1ActiveEpoch, scaleTs, null, executor).join(),
                e -> ExceptionHelpers.getRealException(e) instanceof ScaleOperationExceptions.ScaleConditionInvalidException);

        store.scaleNewSegmentsCreated(scope, stream, scale2SealedSegments, scale2SegmentsCreated, scale2ActiveEpoch, scaleTs2, null, executor).get();

        // rerun of scale 1's new segments created method
        AssertExtensions.assertThrows("", () ->
                store.scaleNewSegmentsCreated(scope, stream, scale1SealedSegments, scale1SegmentsCreated,
                        scale1ActiveEpoch, scaleTs, null, executor).join(),
                e -> ExceptionHelpers.getRealException(e) instanceof ScaleOperationExceptions.ScaleConditionInvalidException);

        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments, scale1SegmentsCreated, scale2ActiveEpoch, scaleTs2, null, executor).get();

        store.setState(scope, stream, State.SCALING, null, executor).get();

        // rerun of scale 1's new segments created method
        AssertExtensions.assertThrows("", () ->
                store.scaleNewSegmentsCreated(scope, stream, scale1SealedSegments, scale1SegmentsCreated,
                        scale1ActiveEpoch, scaleTs, null, executor).join(),
                e -> ExceptionHelpers.getRealException(e) instanceof ScaleOperationExceptions.ScaleConditionInvalidException);
        store.setState(scope, stream, State.ACTIVE, null, executor).get();
        // endregion
    }

    @Test
    public void scaleWithTxTest() throws Exception {
        final String scope = "ScopeScaleWithTx";
        final String stream = "StreamScaleWithTx";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        long scaleTs = System.currentTimeMillis();
        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        List<Integer> scale1SealedSegments = Collections.singletonList(1);

        // scale with transaction test
        VersionedTransactionData tx1 = store.createTransaction(scope, stream, UUID.randomUUID(),
                100, 100, 100, null, executor).get();
        assertEquals(0, tx1.getEpoch());
        StartScaleResponse response = store.startScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, false, null, executor).join();
        final List<Segment> scale1SegmentsCreated = response.getSegmentsCreated();
        final int epoch = response.getActiveEpoch();
        assertEquals(0, epoch);
        assertNotNull(scale1SealedSegments);

        // assert that txn is created on old epoch
        VersionedTransactionData tx2 = store.createTransaction(scope, stream, UUID.randomUUID(),
                100, 100, 100, null, executor).get();
        assertEquals(0, tx2.getEpoch());

        store.scaleNewSegmentsCreated(scope, stream, scale1SealedSegments, scale1SegmentsCreated,
                response.getActiveEpoch(), scaleTs, null, executor).join();

        VersionedTransactionData tx3 = store.createTransaction(scope, stream, UUID.randomUUID(),
                100, 100, 100, null, executor).get();
        assertEquals(1, tx3.getEpoch());

        DeleteEpochResponse deleteResponse = store.tryDeleteEpochIfScaling(scope, stream, 0, null, executor).get(); // should not delete epoch
        assertEquals(false, deleteResponse.isDeleted());
        assertEquals(null, deleteResponse.getSegmentsCreated());
        assertEquals(null, deleteResponse.getSegmentsSealed());

        store.sealTransaction(scope, stream, tx2.getId(), true, Optional.of(tx2.getVersion()), null, executor).get();
        store.commitTransaction(scope, stream, tx2.getEpoch(), tx2.getId(), null, executor).get(); // should not happen

        deleteResponse = store.tryDeleteEpochIfScaling(scope, stream, 0, null, executor).get(); // should not delete epoch
        assertEquals(false, deleteResponse.isDeleted());

        store.sealTransaction(scope, stream, tx1.getId(), true, Optional.of(tx1.getVersion()), null, executor).get();
        store.commitTransaction(scope, stream, tx1.getEpoch(), tx1.getId(), null, executor).get(); // should not happen
        deleteResponse = store.tryDeleteEpochIfScaling(scope, stream, 0, null, executor).get(); // should not delete epoch
        assertEquals(true, deleteResponse.isDeleted());

        store.sealTransaction(scope, stream, tx3.getId(), true, Optional.of(tx3.getVersion()), null, executor).get();
        store.commitTransaction(scope, stream, tx3.getEpoch(), tx3.getId(), null, executor).get(); // should not happen

        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments, scale1SegmentsCreated, response.getActiveEpoch(), scaleTs, null, executor).join();

        deleteResponse = store.tryDeleteEpochIfScaling(scope, stream, 1, null, executor).get(); // should not delete epoch
        assertEquals(false, deleteResponse.isDeleted());
    }

    private boolean checkStoreExceptionType(Throwable t, StoreException.Type type) {
        return t instanceof StoreException && ((StoreException) t).getType() == type;
    }
}

