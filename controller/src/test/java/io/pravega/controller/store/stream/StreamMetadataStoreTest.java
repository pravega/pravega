/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.RecordHelper;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamCutReferenceRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.shared.NameUtils.computeSegmentId;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

/**
 * Stream metadata test.
 */
public abstract class StreamMetadataStoreTest {

    //Ensure each test completes within 10 seconds.
    @Rule 
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);
    protected StreamMetadataStore store;
    protected BucketStore bucketStore;
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    protected final String scope = "scope";
    protected final String stream1 = "stream1";
    protected final String stream2 = "stream2";
    protected final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
    protected final ScalingPolicy policy2 = ScalingPolicy.fixed(3);
    protected final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
    protected final StreamConfiguration configuration2 = StreamConfiguration.builder().scalingPolicy(policy2).build();

    @Before
    public abstract void setupStore() throws Exception;

    @After
    public abstract void cleanupStore() throws Exception;

    @After
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(executor);
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

        assertEquals(configuration1, store.getConfiguration(scope, stream1, null, executor).get());
        // endregion

        // region checkSegments
        List<StreamSegmentRecord> segments = store.getActiveSegments(scope, stream1, null, executor).get();
        assertEquals(2, segments.size());

        Map<StreamSegmentRecord, Long> historicalSegments = store.getSegmentsAtHead(scope, stream1, null, executor).get();
        assertEquals(2, historicalSegments.size());

        segments = store.getActiveSegments(scope, stream2, null, executor).get();
        assertEquals(3, segments.size());

        historicalSegments = store.getSegmentsAtHead(scope, stream2, null, executor).get();
        assertEquals(3, historicalSegments.size());

        // endregion

        // region scaleSegments
        long scaleTs = System.currentTimeMillis();
        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        List<Long> sealedSegments = Collections.singletonList(1L);
        VersionedMetadata<EpochTransitionRecord> response = store.submitScale(scope, stream1, sealedSegments, Arrays.asList(segment1, segment2), scaleTs, null, null, executor).join();
        VersionedMetadata<State> state = store.getVersionedState(scope, stream1, null, executor).join();
        state = store.updateVersionedState(scope, stream1, State.SCALING, state, null, executor).get();
        response = store.startScale(scope, stream1, false, response, state, null, executor).join();
        store.scaleCreateNewEpochs(scope, stream1, response, null, executor).join();
        store.scaleSegmentsSealed(scope, stream1, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), response,
                null, executor).join();
        store.completeScale(scope, stream1, response, null, executor).join();
        store.setState(scope, stream1, State.ACTIVE, null, executor).join();

        segments = store.getActiveSegments(scope, stream1, null, executor).get();
        assertEquals(3, segments.size());
        
        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.75, 1.0);
        sealedSegments = Arrays.asList(0L, 1L, 2L);
        long scaleTs2 = System.currentTimeMillis();
        response = store.submitScale(scope, stream2, sealedSegments, Arrays.asList(segment3, segment4, segment5), scaleTs2, null, null, executor).get();
        store.setState(scope, stream2, State.SCALING, null, executor).join();
        store.scaleCreateNewEpochs(scope, stream2, response, null, executor).get();
        store.scaleSegmentsSealed(scope, stream2, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), response,
                null, executor).get();
        store.setState(scope, stream2, State.ACTIVE, null, executor).join();

        segments = store.getActiveSegments(scope, stream1, null, executor).get();
        assertEquals(3, segments.size());

        // endregion

        // region seal stream

        assertFalse(store.isSealed(scope, stream1, null, executor).get());
        assertNotEquals(0, store.getActiveSegments(scope, stream1, null, executor).get().size());
        store.setSealed(scope, stream1, null, executor).get();
        assertTrue(store.isSealed(scope, stream1, null, executor).get());
        assertEquals(0, store.getActiveSegments(scope, stream1, null, executor).get().size());

        //Sealing an already seal stream should return success.
        store.setSealed(scope, stream1, null, executor).get();
        assertTrue(store.isSealed(scope, stream1, null, executor).get());
        assertEquals(0, store.getActiveSegments(scope, stream1, null, executor).get().size());

        // seal a non-existent stream.
        AssertExtensions.assertFutureThrows("", store.setSealed(scope, "streamNonExistent", null, executor),
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
        // endregion

        // region delete scope and stream
        assertEquals(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY, store.deleteScope(scope).join().getStatus());

        // Deleting a stream should succeed.
        assertNull(store.deleteStream(scope, stream1, null, executor).join());

        // Delete a deleted stream, should fail with node not found error.
        AssertExtensions.assertFutureThrows("Should throw StoreException",
                store.deleteStream(scope, stream1, null, executor),
                t -> Exceptions.unwrap(t) instanceof StoreException.DataNotFoundException);

        // Delete other stream from the scope.
        assertNull(store.deleteStream(scope, stream2, null, executor).join());

        // Delete scope should succeed now.
        assertEquals(DeleteScopeStatus.Status.SUCCESS, store.deleteScope(scope).join().getStatus());

        // Deleting deleted scope should return Scope_Not_Found.
        assertEquals(DeleteScopeStatus.Status.SCOPE_NOT_FOUND, store.deleteScope(scope).join().getStatus());

        // Deleting non-existing stream should return null.
        AssertExtensions.assertFutureThrows("Should throw StoreException",
                store.deleteStream(scope, "nonExistent", null, executor),
                (Throwable t) -> t instanceof StoreException.DataNotFoundException);
        // endregion
    }

    @Test
    public void listScopesPaginated() throws Exception {
        // list stream in scope
        String scope = "scopeList";
        store.createScope(scope + "1").get();
        store.createScope(scope + "2").get();
        store.createScope(scope + "3").get();

        Pair<List<String>, String> scopes = store.listScopes("", 2, executor).get();
        assertEquals("List scopes", 2, scopes.getKey().size());
        assertFalse(Strings.isNullOrEmpty(scopes.getValue()));

        scopes = store.listScopes(scopes.getValue(), 2, executor).get();
        assertEquals("List scopes", 1, scopes.getKey().size());
        assertFalse(Strings.isNullOrEmpty(scopes.getValue()));

        scopes = store.listScopes(scopes.getValue(), 2, executor).get();
        assertEquals("List scopes", 0, scopes.getKey().size());
        assertFalse(Strings.isNullOrEmpty(scopes.getValue()));
    }

    @Test
    public void listStreamsInScope() throws Exception {
        // list stream in scope
        store.createScope("Scope").get();
        store.createStream("Scope", stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState("Scope", stream1, State.ACTIVE, null, executor).get();
        store.createStream("Scope", stream2, configuration2, System.currentTimeMillis(), null, executor).get();
        store.setState("Scope", stream2, State.ACTIVE, null, executor).get();
        Map<String, StreamConfiguration> streamInScope = store.listStreamsInScope("Scope").get();
        assertEquals("List streams in scope", 2, streamInScope.size());
        assertTrue("List streams in scope", streamInScope.containsKey(stream1));
        assertTrue("List streams in scope", streamInScope.containsKey(stream2));

        // List streams in non-existent scope 'Scope1'
        try {
            store.listStreamsInScope("Scope1").join();
        } catch (StoreException se) {
            assertTrue("List streams in non-existent scope Scope1",
                    se instanceof StoreException.DataNotFoundException);
        } catch (CompletionException ce) {
            assertTrue("List streams in non-existent scope Scope1",
                    Exceptions.unwrap(ce) instanceof StoreException.DataNotFoundException);
        }
    }

    @Test
    public void listStreamsInScopePaginated() throws Exception {
        // list stream in scope
        String scope = "scopeList";
        store.createScope(scope).get();
        String stream1 = "stream1";
        String stream2 = "stream2";
        String stream3 = "stream3";

        store.createStream(scope, stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream1, State.ACTIVE, null, executor).get();
        store.createStream(scope, stream2, configuration2, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream2, State.ACTIVE, null, executor).get();
        store.createStream(scope, stream3, configuration2, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream3, State.ACTIVE, null, executor).get();
        
        Pair<List<String>, String> streamInScope = store.listStream(scope, "", 2, executor).get();
        assertEquals("List streams in scope", 2, streamInScope.getKey().size());
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));

        streamInScope = store.listStream(scope, streamInScope.getValue(), 2, executor).get();
        assertEquals("List streams in scope", 1, streamInScope.getKey().size());
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));

        streamInScope = store.listStream(scope, streamInScope.getValue(), 2, executor).get();
        assertEquals("List streams in scope", 0, streamInScope.getKey().size());
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));
        
        store.deleteStream(scope, stream1, null, executor).join();
        
        streamInScope = store.listStream(scope, "", 2, executor).get();
        assertEquals("List streams in scope", 2, streamInScope.getKey().size());
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));

        streamInScope = store.listStream(scope, streamInScope.getValue(), 2, executor).get();
        assertEquals("List streams in scope", 0, streamInScope.getKey().size());
        assertFalse(Strings.isNullOrEmpty(streamInScope.getValue()));
    }

    @Test
    public void partialStreamsInScope() throws Exception {
        // list stream in scope
        store.createScope("Scope").get();
        store.createStream("Scope", stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState("Scope", stream1, State.ACTIVE, null, executor).get();
        String partial = "partial";
        store.createStream("Scope", partial, configuration1, System.currentTimeMillis(), null, executor).get();

        // verify that when we do list stream in scope we filter out partially created streams. 
        Map<String, StreamConfiguration> streamInScope = store.listStreamsInScope("Scope").get();
        assertEquals("List streams in scope", 1, streamInScope.size());
        assertFalse("Does not contain partial", streamInScope.containsKey(partial));

        // now deliberately throw data not found exception for getConfiguration on partial. 
        PersistentStreamBase streamObj = (PersistentStreamBase) ((AbstractStreamMetadataStore) store).getStream("Scope", partial, null);
        PersistentStreamBase streamObjSpied = spy(streamObj);

        doAnswer(x -> {
            CompletableFuture<StreamConfiguration> result = new CompletableFuture<>();
            result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "configuration"));
            return result;
        }).when(streamObjSpied).getConfiguration();

        ((AbstractStreamMetadataStore) store).setStream(streamObjSpied);

        // verify that when we do list stream in scope we do not get partial. 
        streamInScope = store.listStreamsInScope("Scope").get();
        assertEquals("List streams in scope", 1, streamInScope.size());
        assertFalse("List streams in scope", streamInScope.containsKey(partial));
        
        reset(streamObjSpied);
        // set to return unknown state
        doAnswer(x -> CompletableFuture.completedFuture(State.UNKNOWN)).when(streamObjSpied).getState(anyBoolean());
        // verify that when we do list stream in scope we do not get partial. 
        streamInScope = store.listStreamsInScope("Scope").get();
        assertEquals("List streams in scope", 1, streamInScope.size());
        assertFalse("List streams in scope", streamInScope.containsKey(partial));
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

        store.createStream("Scope3", "stream1", configuration1, System.currentTimeMillis(), null, executor).join();
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
        AssertExtensions.assertFutureThrows("Should throw StoreException",
                store.getScopeConfiguration(scope2),
                (Throwable t) -> t instanceof StoreException.DataNotFoundException);
    }

    @Test
    public void txnHostIndexTest() {
        String host1 = "host1";
        String host2 = "host2";

        TxnResource txn1 = new TxnResource(scope, stream1, UUID.randomUUID());
        TxnResource txn2 = new TxnResource(scope, stream1, UUID.randomUUID());

        addTxnToHost(host1, txn1, new Version.IntVersion(0));
        Assert.assertEquals(1, store.listHostsOwningTxn().join().size());
        Optional<TxnResource> txn = store.getRandomTxnFromIndex(host1).join();
        Assert.assertTrue(txn.isPresent());
        Assert.assertEquals(txn1.getTxnId().toString(), txn.get().getTxnId().toString());

        // Adding a txn again should not fail.
        addTxnToHost(host1, txn1, new Version.IntVersion(0));
        addTxnToHost(host1, txn2, new Version.IntVersion(5));
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
        Assert.assertTrue(store.listHostsOwningTxn().join().size() <= 1);
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

    private void addTxnToHost(String host, TxnResource txnResource, Version version) {
        store.addTxnToIndex(host, txnResource, version).join();
        Assert.assertEquals(version, store.getTxnVersionFromIndex(host, txnResource).join());
    }

    @Test
    public void scaleTest() throws Exception {
        final String scope = "ScopeScale";
        final String stream = "StreamScale";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        // set minimum number of segments to 1 so that we can also test scale downs
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        store.startUpdateConfiguration(scope, stream, config, null, executor).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = store.getConfigurationRecord(scope, stream, null, executor).join();
        store.completeUpdateConfiguration(scope, stream, configRecord, null, executor).join();

        // region idempotent

        long scaleTs = System.currentTimeMillis();
        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        List<Long> scale1SealedSegments = Collections.singletonList(computeSegmentId(1, 0));

        // 1. submit scale
        VersionedMetadata<EpochTransitionRecord> empty = store.getEpochTransition(scope, stream, null, executor).join();
        
        VersionedMetadata<EpochTransitionRecord> response = store.submitScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, null, null, executor).join();
        Map<Long, Map.Entry<Double, Double>> scale1SegmentsCreated = response.getObject().getNewSegmentsWithRange();
        final int scale1ActiveEpoch = response.getObject().getActiveEpoch();
        assertEquals(0, scale1ActiveEpoch);
        
        // rerun start scale with old epoch transition. should throw write conflict
        AssertExtensions.assertSuppliedFutureThrows("", () -> store.submitScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, empty, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        // rerun start scale with null epoch transition, should be idempotent
        response = store.submitScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, null, null, executor).join();
        assertEquals(response.getObject().getNewSegmentsWithRange(), scale1SegmentsCreated);

        VersionedMetadata<State> state = store.getVersionedState(scope, stream, null, executor).join();
        state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).get();
        response = store.startScale(scope, stream, false, response, state, null, executor).join();
        // 2. scale new segments created
        store.scaleCreateNewEpochs(scope, stream, response, null, executor).join();

        // rerun start scale and new segments created
        response = store.submitScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, null, null, executor).join();
        assertEquals(response.getObject().getNewSegmentsWithRange(), scale1SegmentsCreated);
        
        response = store.startScale(scope, stream, false, response, state, null, executor).join();
        store.scaleCreateNewEpochs(scope, stream, response, null, executor).join();

        // 3. scale segments sealed -- this will complete scale
        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), response,
                null, executor).join();
        store.completeScale(scope, stream, response, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        // rerun -- idempotent
        store.scaleCreateNewEpochs(scope, stream, response, null, executor).join();
        EpochRecord activeEpoch = store.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(1, activeEpoch.getEpoch());
        
        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), 
                response, null, executor).join();
        store.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(1, activeEpoch.getEpoch());

        // rerun submit scale -- should fail with precondition failure
        VersionedMetadata<EpochTransitionRecord> etr = store.getEpochTransition(scope, stream, null, executor).join();
        assertEquals(EpochTransitionRecord.EMPTY, empty.getObject());

        AssertExtensions.assertThrows("Submit scale with old data with old etr", () ->
                        store.submitScale(scope, stream, scale1SealedSegments,
                                Arrays.asList(segment1, segment2), scaleTs, empty, null, executor).join(),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        AssertExtensions.assertThrows("Submit scale with old data with latest etr", () ->
                        store.submitScale(scope, stream, scale1SealedSegments,
                                Arrays.asList(segment1, segment2), scaleTs, etr, null, executor).join(),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.PreConditionFailureException);

        AssertExtensions.assertThrows("Submit scale with null etr", () ->
                store.submitScale(scope, stream, scale1SealedSegments,
                        Arrays.asList(segment1, segment2), scaleTs, null, null, executor).join(),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.PreConditionFailureException);
        // endregion

        // 2 different conflicting scale operations
        // region run concurrent conflicting scale
        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.75, 1.0);
        List<Long> scale2SealedSegments = Arrays.asList(computeSegmentId(0, 0), computeSegmentId(2, 1), computeSegmentId(3, 1));
        long scaleTs2 = System.currentTimeMillis();
        response = store.submitScale(scope, stream, scale2SealedSegments, Arrays.asList(segment3, segment4, segment5), scaleTs2, null, null, executor).get();
        Map<Long, Map.Entry<Double, Double>> scale2SegmentsCreated = response.getObject().getNewSegmentsWithRange();
        final int scale2ActiveEpoch = response.getObject().getActiveEpoch();
        store.setState(scope, stream, State.SCALING, null, executor).get();

        // rerun of scale 1 -- should fail with conflict
        AssertExtensions.assertThrows("Concurrent conflicting scale", () ->
                store.submitScale(scope, stream, scale1SealedSegments,
                        Arrays.asList(segment1, segment2), scaleTs, null, null, executor).join(),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.ConflictException);

        store.scaleCreateNewEpochs(scope, stream, response, null, executor).get();

        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), response,
                null, executor).get();
        store.completeScale(scope, stream, response, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();
        // endregion

        // region concurrent submit scale requests
        // run two concurrent runScale operations such that after doing a getEpochTransition, we create a new epoch
        // transition node. We should get ScaleConflict in such a case.
        // mock createEpochTransition
        SimpleEntry<Double, Double> segment6 = new SimpleEntry<>(0.0, 1.0);
        List<Long> scale3SealedSegments = Arrays.asList(computeSegmentId(4, 2), computeSegmentId(5, 2), computeSegmentId(6, 2));
        long scaleTs3 = System.currentTimeMillis();

        @SuppressWarnings("unchecked")
        PersistentStreamBase streamObj = (PersistentStreamBase) ((AbstractStreamMetadataStore) store).getStream(scope, stream, null);
        PersistentStreamBase streamObjSpied = spy(streamObj);

        CompletableFuture<Void> latch = new CompletableFuture<>();
        CompletableFuture<Void> updateEpochTransitionCalled = new CompletableFuture<>();

        doAnswer(x -> CompletableFuture.runAsync(() -> {
            // wait until we create epoch transition outside of this method
            updateEpochTransitionCalled.complete(null);
            latch.join();
        }).thenCompose(v -> streamObj.updateEpochTransitionNode(x.getArgument(0)))).when(streamObjSpied).updateEpochTransitionNode(any());

        doAnswer(x -> streamObj.getEpochTransitionNode()).when(streamObjSpied).getEpochTransitionNode();

        ((AbstractStreamMetadataStore) store).setStream(streamObjSpied);

        // the following should be stuck at createEpochTransition
        CompletableFuture<VersionedMetadata<EpochTransitionRecord>> resp = store.submitScale(scope, stream, scale3SealedSegments, Arrays.asList(segment6), scaleTs3, null, null, executor);
        updateEpochTransitionCalled.join();
        VersionedMetadata<EpochTransitionRecord> epochRecord = streamObj.getEpochTransition().join();
        streamObj.updateEpochTransitionNode(new VersionedMetadata<>(EpochTransitionRecord.EMPTY, epochRecord.getVersion())).join();
        latch.complete(null);

        AssertExtensions.assertFutureThrows("", resp, e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);
        // endregion
    }

    @Test
    public void concurrentStartScaleTest() throws Exception {
        final String scope = "ScopeScale";
        final String stream = "StreamScale1";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();
        // set minimum number of segments to 1
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        store.startUpdateConfiguration(scope, stream, config, null, executor).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = store.getConfigurationRecord(scope, stream, null, executor).join();
        store.completeUpdateConfiguration(scope, stream, configRecord, null, executor).join();

        // region concurrent start scale
        // Test scenario where one request starts and completes as the other is waiting on StartScale.createEpochTransition
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.0, 1.0);
        List<Long> segmentsToSeal = Arrays.asList(0L, 1L);
        long scaleTs = System.currentTimeMillis();

        @SuppressWarnings("unchecked")
        PersistentStreamBase streamObj = (PersistentStreamBase) ((AbstractStreamMetadataStore) store)
                .getStream(scope, stream, null);
        PersistentStreamBase streamObjSpied = spy(streamObj);

        CompletableFuture<Void> latch = new CompletableFuture<>();
        CompletableFuture<Void> updateEpochTransitionCalled = new CompletableFuture<>();

        doAnswer(x -> streamObj.getEpochTransitionNode()).when(streamObjSpied).getEpochTransitionNode();
        doAnswer(x -> streamObj.updateEpochTransitionNode(any())).when(streamObjSpied).updateEpochTransitionNode(any());

        doAnswer(x -> CompletableFuture.runAsync(() -> {
            VersionedMetadata<EpochTransitionRecord> argument = x.getArgument(0);

            EpochTransitionRecord record = argument.getObject();

            if (record.getSegmentsToSeal().containsAll(segmentsToSeal)) {
                // wait until we create epoch transition outside of this method
                updateEpochTransitionCalled.complete(null);
                latch.join();
            }
        }).thenCompose(v -> streamObj.updateEpochTransitionNode(x.getArgument(0))))
                .when(streamObjSpied).updateEpochTransitionNode(any());

        ((AbstractStreamMetadataStore) store).setStream(streamObjSpied);

        // the following should be stuck at createEpochTransition
        CompletableFuture<VersionedMetadata<EpochTransitionRecord>> response = store.submitScale(scope, stream, segmentsToSeal,
                Arrays.asList(segment2), scaleTs, null, null, executor);
        updateEpochTransitionCalled.join();

        // create new epochs corresponding to new scale as the previous scale waits to create epoch transition record
        SimpleEntry<Double, Double> segment2p = new SimpleEntry<>(0.0, 0.5);
        List<Long> segmentsToSeal2 = Arrays.asList(0L);
        long scaleTs2 = System.currentTimeMillis();

        streamObjSpied.getEpochRecord(0)
                .thenCompose(epochRecord -> {
                            EpochTransitionRecord record = RecordHelper.computeEpochTransition(epochRecord, segmentsToSeal2,
                                    Arrays.asList(segment2p), scaleTs2);
                            return streamObjSpied.getEpochTransition()
                                .thenCompose(existing -> streamObjSpied.updateEpochTransitionNode(new VersionedMetadata<>(record, existing.getVersion())))
                                    .thenApply(v -> new VersionedMetadata<>(record, v));
                        })
                .thenCompose(epochRecord -> store.getVersionedState(scope, stream, null, executor)
                        .thenCompose(state -> store.updateVersionedState(scope, stream, State.SCALING, state, null, executor)
                                .thenCompose(updatedState -> store.startScale(scope, stream, false, epochRecord, updatedState, null, executor))
                                .thenCompose(x -> store.scaleCreateNewEpochs(scope, stream, epochRecord, null, executor))
                                .thenCompose(x -> store.scaleSegmentsSealed(scope, stream,
                                        segmentsToSeal2.stream().collect(Collectors.toMap(r -> r, r -> 0L)), epochRecord, null, executor))
                                .thenCompose(x -> store.completeScale(scope, stream, epochRecord, null, executor))))
                .thenCompose(y -> store.setState(scope, stream, State.ACTIVE, null, executor))
                .join();

        latch.complete(null);

        // first scale should fail in attempting to update epoch transition record.
        AssertExtensions.assertSuppliedFutureThrows("WriteConflict in start scale", () -> response, e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);
        VersionedMetadata<EpochTransitionRecord> versioned = streamObj.getEpochTransition().join();
        EpochTransitionRecord epochTransitionRecord = versioned.getObject();
        assertEquals(EpochTransitionRecord.EMPTY, epochTransitionRecord);
        // now that start scale succeeded, we should set the state to scaling.
        VersionedMetadata<State> state = store.getVersionedState(scope, stream, null, executor).join();
        state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).join();
        // now call first step of scaling -- createNewSegments. this should throw exception
        AssertExtensions.assertFutureThrows("epoch transition was supposed to be invalid",
                store.startScale(scope, stream, false, versioned, state, null, executor),
                e -> Exceptions.unwrap(e) instanceof IllegalStateException);
        // verify that state is reset to ACTIVE
        assertEquals(State.ACTIVE, store.getState(scope, stream, true, null, executor).join());
        // endregion
    }

    @Test
    public void updateTest() throws Exception {
        final String scope = "ScopeUpdate";
        final String stream = "StreamUpdate";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        final StreamConfiguration configuration2 = StreamConfiguration.builder().scalingPolicy(policy).build();

        StreamConfigurationRecord configProperty = store.getConfigurationRecord(scope, stream, null, executor).join().getObject();
        assertFalse(configProperty.isUpdating());
        // run update configuration multiple times
        assertTrue(Futures.await(store.startUpdateConfiguration(scope, stream, configuration2, null, executor)));
        store.setState(scope, stream, State.UPDATING, null, executor).join();
        configProperty = store.getConfigurationRecord(scope, stream, null, executor).join().getObject();

        assertTrue(configProperty.isUpdating());

        final StreamConfiguration configuration3 = StreamConfiguration.builder().scalingPolicy(policy).build();

        assertFalse(Futures.await(store.startUpdateConfiguration(scope, stream, configuration3, null, executor)));

        VersionedMetadata<StreamConfigurationRecord> existing = store.getConfigurationRecord(scope, stream, null, executor).join();
        assertTrue(Futures.await(store.completeUpdateConfiguration(scope, stream, existing, null, executor)));

        configProperty = store.getConfigurationRecord(scope, stream, null, executor).join().getObject();
        assertEquals(configuration2, configProperty.getStreamConfiguration());

        assertTrue(Futures.await(store.startUpdateConfiguration(scope, stream, configuration3, null, executor)));
        existing = store.getConfigurationRecord(scope, stream, null, executor).join();
        assertTrue(Futures.await(store.completeUpdateConfiguration(scope, stream, existing, null, executor)));
        store.setState(scope, stream, State.ACTIVE, null, executor).join();
    }

    @Test
    public void deleteTest() throws Exception {
        final String scope = "ScopeDelete";
        final String stream = "StreamDelete";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();
        assertTrue(store.checkStreamExists(scope, stream).join());

        store.deleteStream(scope, stream, null, executor).get();
        assertFalse(store.checkStreamExists(scope, stream).join());
        DeleteScopeStatus status = store.deleteScope(scope).join();
        assertEquals(status.getStatus(), DeleteScopeStatus.Status.SUCCESS);
    }

    @Test
    public void scaleWithTxTest() throws Exception {
        final String scope = "ScopeScaleWithTx";
        final String stream = "StreamScaleWithTx";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        long scaleTs = System.currentTimeMillis();
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.75, 1.0);
        List<Long> scale1SealedSegments = Collections.singletonList(1L);

        // region Txn created before scale and during scale
        // scale with transaction test
        // first txn created before-scale
        UUID txnId = store.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData tx01 = store.createTransaction(scope, stream, txnId,
                100, 100, null, executor).get();
        assertEquals(0, tx01.getEpoch());
        VersionedMetadata<EpochTransitionRecord> versioned = store.submitScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment2, segment3), scaleTs, null, null, executor).join();
        EpochTransitionRecord response = versioned.getObject();
        Map<Long, Map.Entry<Double, Double>> scale1SegmentsCreated = response.getNewSegmentsWithRange();
        final int epoch = response.getActiveEpoch();
        assertEquals(0, epoch);
        assertNotNull(scale1SegmentsCreated);
        VersionedMetadata<State> state = store.getVersionedState(scope, stream, null, executor).join();
        state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).join();
        versioned = store.startScale(scope, stream, false, versioned, state, null, executor).join();
        // second txn created after new segments are created in segment table but not yet in history table
        // assert that txn is created on old epoch
        store.scaleCreateNewEpochs(scope, stream, versioned, null, executor).join();
        txnId = store.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData tx02 = store.createTransaction(scope, stream, txnId,
                100, 100, null, executor).get();
        assertEquals(0, tx02.getEpoch());
        assertEquals(0, (int) (tx02.getId().getMostSignificantBits() >> 32));

        // third transaction created after new epoch created
        txnId = store.generateTransactionId(scope, stream, null, executor).join();

        store.sealTransaction(scope, stream, tx02.getId(), true, Optional.of(tx02.getVersion()), "", Long.MIN_VALUE, null, executor).get();
        store.sealTransaction(scope, stream, tx01.getId(), true, Optional.of(tx01.getVersion()), "", Long.MIN_VALUE, null, executor).get();

        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned,
                null, executor).join();
        store.completeScale(scope, stream, versioned, null, executor).join();
        VersionedTransactionData tx03 = store.createTransaction(scope, stream, txnId,
                100, 100, null, executor).get();
        assertEquals(0, tx03.getEpoch());
        assertEquals(0, (int) (tx03.getId().getMostSignificantBits() >> 32));

        store.setState(scope, stream, State.ACTIVE, null, executor).join();

        // ensure that we can commit transactions on old epoch and roll over.
        EpochRecord activeEpoch = store.getActiveEpoch(scope, stream, null, true, executor).join();

        // submit another scale request without starting the scale
        List<Long> scale2SealedSegments = Collections.singletonList(0L);
        long scaleTs2 = System.currentTimeMillis();
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.0, 0.25);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.25, 0.5);

        VersionedMetadata<EpochTransitionRecord> versioned2 = store.submitScale(scope, stream, scale2SealedSegments,
                Arrays.asList(segment4, segment5), scaleTs2, null, null, executor).join();
        EpochTransitionRecord response2 = versioned2.getObject();
        assertEquals(activeEpoch.getEpoch(), response2.getActiveEpoch());

        VersionedMetadata<CommittingTransactionsRecord> record = store.startCommitTransactions(scope, stream, 100, null, executor).join();
        store.setState(scope, stream, State.COMMITTING_TXN, null, executor).join();
        record = store.startRollingTxn(scope, stream, activeEpoch.getEpoch(), record, null, executor).join();
        store.rollingTxnCreateDuplicateEpochs(scope, stream, Collections.emptyMap(), System.currentTimeMillis(), record, null, executor).join();
        store.completeRollingTxn(scope, stream, Collections.emptyMap(), record, null, executor).join();
        store.completeCommitTransactions(scope, stream, record, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();
        activeEpoch = store.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(3, activeEpoch.getEpoch());
        assertEquals(1, activeEpoch.getReferenceEpoch());
        assertEquals(3, activeEpoch.getSegments().size());
        List<StreamSegmentRecord> txnDuplicate = store.getSegmentsInEpoch(scope, stream, 2, null, executor).join();
        assertEquals(2, txnDuplicate.size());
        List<StreamSegmentRecord> activeEpochDuplicate = store.getSegmentsInEpoch(scope, stream, 3, null, executor).join();
        assertEquals(3, activeEpochDuplicate.size());
        EpochRecord txnCommittedEpoch = store.getEpoch(scope, stream, 2, null, executor).join();
        assertEquals(0, txnCommittedEpoch.getReferenceEpoch());
        assertEquals(store.transactionStatus(scope, stream, tx01.getId(), null, executor).join(), TxnStatus.COMMITTED);
        assertEquals(store.transactionStatus(scope, stream, tx02.getId(), null, executor).join(), TxnStatus.COMMITTED);
        assertEquals(store.transactionStatus(scope, stream, tx03.getId(), null, executor).join(), TxnStatus.OPEN);
        store.sealTransaction(scope, stream, tx03.getId(), true, Optional.of(tx03.getVersion()), "", Long.MIN_VALUE, null, executor).get();
        // endregion

        // region verify migrate request for manual scale

        // now start manual scale against previously submitted scale request that was on old epoch from before rolling txn.
        // verify that it gets migrated to latest duplicate epoch
        state = store.getVersionedState(scope, stream, null, executor).join();
        state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).join();
        versioned2 = store.submitScale(scope, stream, scale2SealedSegments,
                Arrays.asList(segment4, segment5), scaleTs2, null, null, executor).join();
        versioned2 = store.startScale(scope, stream, true, versioned2, state, null, executor).join();
        store.scaleCreateNewEpochs(scope, stream, versioned2, null, executor).join();

        txnId = store.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData tx14 = store.createTransaction(scope, stream, txnId,
                100, 100, null, executor).get();
        assertEquals(1, tx14.getEpoch());

        store.sealTransaction(scope, stream, tx14.getId(), true, Optional.of(tx14.getVersion()), "", Long.MIN_VALUE, null, executor).get();

        // verify that new txns can be created and are created on original epoch
        txnId = store.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData tx15 = store.createTransaction(scope, stream, txnId,
                100, 100, null, executor).get();
        assertEquals(1, tx15.getEpoch());

        store.scaleCreateNewEpochs(scope, stream, versioned2, null, executor).join();

        store.scaleSegmentsSealed(scope, stream, Collections.emptyMap(), versioned2, null, executor).join();
        store.completeScale(scope, stream, versioned2, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();
        activeEpoch = store.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(4, activeEpoch.getEpoch());
        assertEquals(4, activeEpoch.getReferenceEpoch());

        store.sealTransaction(scope, stream, tx15.getId(), true, Optional.of(tx15.getVersion()), "", Long.MIN_VALUE, null, executor).get();

        record = store.startCommitTransactions(scope, stream, 100, null, executor).join();
        store.setState(scope, stream, State.COMMITTING_TXN, null, executor).get();
        record = store.startRollingTxn(scope, stream, activeEpoch.getEpoch(), record, null, executor).join();
        store.rollingTxnCreateDuplicateEpochs(scope, stream, Collections.emptyMap(), System.currentTimeMillis(), record, null, executor).join();
        store.completeRollingTxn(scope, stream, Collections.emptyMap(), record, null, executor).join();
        store.completeCommitTransactions(scope, stream, record, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();

        activeEpoch = store.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(6, activeEpoch.getEpoch());
        assertEquals(4, activeEpoch.getReferenceEpoch());
        // endregion
    }

    @Test
    public void scaleWithTxnForInconsistentScanerios() throws Exception {
        final String scope = "ScopeScaleWithTx";
        final String stream = "StreamScaleWithTx1";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        UUID txnId = store.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData tx1 = store.createTransaction(scope, stream, txnId,
                100, 100, null, executor).get();
        store.sealTransaction(scope, stream, txnId, true, Optional.of(tx1.getVersion()), "", Long.MIN_VALUE, null, executor).get();

        long scaleTs = System.currentTimeMillis();
        List<Long> scale1SealedSegments = Collections.singletonList(0L);

        // run a scale on segment 1
        VersionedMetadata<EpochTransitionRecord> versioned = store.submitScale(scope, stream, scale1SealedSegments,
                Arrays.asList(new AbstractMap.SimpleEntry<>(0.0, 0.25), new AbstractMap.SimpleEntry<>(0.25, 0.5)), scaleTs, null, null, executor).join();
        EpochTransitionRecord response = versioned.getObject();
        assertEquals(0, response.getActiveEpoch());

        VersionedMetadata<State> state = store.getVersionedState(scope, stream, null, executor).join();
        state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).join();
        store.startScale(scope, stream, false, versioned, state, null, executor).join();
        store.scaleCreateNewEpochs(scope, stream, versioned, null, executor).join();
        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned,
                null, executor).join();
        store.completeScale(scope, stream, versioned, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();

        // start second scale
        versioned = store.submitScale(scope, stream, Collections.singletonList(1L),
                Arrays.asList(new AbstractMap.SimpleEntry<>(0.5, 0.75), new AbstractMap.SimpleEntry<>(0.75, 1.0)), scaleTs, null, null, executor).join();
        response = versioned.getObject();
        assertEquals(1, response.getActiveEpoch());

        EpochRecord activeEpoch = store.getActiveEpoch(scope, stream, null, true, executor).join();
        VersionedMetadata<CommittingTransactionsRecord> record = store.startCommitTransactions(scope, stream, 100, null, executor).join();
        store.setState(scope, stream, State.COMMITTING_TXN, null, executor).join();
        record = store.startRollingTxn(scope, stream, activeEpoch.getEpoch(), record, null, executor).join();
        store.rollingTxnCreateDuplicateEpochs(scope, stream, Collections.emptyMap(), System.currentTimeMillis(), record, null, executor).join();
        store.completeRollingTxn(scope, stream, Collections.emptyMap(), record, null, executor).join();
        store.completeCommitTransactions(scope, stream, record, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();

        state = store.getVersionedState(scope, stream, null, executor).join();
        state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).join();
        versioned = store.submitScale(scope, stream, Collections.singletonList(1L),
                Arrays.asList(new AbstractMap.SimpleEntry<>(0.5, 0.75), new AbstractMap.SimpleEntry<>(0.75, 1.0)), scaleTs, null, null, executor).join();
        response = versioned.getObject();
        assertEquals(1, response.getActiveEpoch());
        AssertExtensions.assertFutureThrows("attempting to create new segments against inconsistent epoch transition record",
                store.startScale(scope, stream, false, versioned, state, null, executor),
                e -> Exceptions.unwrap(e) instanceof IllegalStateException);
        
        // verify that state is reset to active
        State stateVal = store.getState(scope, stream, true, null, executor).join();
        assertEquals(State.ACTIVE, stateVal);
    }

    @Test
    public void txnOrderTest() throws Exception {
        final String scope = "txnOrder";
        final String stream = "txnOrder";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        long scaleTs = System.currentTimeMillis();
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.75, 1.0);
        List<Long> scale1SealedSegments = Collections.singletonList(1L);

        // create 3 transactions on epoch 0 --> tx00, tx01, tx02.. mark first as commit, mark second as abort, 
        // keep third as open. add ordered entries for all three.. verify that they are present in ordered set.
        UUID tx00 = store.generateTransactionId(scope, stream, null, executor).join();
        store.createTransaction(scope, stream, tx00,
                100, 100, null, executor).get();
        UUID tx01 = store.generateTransactionId(scope, stream, null, executor).join();
        store.createTransaction(scope, stream, tx01,
                100, 100, null, executor).get();
        UUID tx02 = store.generateTransactionId(scope, stream, null, executor).join();
        store.createTransaction(scope, stream, tx02,
                100, 100, null, executor).get();

        // committing
        store.sealTransaction(scope, stream, tx00, true, Optional.empty(),
                "", Long.MIN_VALUE, null, executor).get();
        // aborting
        store.sealTransaction(scope, stream, tx01, false, Optional.empty(),
                "", Long.MIN_VALUE, null, executor).get();

        PersistentStreamBase streamObj = (PersistentStreamBase) ((AbstractStreamMetadataStore) store).getStream(scope, stream, null);
        // duplicate for tx00
        streamObj.addTxnToCommitOrder(tx00).join();
        // entry for aborting transaction tx01
        streamObj.addTxnToCommitOrder(tx01).join();
        // entry for open transaction tx02
        streamObj.addTxnToCommitOrder(tx02).join();

        Map<Long, UUID> positions = streamObj.getAllOrderedCommittingTxns().join();
        assertEquals(4, positions.size());
        assertEquals(positions.get(0L), tx00);
        assertEquals(positions.get(1L), tx00);
        assertEquals(positions.get(2L), tx01);
        assertEquals(positions.get(3L), tx02);
        
        // verify that when we retrieve transactions from lowest epoch we get tx00
        List<Map.Entry<UUID, ActiveTxnRecord>> orderedRecords = streamObj.getOrderedCommittingTxnInLowestEpoch(100).join();
        List<UUID> ordered = orderedRecords.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        assertEquals(1, ordered.size());
        assertEquals(tx00, ordered.get(0));

        // verify that duplicates and stale entries are purged. entries for open transaction and committing are retained
        positions = streamObj.getAllOrderedCommittingTxns().join();
        assertEquals(2, positions.size());
        assertEquals(positions.get(0L), tx00);
        assertEquals(positions.get(3L), tx02);

        // scale
        scale(scope, stream, scaleTs, Arrays.asList(segment2, segment3), scale1SealedSegments);

        // create 3 transactions on epoch 1 --> tx10, tx11, tx12.. mark first as commit, mark second as abort, 
        // keep third as open. add ordered entries for all three.. verify that they are present in ordered set.
        UUID tx10 = store.generateTransactionId(scope, stream, null, executor).join();
        store.createTransaction(scope, stream, tx10,
                100, 100, null, executor).get();
        UUID tx11 = store.generateTransactionId(scope, stream, null, executor).join();
        store.createTransaction(scope, stream, tx11,
                100, 100, null, executor).get();
        UUID tx12 = store.generateTransactionId(scope, stream, null, executor).join();
        store.createTransaction(scope, stream, tx12,
                100, 100, null, executor).get();
        // set all three transactions to committing
        store.sealTransaction(scope, stream, tx10, true, Optional.empty(),
                "", Long.MIN_VALUE, null, executor).get();
        store.sealTransaction(scope, stream, tx11, true, Optional.empty(),
                "", Long.MIN_VALUE, null, executor).get();
        store.sealTransaction(scope, stream, tx12, true, Optional.empty(),
                "", Long.MIN_VALUE, null, executor).get();
        
        // verify that we still get tx00 only 
        orderedRecords = streamObj.getOrderedCommittingTxnInLowestEpoch(100).join();
        ordered = orderedRecords.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        assertEquals(1, ordered.size());
        assertEquals(tx00, ordered.get(0));
        assertEquals(0L, orderedRecords.get(0).getValue().getCommitOrder());

        // verify that positions has 3 new entries added though
        positions = streamObj.getAllOrderedCommittingTxns().join();
        assertEquals(5, positions.size());
        assertEquals(positions.get(0L), tx00);
        assertEquals(positions.get(3L), tx02);
        assertEquals(positions.get(4L), tx10);
        assertEquals(positions.get(5L), tx11);
        assertEquals(positions.get(6L), tx12);

        VersionedMetadata<CommittingTransactionsRecord> record = store.startCommitTransactions(scope, stream, 100, null, executor).join();
        
        // verify that after including transaction tx00 in the record, we no longer keep its reference in the ordered
        positions = streamObj.getAllOrderedCommittingTxns().join();
        assertEquals(4, positions.size());
        assertFalse(positions.containsKey(0L));
        assertEquals(positions.get(3L), tx02);
        assertEquals(positions.get(4L), tx10);
        assertEquals(positions.get(5L), tx11);
        assertEquals(positions.get(6L), tx12);

        store.setState(scope, stream, State.COMMITTING_TXN, null, executor).join();
        // verify that we need to perform rolling transaction
        EpochRecord activeEpoch = store.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(0, record.getObject().getEpoch());
        assertEquals(1, activeEpoch.getEpoch());
        // also, transactions to commit match transactions in lowest epoch
        assertEquals(record.getObject().getTransactionsToCommit(), ordered);
        
        record = store.startRollingTxn(scope, stream, activeEpoch.getEpoch(), record, null, executor).join();
        store.rollingTxnCreateDuplicateEpochs(scope, stream, Collections.emptyMap(), System.currentTimeMillis(), record, null, executor).join();
        store.completeRollingTxn(scope, stream, Collections.emptyMap(), record, null, executor).join();
        store.completeCommitTransactions(scope, stream, record, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();
        
        // after committing, we should have committed tx00 while having purged references for tx01 and tx02
        // getting ordered list should return txn on epoch 1 in the order in which we issued commits
        orderedRecords = streamObj.getOrderedCommittingTxnInLowestEpoch(100).join();
        ordered = orderedRecords.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        assertEquals(3, ordered.size());
        assertEquals(tx10, ordered.get(0));
        assertEquals(tx11, ordered.get(1));
        assertEquals(tx12, ordered.get(2));

        // verify that transactions are still present in position
        positions = streamObj.getAllOrderedCommittingTxns().join();
        assertEquals(4, positions.size());
        assertEquals(positions.get(3L), tx02);
        assertEquals(positions.get(4L), tx10);
        assertEquals(positions.get(5L), tx11);
        assertEquals(positions.get(6L), tx12);

        // we will issue next round of commit, which will commit txns on epoch 1. 
        activeEpoch = store.getActiveEpoch(scope, stream, null, true, executor).join();
        record = store.startCommitTransactions(scope, stream, 100, null, executor).join();
        // verify that the order in record is same
        assertEquals(record.getObject().getTransactionsToCommit(), ordered);
        
        // verify that transactions included for commit are removed from positions.
        positions = streamObj.getAllOrderedCommittingTxns().join();
        assertEquals(1, positions.size());
        assertEquals(positions.get(3L), tx02);

        assertEquals(record.getObject().getTransactionsToCommit(), ordered);
        store.setState(scope, stream, State.COMMITTING_TXN, null, executor).join();
        // verify that it is committing transactions on epoch 1         

        store.completeCommitTransactions(scope, stream, record, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();

        // references for tx00 should be removed from orderer
        orderedRecords = streamObj.getOrderedCommittingTxnInLowestEpoch(100).join();
        ordered = orderedRecords.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        assertEquals(0, ordered.size());

        // verify that only reference to the open transaction is retained in position
        positions = streamObj.getAllOrderedCommittingTxns().join();
        assertEquals(1, positions.size());
        assertEquals(positions.get(3L), tx02);
    }

    @Test
    public void txnCommitBatchLimitTest() throws Exception {
        final String scope = "txnCommitBatch";
        final String stream = "txnCommitBatch";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        long scaleTs = System.currentTimeMillis();
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.75, 1.0);
        List<Long> scale1SealedSegments = Collections.singletonList(1L);

        // create 3 transactions on epoch 0 --> tx00, tx01, tx02 and mark them as committing.. 
        UUID tx00 = store.generateTransactionId(scope, stream, null, executor).join();
        store.createTransaction(scope, stream, tx00,
                100, 100, null, executor).get();
        UUID tx01 = store.generateTransactionId(scope, stream, null, executor).join();
        store.createTransaction(scope, stream, tx01,
                100, 100, null, executor).get();
        UUID tx02 = store.generateTransactionId(scope, stream, null, executor).join();
        store.createTransaction(scope, stream, tx02,
                100, 100, null, executor).get();

        // committing
        store.sealTransaction(scope, stream, tx00, true, Optional.empty(),
                "", Long.MIN_VALUE, null, executor).get();
        store.sealTransaction(scope, stream, tx01, true, Optional.empty(),
                "", Long.MIN_VALUE, null, executor).get();
        store.sealTransaction(scope, stream, tx02, true, Optional.empty(),
                "", Long.MIN_VALUE, null, executor).get();

        PersistentStreamBase streamObj = (PersistentStreamBase) ((AbstractStreamMetadataStore) store).getStream(scope, stream, null);
        
        // verify that when we retrieve transactions from lowest epoch we get tx00, tx01
        List<Map.Entry<UUID, ActiveTxnRecord>> orderedRecords = streamObj.getOrderedCommittingTxnInLowestEpoch(2).join();
        List<UUID> ordered = orderedRecords.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        assertEquals(2, ordered.size());
        assertEquals(tx00, ordered.get(0));
        assertEquals(tx01, ordered.get(1));

        orderedRecords = streamObj.getOrderedCommittingTxnInLowestEpoch(1000).join();
        ordered = orderedRecords.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        assertEquals(3, ordered.size());
        assertEquals(tx00, ordered.get(0));
        assertEquals(tx01, ordered.get(1));
        assertEquals(tx02, ordered.get(2));
        
        // commit tx00 and tx01
        ((AbstractStreamMetadataStore) store).commitTransaction(scope, stream, tx00, null, executor).join();
        ((AbstractStreamMetadataStore) store).commitTransaction(scope, stream, tx01, null, executor).join();

        orderedRecords = streamObj.getOrderedCommittingTxnInLowestEpoch(1000).join();
        ordered = orderedRecords.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        assertEquals(1, ordered.size());
        assertEquals(tx02, ordered.get(0));
        
        // scale and create transaction on new epoch too. 
    }

    private void scale(String scope, String stream, long scaleTs, List<Map.Entry<Double, Double>> newSegments, List<Long> scale1SealedSegments) {
        VersionedMetadata<EpochTransitionRecord> versioned = store.submitScale(scope, stream, scale1SealedSegments,
                newSegments, scaleTs, null, null, executor).join();
        EpochTransitionRecord response = versioned.getObject();
        Map<Long, Map.Entry<Double, Double>> scale1SegmentsCreated = response.getNewSegmentsWithRange();
        final int epoch = response.getActiveEpoch();
        assertEquals(0, epoch);
        assertNotNull(scale1SegmentsCreated);
        VersionedMetadata<State> state = store.getVersionedState(scope, stream, null, executor).join();
        state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).join();
        versioned = store.startScale(scope, stream, false, versioned, state, null, executor).join();
        // second txn created after new segments are created in segment table but not yet in history table
        // assert that txn is created on old epoch
        store.scaleCreateNewEpochs(scope, stream, versioned, null, executor).join();
        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned,
                null, executor).join();
        store.completeScale(scope, stream, versioned, null, executor).join();
        state = store.updateVersionedState(scope, stream, State.ACTIVE, state, null, executor).join();
    }

    protected void scale(String scope, String stream, int numOfSegments) {
        List<Map.Entry<Double, Double>> newRanges = new ArrayList<>();
        double delta = 1.0 / numOfSegments;
        for (int i = 0; i < numOfSegments; i++) {
            double low = delta * i;
            double high = i == numOfSegments - 1 ? 1.0 : delta * (i + 1);

            newRanges.add(new SimpleEntry<>(low, high));
        }

        List<Long> segmentsToSeal = store.getActiveSegments(scope, stream, null, executor).join()
                                         .stream().map(StreamSegmentRecord::segmentId).collect(Collectors.toList());
        VersionedMetadata<EpochTransitionRecord> versioned = store.submitScale(scope, stream, segmentsToSeal,
                newRanges, System.currentTimeMillis(), null, null, executor).join();
        VersionedMetadata<State> state = store.getVersionedState(scope, stream, null, executor).join();
        state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).join();
        store.startScale(scope, stream, false, versioned, state, null, executor).join();
        store.scaleCreateNewEpochs(scope, stream, versioned, null, executor).join();
        store.scaleSegmentsSealed(scope, stream, segmentsToSeal.stream().collect(Collectors.toMap(x -> x, x -> 10L)), versioned,
                null, executor).join();
        store.completeScale(scope, stream, versioned, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();
    }

    @Test
    public void truncationTest() throws Exception {
        final String scope = "ScopeTruncate";
        final String stream = "ScopeTruncate";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        Map<Long, Long> truncation = new HashMap<>();
        truncation.put(0L, 0L);
        truncation.put(1L, 0L);
        assertTrue(Futures.await(store.startTruncation(scope, stream, truncation, null, executor)));
        store.setState(scope, stream, State.TRUNCATING, null, executor).join();
        StreamTruncationRecord truncationProperty = store.getTruncationRecord(scope, stream, null, executor).join().getObject();
        assertTrue(truncationProperty.isUpdating());

        Map<Long, Long> truncation2 = new HashMap<>();
        truncation2.put(0L, 1L);
        truncation2.put(1L, 1L);

        assertFalse(Futures.await(store.startTruncation(scope, stream, truncation2, null, executor)));
        VersionedMetadata<StreamTruncationRecord> record = store.getTruncationRecord(scope, stream, null, executor).join();
        assertTrue(Futures.await(store.completeTruncation(scope, stream, record, null, executor)));

        truncationProperty = store.getTruncationRecord(scope, stream, null, executor).join().getObject();
        assertEquals(truncation, truncationProperty.getStreamCut());

        assertTrue(truncationProperty.getSpan().size() == 2);

        Map<Long, Long> truncation3 = new HashMap<>();
        truncation3.put(0L, 2L);
        truncation3.put(1L, 2L);

        assertTrue(Futures.await(store.startTruncation(scope, stream, truncation3, null, executor)));
        record = store.getTruncationRecord(scope, stream, null, executor).join();
        assertTrue(Futures.await(store.completeTruncation(scope, stream, record, null, executor)));
        store.setState(scope, stream, State.ACTIVE, null, executor).join();
    }

    @Test
    public void streamCutTest() throws Exception {
        final String scope = "ScopeStreamCut";
        final String stream = "StreamCut";
        final ScalingPolicy policy = ScalingPolicy.fixed(100);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        // perform 10 scales
        for (int i = 0; i < 10; i++) {
            scale(scope, stream, 100);
        }
        
        Map<Long, Long> invalid = new HashMap<>();
        invalid.put(0L, 0L);

        Map<Long, Long> valid = new HashMap<>();
        
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int epoch = random.nextInt(10);
            valid.put(NameUtils.computeSegmentId(epoch * 100 + i, epoch), 0L);
        }
        
        assertTrue(store.isStreamCutValid(scope, stream, valid, null, executor).join());
        assertFalse(store.isStreamCutValid(scope, stream, invalid, null, executor).join());
    }

    @Test
    public void retentionSetTest() throws Exception {
        final String scope = "ScopeRetain";
        final String stream = "StreamRetain";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.builder()
                .retentionType(RetentionPolicy.RetentionType.TIME)
                .retentionParam(Duration.ofDays(2).toMillis())
                .build();
        final StreamConfiguration configuration = StreamConfiguration.builder()
                .scalingPolicy(policy).retentionPolicy(retentionPolicy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();
        
        bucketStore.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, scope, stream, executor).get();
        Set<String> streams = bucketStore.getStreamsForBucket(BucketStore.ServiceType.RetentionService, 0, executor).get();
        assertTrue(streams.contains(String.format("%s/%s", scope, stream)));

        Map<Long, Long> map1 = new HashMap<>();
        map1.put(0L, 0L);
        map1.put(1L, 0L);
        long recordingTime = System.currentTimeMillis();
        StreamCutRecord streamCut1 = new StreamCutRecord(recordingTime, Long.MIN_VALUE, ImmutableMap.copyOf(map1));
        store.addStreamCutToRetentionSet(scope, stream, streamCut1, null, executor).get();

        Map<Long, Long> map2 = new HashMap<>();
        map2.put(0L, 10L);
        map2.put(1L, 10L);
        StreamCutRecord streamCut2 = new StreamCutRecord(recordingTime + 10, Long.MIN_VALUE, ImmutableMap.copyOf(map2));
        store.addStreamCutToRetentionSet(scope, stream, streamCut2, null, executor).get();

        Map<Long, Long> map3 = new HashMap<>();
        map3.put(0L, 20L);
        map3.put(1L, 20L);
        StreamCutRecord streamCut3 = new StreamCutRecord(recordingTime + 20, Long.MIN_VALUE, ImmutableMap.copyOf(map3));
        store.addStreamCutToRetentionSet(scope, stream, streamCut3, null, executor).get();

        List<StreamCutRecord> list = store.getRetentionSet(scope, stream, null, executor)
                .thenCompose(x -> Futures.allOfWithResults(x.getRetentionRecords().stream()
                        .map(y -> store.getStreamCutRecord(scope, stream, y, null, executor))
                .collect(Collectors.toList()))).join();
        assertTrue(list.contains(streamCut1));
        assertTrue(list.contains(streamCut2));
        assertTrue(list.contains(streamCut3));

        store.deleteStreamCutBefore(scope, stream, streamCut2.getReferenceRecord(), null, executor).get();

        list = store.getRetentionSet(scope, stream, null, executor)
                    .thenCompose(x -> Futures.allOfWithResults(x.getRetentionRecords().stream()
                                                                .map(y -> store.getStreamCutRecord(scope, stream, y, null, executor))
                                                                .collect(Collectors.toList()))).join();
        assertTrue(!list.contains(streamCut1));
        assertTrue(!list.contains(streamCut2));
        assertTrue(list.contains(streamCut3));

        bucketStore.removeStreamFromBucketStore(BucketStore.ServiceType.RetentionService, scope, stream, executor).get();
        streams = bucketStore.getStreamsForBucket(BucketStore.ServiceType.RetentionService, 0, executor).get();
        assertTrue(!streams.contains(String.format("%s/%s", scope, stream)));
    }

    @Test
    public void strictlyGreaterThanTest() throws Exception {
        final String scope = "ScopeRetain3";
        final String stream = "StreamRetain";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.builder()
                .retentionType(RetentionPolicy.RetentionType.TIME)
                .retentionParam(Duration.ofDays(2).toMillis())
                .build();
        final StreamConfiguration configuration = StreamConfiguration.builder()
                .scalingPolicy(policy).retentionPolicy(retentionPolicy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        Map<Long, Long> map1 = new HashMap<>();
        map1.put(0L, 10L);
        map1.put(1L, 10L);

        Map<Long, Long> streamCut = new HashMap<>();

        streamCut.put(0L, 0L);
        streamCut.put(1L, 10L);
        assertFalse(store.streamCutStrictlyGreaterThan(scope, stream, streamCut, map1, null, executor).join());

        streamCut.put(0L, 10L);
        streamCut.put(1L, 10L);
        assertTrue(store.streamCutStrictlyGreaterThan(scope, stream, streamCut, map1, null, executor).join());

        streamCut.put(0L, 1L);
        streamCut.put(1L, 11L);
        assertFalse(store.streamCutStrictlyGreaterThan(scope, stream, streamCut, map1, null, executor).join());

        streamCut.put(0L, 20L);
        streamCut.put(1L, 20L);
        assertTrue(store.streamCutStrictlyGreaterThan(scope, stream, streamCut, map1, null, executor).join());
    }

    @Test
    public void streamCutReferenceRecordBeforeTest() throws Exception {
        final String scope = "ScopeRetain2";
        final String stream = "StreamRetain";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.builder()
                .retentionType(RetentionPolicy.RetentionType.TIME)
                .retentionParam(Duration.ofDays(2).toMillis())
                .build();
        final StreamConfiguration configuration = StreamConfiguration.builder()
                .scalingPolicy(policy).retentionPolicy(retentionPolicy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        Map<Long, Long> map1 = new HashMap<>();
        map1.put(0L, 1L);
        map1.put(1L, 1L);
        long recordingTime = 1;
        StreamCutRecord streamCut1 = new StreamCutRecord(recordingTime, Long.MIN_VALUE, ImmutableMap.copyOf(map1));
        store.addStreamCutToRetentionSet(scope, stream, streamCut1, null, executor).get();

        Map<Long, Long> map2 = new HashMap<>();
        map2.put(0L, 10L);
        map2.put(1L, 10L);
        StreamCutRecord streamCut2 = new StreamCutRecord(recordingTime + 10, Long.MIN_VALUE, ImmutableMap.copyOf(map2));
        store.addStreamCutToRetentionSet(scope, stream, streamCut2, null, executor).get();

        Map<Long, Long> map3 = new HashMap<>();
        map3.put(0L, 20L);
        map3.put(1L, 20L);
        StreamCutRecord streamCut3 = new StreamCutRecord(recordingTime + 20, Long.MIN_VALUE, ImmutableMap.copyOf(map3));
        store.addStreamCutToRetentionSet(scope, stream, streamCut3, null, executor).get();

        Map<Long, Long> streamCut = new HashMap<>();

        RetentionSet retentionSet = store.getRetentionSet(scope, stream, null, executor).join();

        // 0/0, 1/1 ..there should be nothing before it
        streamCut.put(0L, 0L);
        streamCut.put(1L, 1L);
        StreamCutReferenceRecord beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertNull(beforeRef);

        // 0/1, 1/1 .. sc1
        streamCut.put(0L, 1L);
        streamCut.put(1L, 1L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut1.getRecordingTime());

        // 0/5, 1/5 .. sc1
        streamCut.put(0L, 1L);
        streamCut.put(1L, 1L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut1.getRecordingTime());

        // 0/0, 1/5 .. nothing
        streamCut.put(0L, 0L);
        streamCut.put(1L, 5L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertNull(beforeRef);

        // 0/10, 1/10 ... sc2
        streamCut.put(0L, 10L);
        streamCut.put(1L, 10L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut2.getRecordingTime());

        // 0/9, 1/15 ... sc1
        streamCut.put(0L, 9L);
        streamCut.put(1L, 15L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut1.getRecordingTime());

        // 0/19, 1/20 ... sc2
        streamCut.put(0L, 19L);
        streamCut.put(1L, 20L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut2.getRecordingTime());

        // 0/20, 1/20 ... sc3
        streamCut.put(0L, 20L);
        streamCut.put(1L, 20L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut3.getRecordingTime());

        // 0/21, 1/21 ... sc3
        streamCut.put(0L, 21L);
        streamCut.put(1L, 21L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut3.getRecordingTime());

        // now add another entry so that we have even number of records and and repeat the test
        // but here we make sure we are still using map3 but adding the time. we should always pick the latest if there
        // are subsequent streamcutrecords with identical streamcuts.
        StreamCutRecord streamCut4 = new StreamCutRecord(recordingTime + 30, Long.MIN_VALUE, ImmutableMap.copyOf(map3));
        store.addStreamCutToRetentionSet(scope, stream, streamCut4, null, executor).get();

        retentionSet = store.getRetentionSet(scope, stream, null, executor).join();

        // 0/0, 1/1 ..there should be nothing before it
        streamCut.put(0L, 0L);
        streamCut.put(1L, 1L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertNull(beforeRef);

        // 0/1, 1/1 .. 0/1, 1/1
        streamCut.put(0L, 1L);
        streamCut.put(1L, 1L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut1.getRecordingTime());

        // 0/5, 1/5 .. 0/1, 1/1
        streamCut.put(0L, 5L);
        streamCut.put(1L, 5L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut1.getRecordingTime());

        // 0/0, 1/5 .. nothing
        streamCut.put(0L, 0L);
        streamCut.put(1L, 5L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertNull(beforeRef);

        // 0/10, 1/10 ... 0/10, 1/10
        streamCut.put(0L, 10L);
        streamCut.put(1L, 10L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut2.getRecordingTime());

        // 0/9, 1/15 ... 0/1, 1/1
        streamCut.put(0L, 9L);
        streamCut.put(1L, 15L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut1.getRecordingTime());

        // 0/19, 1/20 ... 0/10, 1/10
        streamCut.put(0L, 19L);
        streamCut.put(1L, 20L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut2.getRecordingTime());

        // 0/20, 1/20 ... 0/20, 1/20
        streamCut.put(0L, 20L);
        streamCut.put(1L, 20L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut4.getRecordingTime());

        // 0/21, 1/21 ... 0/20, 1/20
        streamCut.put(0L, 21L);
        streamCut.put(1L, 21L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut4.getRecordingTime());

        // 0/31, 1/31 ... 0/30, 1/30
        streamCut.put(0L, 30L);
        streamCut.put(1L, 30L);
        beforeRef = store.findStreamCutReferenceRecordBefore(scope, stream, streamCut, retentionSet, null, executor).join();
        assertEquals(beforeRef.getRecordingTime(), streamCut4.getRecordingTime());
    }

    @Test
    public void sizeTest() throws Exception {
        final String scope = "ScopeSize";
        final String stream = "StreamSize";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.builder().retentionType(RetentionPolicy.RetentionType.SIZE)
                .retentionParam(100L).build();
        final StreamConfiguration configuration = StreamConfiguration.builder()
                .scalingPolicy(policy).retentionPolicy(retentionPolicy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).get();

        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        bucketStore.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, scope, stream, executor).get();
        Set<String> streams = bucketStore.getStreamsForBucket(BucketStore.ServiceType.RetentionService, 0, executor).get();
        assertTrue(streams.contains(String.format("%s/%s", scope, stream)));

        // region Size Computation on stream cuts on epoch 0
        Map<Long, Long> map1 = new HashMap<>();
        map1.put(0L, 10L);
        map1.put(1L, 10L);

        Long size = store.getSizeTillStreamCut(scope, stream, map1, Optional.empty(), null, executor).join();
        assertEquals(20L, (long) size);

        long recordingTime = System.currentTimeMillis();
        StreamCutRecord streamCut1 = new StreamCutRecord(recordingTime, size, ImmutableMap.copyOf(map1));
        store.addStreamCutToRetentionSet(scope, stream, streamCut1, null, executor).get();

        Map<Long, Long> map2 = new HashMap<>();
        map2.put(0L, 20L);
        map2.put(1L, 20L);
        size = store.getSizeTillStreamCut(scope, stream, map2, Optional.empty(), null, executor).join();
        assertEquals(40L, (long) size);

        StreamCutRecord streamCut2 = new StreamCutRecord(recordingTime + 10, size, ImmutableMap.copyOf(map2));
        store.addStreamCutToRetentionSet(scope, stream, streamCut2, null, executor).get();

        Map<Long, Long> map3 = new HashMap<>();
        map3.put(0L, 30L);
        map3.put(1L, 30L);

        size = store.getSizeTillStreamCut(scope, stream, map3, Optional.empty(), null, executor).join();
        assertEquals(60L, (long) size);
        StreamCutRecord streamCut3 = new StreamCutRecord(recordingTime + 20, 60L, ImmutableMap.copyOf(map3));
        store.addStreamCutToRetentionSet(scope, stream, streamCut3, null, executor).get();

        // endregion

        // region Size Computation on multiple epochs

        long scaleTs = System.currentTimeMillis();
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.5, 1.0);
        List<Long> scale1SealedSegments = Lists.newArrayList(0L, 1L);

        VersionedMetadata<EpochTransitionRecord> versioned = store.submitScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment2, segment3), scaleTs, null, null, executor).join();
        VersionedMetadata<State> state = store.getVersionedState(scope, stream, null, executor).get();
        state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).get();
        store.startScale(scope, stream, false, versioned, state, null, executor).join();
        store.scaleCreateNewEpochs(scope, stream, versioned, null, executor).join();
        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 40L)), versioned,
                null, executor).join();
        store.completeScale(scope, stream, versioned, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        // complex stream cut - across two epochs
        Map<Long, Long> map4 = new HashMap<>();
        map4.put(0L, 40L);
        map4.put(computeSegmentId(3, 1), 10L);
        size = store.getSizeTillStreamCut(scope, stream, map4, Optional.empty(), null, executor).join();
        assertEquals(Long.valueOf(90L), size);
        StreamCutRecord streamCut4 = new StreamCutRecord(recordingTime + 30, size, ImmutableMap.copyOf(map4));
        store.addStreamCutToRetentionSet(scope, stream, streamCut4, null, executor).get();

        // simple stream cut on epoch 2
        Map<Long, Long> map5 = new HashMap<>();
        map5.put(computeSegmentId(2, 1), 10L);
        map5.put(computeSegmentId(3, 1), 10L);

        size = store.getSizeTillStreamCut(scope, stream, map5, Optional.empty(), null, executor).join();
        assertTrue(size == 100L);
        StreamCutRecord streamCut5 = new StreamCutRecord(recordingTime + 30, size, ImmutableMap.copyOf(map5));
        store.addStreamCutToRetentionSet(scope, stream, streamCut5, null, executor).get();
        // endregion
    }

    @Test
    public void getSafeStartingSegmentNumberForTest() {
        final String scope = "RecreationScope";
        final String stream = "RecreatedStream";
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder()
                                                                     .scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).join();

        for (int i = 0; i < 10; i++) {
            assertEquals(i * policy.getMinNumSegments(), (int) ((AbstractStreamMetadataStore) store).getSafeStartingSegmentNumberFor(scope, stream).join());
            store.createStream(scope, stream, configuration, start, null, executor).join();
            store.setState(scope, stream, State.ACTIVE, null, executor).join();
            store.setSealed(scope, stream, null, executor).join();
            store.deleteStream(scope, stream, null, executor).join();
        }
    }

    @Test
    public void recordLastStreamSegmentTest() {
        final String scope = "RecreationScope2";
        final String stream = "RecreatedStream2";

        for (int i = 0; i < 10; i++) {
            ((AbstractStreamMetadataStore) store).recordLastStreamSegment(scope, stream, i, null, executor).join();
            assertEquals(i + 1, (int) ((AbstractStreamMetadataStore) store).getSafeStartingSegmentNumberFor(scope, stream).join());
        }
    }

    @Test
    public void deletePartiallyCreatedStreamTest() {
        final String scopeName = "RecreationScopePartial";
        final String streamName = "RecreatedStreamPartial";

        store.createScope(scopeName).join();
        
        // region case 1: only add stream to scope without any additional metadata
        StreamMetadataStoreTestHelper.addStreamToScope(store, scopeName, streamName);
        assertTrue(store.checkStreamExists(scopeName, streamName).join());
        store.deleteStream(scopeName, streamName, null, executor).join();
        assertFalse(store.checkStreamExists(scopeName, streamName).join());

        // region case 2: only add creation time for the stream and then delete it. 
        StreamMetadataStoreTestHelper.partiallyCreateStream(store, scopeName, streamName, Optional.of(100L), false);
        assertTrue(store.checkStreamExists(scopeName, streamName).join());
        store.deleteStream(scopeName, streamName, null, executor).join();
        assertFalse(store.checkStreamExists(scopeName, streamName).join());
        // endregion
        
        // region case 3: create stream again but this time create the `state` but not history record. 
        StreamMetadataStoreTestHelper.partiallyCreateStream(store, scopeName, streamName, Optional.of(100L), true);
        assertTrue(store.checkStreamExists(scopeName, streamName).join());
        store.deleteStream(scopeName, streamName, null, executor).join();
        assertFalse(store.checkStreamExists(scopeName, streamName).join());
        // endregion
        
        // region case 4: now create full stream metadata. 
        // now create full stream metadata without setting state to active
        // since there was no active segments, so we should have segments created from segment 0.
        // configuration 2 has 3 segments. So highest segment number should be 2. 
        store.createStream(scopeName, streamName, configuration2, 101L, null, executor).join();
        assertTrue(store.checkStreamExists(scopeName, streamName).join());

        assertEquals(store.getActiveEpoch(scopeName, streamName, null, true, executor).join()
                .getSegmentIds().stream().max(Long::compareTo).get().longValue(), 2L);
        
        store.deleteStream(scopeName, streamName, null, executor).join();
        assertFalse(store.checkStreamExists(scopeName, streamName).join());

        store.createStream(scopeName, streamName, configuration2, 102L, null, executor).join();
        assertEquals(store.getActiveEpoch(scopeName, streamName, null, true, executor).join()
                          .getSegmentIds().stream().max(Long::compareTo).get().longValue(), 5L);
        // endregion
    }
    
    @Test
    public void testWriterMark() {
        String stream = "mark";
        store.createScope(scope).join();
        store.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1))
                                                             .build(), System.currentTimeMillis(), null, executor).join();

        // data not found
        String writer1 = "writer1";
        AssertExtensions.assertFutureThrows("", store.getWriterMark(scope, stream, writer1, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);

        // now note writer record
        store.noteWriterMark(scope, stream, writer1, 0L, Collections.singletonMap(0L, 1L), null, executor).join();
        store.getWriterMark(scope, stream, writer1, null, executor).join();

        // update writer record
        store.noteWriterMark(scope, stream, writer1, 1L, Collections.singletonMap(0L, 2L), null, executor).join();
        WriterMark writerMark = store.getWriterMark(scope, stream, writer1, null, executor).join();
        assertTrue(writerMark.isAlive());

        Map<String, WriterMark> marks = store.getAllWriterMarks(scope, stream, null, executor).join();
        assertTrue(marks.containsKey(writer1));

        store.shutdownWriter(scope, stream, writer1, null, executor).join();
        writerMark = store.getWriterMark(scope, stream, writer1, null, executor).join();
        assertFalse(writerMark.isAlive());

        // note a mark after a writer has been shutdown. It should become alive again. 
        store.noteWriterMark(scope, stream, writer1, 2L, Collections.singletonMap(0L, 2L), null, executor).join();
        writerMark = store.getWriterMark(scope, stream, writer1, null, executor).join();
        assertTrue(writerMark.isAlive());

        // remove writer
        AssertExtensions.assertFutureThrows("Mismatched writer mark did not throw exception",
                store.removeWriter(scope, stream, writer1, WriterMark.EMPTY, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        store.removeWriter(scope, stream, writer1, writerMark, null, executor).join();
        AssertExtensions.assertFutureThrows("", store.getWriterMark(scope, stream, writer1, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);

        marks = store.getAllWriterMarks(scope, stream, null, executor).join();
        assertTrue(marks.isEmpty());

        String writer2 = "writer2";
        store.noteWriterMark(scope, stream, writer2, 1L, Collections.singletonMap(0L, 1L), null, executor).join();
        String writer3 = "writer3";
        store.noteWriterMark(scope, stream, writer3, 1L, Collections.singletonMap(0L, 1L), null, executor).join();
        String writer4 = "writer4";
        store.noteWriterMark(scope, stream, writer4, 1L, Collections.singletonMap(0L, 1L), null, executor).join();

        marks = store.getAllWriterMarks(scope, stream, null, executor).join();
        assertFalse(marks.containsKey(writer1));
        assertTrue(marks.containsKey(writer2));
        assertTrue(marks.containsKey(writer3));
        assertTrue(marks.containsKey(writer4));
    }

    @Test
    public void testMarkOnTransactionCommit() {
        // create txn
        // seal txn with committing
        final String scope = "MarkOnTransactionCommit";
        final String stream = "MarkOnTransactionCommit";
        final ScalingPolicy policy = ScalingPolicy.fixed(1);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        long start = System.currentTimeMillis();
        store.createScope(scope).join();

        store.createStream(scope, stream, configuration, start, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();

        UUID txnId = store.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData tx01 = store.createTransaction(scope, stream, txnId,
                100, 100, null, executor).join();

        String writer1 = "writer1";
        long time = 1L;
        store.sealTransaction(scope, stream, txnId, true, Optional.of(tx01.getVersion()), writer1, time, null, executor).join();
        VersionedMetadata<CommittingTransactionsRecord> record = store.startCommitTransactions(scope, stream, 100, null, executor).join();
        store.recordCommitOffsets(scope, stream, txnId, Collections.singletonMap(0L, 1L), null, executor).join();
        store.completeCommitTransactions(scope, stream, record, null, executor).join();

        // verify that writer mark is created in the store
        WriterMark mark = store.getWriterMark(scope, stream, writer1, null, executor).join();
        assertEquals(mark.getTimestamp(), time);
        assertEquals(mark.getPosition().size(), 1);
        assertTrue(mark.getPosition().containsKey(0L));
        assertEquals(mark.getPosition().get(0L).longValue(), 1L);
    }
    
    @Test
    public void testHistoryTimeSeriesChunk() throws Exception {
        String scope = "history";
        String stream = "history";
        createAndScaleStream(store, scope, stream, 2);
        HistoryTimeSeries chunk = store.getHistoryTimeSeriesChunk(scope, stream, 0, null, executor).join();
        assertEquals(chunk.getLatestRecord().getEpoch(), 2);
    }
    
    @Test
    public void testSealedSegmentSizeMapShard() throws Exception {
        String scope = "sealedMap";
        String stream = "sealedMap";
        createAndScaleStream(store, scope, stream, 2);
        SealedSegmentsMapShard shard = store.getSealedSegmentSizeMapShard(scope, stream, 0, null, executor).join();
        assertEquals(shard.getSize(NameUtils.computeSegmentId(0, 0)).longValue(), 0L);
        assertEquals(shard.getSize(NameUtils.computeSegmentId(1, 1)).longValue(), 1L);
        assertNull(shard.getSize(NameUtils.computeSegmentId(2, 2)));
    }
    
    @Test
    public void testSegmentSealedEpoch() throws Exception {
        String scope = "sealedMap";
        String stream = "sealedMap";
        createAndScaleStream(store, scope, stream, 2);
        long segmentId = NameUtils.computeSegmentId(0, 0);
        int epoch = store.getSegmentSealedEpoch(scope, stream, segmentId, null, executor).join();
        assertEquals(epoch, 1);
        segmentId = NameUtils.computeSegmentId(1, 1);
        epoch = store.getSegmentSealedEpoch(scope, stream, segmentId, null, executor).join();
        assertEquals(epoch, 2);
        segmentId = NameUtils.computeSegmentId(2, 2);
        epoch = store.getSegmentSealedEpoch(scope, stream, segmentId, null, executor).join();
        assertEquals(epoch, -1);
    }
    
    private void createAndScaleStream(StreamMetadataStore store, String scope, String stream, int times) {
        long time = System.currentTimeMillis();
        store.createScope(scope).join();
        store.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1))
                                                             .build(), time, null, executor).join();
        VersionedMetadata<State> state = store.getVersionedState(scope, stream, null, executor).join();
        store.updateVersionedState(scope, stream, State.ACTIVE, state, null, executor).join();

        for (int i = 0; i < times; i++) {
            long scaleTs = time + i;
            List<Long> sealedSegments = Collections.singletonList(NameUtils.computeSegmentId(i, i));
            VersionedMetadata<EpochTransitionRecord> etr = store.submitScale(scope, stream, sealedSegments,
                    Collections.singletonList(new SimpleEntry<>(0.0, 1.0)), scaleTs, null, null, executor).join();
            state = store.getVersionedState(scope, stream, null, executor).join();
            state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).join();
            etr = store.startScale(scope, stream, false, etr, state, null, executor).join();
            store.scaleCreateNewEpochs(scope, stream, etr, null, executor).join();
            long size = i;
            store.scaleSegmentsSealed(scope, stream, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> size)), etr,
                    null, executor).join();
            store.completeScale(scope, stream, etr, null, executor).join();
            store.setState(scope, stream, State.ACTIVE, null, executor).join();
        }
    }
}
