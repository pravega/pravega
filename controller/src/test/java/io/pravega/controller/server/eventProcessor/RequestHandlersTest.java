/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import com.google.common.collect.Lists;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.CommitRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.tables.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamConfigurationRecord;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RequestHandlersTest {
    private final String scope = "scope";
    private final String stream = "stream";
    StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(
            ScalingPolicy.byEventRate(1, 2, 3)).build();

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    private StreamMetadataStore streamStore;
    private TaskMetadataStore taskMetadataStore;
    private HostControllerStore hostStore;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;

    private TestingServer zkServer;

    private CuratorFramework zkClient;
    private ClientFactory clientFactory;
    private ConnectionFactoryImpl connectionFactory;

    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(20, 1, 50));

        zkClient.start();

        String hostId;
        try {
            //On each controller process restart, it gets a fresh hostId,
            //which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            hostId = UUID.randomUUID().toString();
        }

        streamStore = spy(StreamStoreFactory.createZKStore(zkClient, executor));

        taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);

        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        clientFactory = mock(ClientFactory.class);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelper,
                executor, hostId, connectionFactory, AuthHelper.getDisabledAuthHelper());
        streamMetadataTasks.initializeStreamWriters(clientFactory, Config.SCALE_STREAM_NAME);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, hostStore,
                segmentHelper, executor, hostId, connectionFactory, AuthHelper.getDisabledAuthHelper());

        long createTimestamp = System.currentTimeMillis();

        // add a host in zk
        // mock pravega
        // create a stream
        streamStore.createScope(scope).get();
    }

    @After
    public void tearDown() throws Exception {
        clientFactory.close();
        connectionFactory.close();
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        zkClient.close();
        zkServer.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 300000)
    public void testConcurrentIdempotentCommitTxnRequest() {
        Map<String, Integer> map = new HashMap<>();
        map.put("startRollingTxn", 0);
        map.put("rollingTxnCreateDuplicateEpochs", 0);
        map.put("completeRollingTxn", 0);
        map.put("startCommitTransactions", 1);
        map.put("completeCommitTransactions", 1);
        map.put("updateVersionedState", 1);
        concurrentTxnCommit("commit1", "startCommitTransactions", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 3);

        map.put("startCommitTransactions", 1);
        map.put("completeCommitTransactions", 1);
        concurrentTxnCommit("commit2", "completeCommitTransactions", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 2);
    }

    private void concurrentTxnCommit(String stream, String func,
                                     boolean expectFailureOnFirstJob,
                                     Predicate<Throwable> firstExceptionPredicate,
                                     Map<String, Integer> invocationCount, int expectedVersion) {
        StreamMetadataStore streamStore1 = StreamStoreFactory.createZKStore(zkClient, executor);
        StreamMetadataStore streamStore1Spied = spy(StreamStoreFactory.createZKStore(zkClient, executor));
        StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();

        StreamMetadataStore streamStore2 = StreamStoreFactory.createZKStore(zkClient, executor);

        CommitRequestHandler requestHandler1 = new CommitRequestHandler(streamStore1Spied, streamMetadataTasks, streamTransactionMetadataTasks, executor);
        CommitRequestHandler requestHandler2 = new CommitRequestHandler(streamStore2, streamMetadataTasks, streamTransactionMetadataTasks, executor);
        ScaleOperationTask scaleRequesthandler = new ScaleOperationTask(streamMetadataTasks, streamStore2, executor);

        // create txn on epoch 0
        UUID txnId = streamStore1.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnEpoch0 = streamStore1.createTransaction(scope, stream, txnId, 1000L, 10000L, null, executor).join();
        streamStore1.sealTransaction(scope, stream, txnId, true, Optional.of(txnEpoch0.getVersion()), null, executor).join();
        // perform scale
        ScaleOpEvent event = new ScaleOpEvent(scope, stream, Lists.newArrayList(0L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), false, System.currentTimeMillis());
        scaleRequesthandler.execute(event).join();

        txnId = streamStore1.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnEpoch1 = streamStore1.createTransaction(scope, stream, txnId, 1000L, 10000L, null, executor).join();
        streamStore1.sealTransaction(scope, stream, txnId, true, Optional.of(txnEpoch1.getVersion()), null, executor).join();

        // regular commit
        // start commit transactions
        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();

        CommitEvent commitOnEpoch1 = new CommitEvent(scope, stream, 1);

        setMockCommitTxnLatch(streamStore1, streamStore1Spied, func, signal, wait);

        CompletableFuture<Void> future1 = requestHandler1.execute(commitOnEpoch1);

        signal.join();
        // let this run to completion. this should succeed 
        requestHandler2.execute(commitOnEpoch1).join();

        wait.complete(null);

        if (expectFailureOnFirstJob) {
            AssertExtensions.assertThrows("first commit should fail", () -> future1, firstExceptionPredicate);
            verify(streamStore1Spied, times(invocationCount.get("startCommitTransactions")))
                    .startCommitTransactions(anyString(), anyString(), anyInt(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("completeCommitTransactions")))
                    .completeCommitTransactions(anyString(), anyString(), any(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("updateVersionedState")))
                    .updateVersionedState(anyString(), anyString(), any(), any(), any(), any());
        } else {
            future1.join();
        }

        VersionedMetadata<CommittingTransactionsRecord> versioned = streamStore1.getVersionedCommittingTransactionsRecord(scope, stream, null, executor).join();
        assertEquals(CommittingTransactionsRecord.EMPTY, versioned.getObject());
        assertEquals(expectedVersion, versioned.getVersion().asIntVersion().getIntValue().intValue());
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
    }


    @SuppressWarnings("unchecked")
    @Test(timeout = 300000)
    public void testConcurrentIdempotentRollingTxnRequest() {
        Map<String, Integer> map = new HashMap<>();
        map.put("startRollingTxn", 0);
        map.put("rollingTxnCreateDuplicateEpochs", 0);
        map.put("completeRollingTxn", 0);
        map.put("startCommitTransactions", 1);
        map.put("completeCommitTransactions", 1);
        map.put("updateVersionedState", 1);
        concurrentRollingTxnCommit("stream1", "startCommitTransactions", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 4);

        map.put("startRollingTxn", 1);
        map.put("completeCommitTransactions", 0);
        concurrentRollingTxnCommit("stream2", "startRollingTxn", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 3);

        map.put("rollingTxnCreateDuplicateEpochs", 1);
        map.put("completeRollingTxn", 1);
        map.put("completeCommitTransactions", 1);
        concurrentRollingTxnCommit("stream3", "rollingTxnCreateDuplicateEpochs", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 3);

        concurrentRollingTxnCommit("stream4", "completeRollingTxn", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 3);

        concurrentRollingTxnCommit("stream5", "completeCommitTransactions", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 3);
    }

    private void concurrentRollingTxnCommit(String stream, String func,
                                            boolean expectFailureOnFirstJob,
                                            Predicate<Throwable> firstExceptionPredicate,
                                            Map<String, Integer> invocationCount, int expectedVersion) {
        StreamMetadataStore streamStore1 = StreamStoreFactory.createZKStore(zkClient, executor);
        StreamMetadataStore streamStore1Spied = spy(StreamStoreFactory.createZKStore(zkClient, executor));
        StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();

        StreamMetadataStore streamStore2 = StreamStoreFactory.createZKStore(zkClient, executor);

        CommitRequestHandler requestHandler1 = new CommitRequestHandler(streamStore1Spied, streamMetadataTasks, streamTransactionMetadataTasks, executor);
        CommitRequestHandler requestHandler2 = new CommitRequestHandler(streamStore2, streamMetadataTasks, streamTransactionMetadataTasks, executor);
        ScaleOperationTask scaleRequesthandler = new ScaleOperationTask(streamMetadataTasks, streamStore2, executor);

        // create txn on epoch 0
        UUID txnId = streamStore1.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnEpoch0 = streamStore1.createTransaction(scope, stream, txnId, 1000L, 10000L, null, executor).join();
        streamStore1.sealTransaction(scope, stream, txnId, true, Optional.of(txnEpoch0.getVersion()), null, executor).join();
        // perform scale
        ScaleOpEvent event = new ScaleOpEvent(scope, stream, Lists.newArrayList(0L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), false, System.currentTimeMillis());
        scaleRequesthandler.execute(event).join();

        txnId = streamStore1.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnEpoch1 = streamStore1.createTransaction(scope, stream, txnId, 1000L, 10000L, null, executor).join();
        streamStore1.sealTransaction(scope, stream, txnId, true, Optional.of(txnEpoch1.getVersion()), null, executor).join();

        // regular commit
        // start commit transactions
        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();

        // test rolling transaction
        CommitEvent commitOnEpoch0 = new CommitEvent(scope, stream, 0);

        setMockCommitTxnLatch(streamStore1, streamStore1Spied, func, signal, wait);

        // start rolling txn
        // stall rolling transaction in different stages
        CompletableFuture<Void> future1Rolling = requestHandler1.execute(commitOnEpoch0);
        signal.join();
        requestHandler2.execute(commitOnEpoch0).join();
        wait.complete(null);

        if (expectFailureOnFirstJob) {
            AssertExtensions.assertThrows("first commit should fail", () -> future1Rolling, firstExceptionPredicate);
            verify(streamStore1Spied, times(invocationCount.get("startCommitTransactions")))
                    .startCommitTransactions(anyString(), anyString(), anyInt(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("startRollingTxn"))).startRollingTxn(anyString(), anyString(), anyInt(), any(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("rollingTxnCreateDuplicateEpochs")))
                    .rollingTxnCreateDuplicateEpochs(anyString(), anyString(), any(), anyLong(), any(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("completeRollingTxn")))
                    .completeRollingTxn(anyString(), anyString(), any(), anyLong(), any(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("completeCommitTransactions")))
                    .completeCommitTransactions(anyString(), anyString(), any(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("updateVersionedState")))
                    .updateVersionedState(anyString(), anyString(), any(), any(), any(), any());
        } else {
            future1Rolling.join();
        }
        // validate rolling txn done
        VersionedMetadata<CommittingTransactionsRecord> versioned = streamStore1.getVersionedCommittingTransactionsRecord(scope, stream, null, executor).join();
        assertEquals(CommittingTransactionsRecord.EMPTY, versioned.getObject());
        assertEquals(expectedVersion, versioned.getVersion().asIntVersion().getIntValue().intValue());
        assertEquals(3, streamStore1.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch());
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
    }

    private void setMockCommitTxnLatch(StreamMetadataStore store, StreamMetadataStore spied,
                                       String func, CompletableFuture<Void> signal, CompletableFuture<Void> waitOn) {
        switch (func) {
            case "startCommitTransactions":
                doAnswer(x -> {
                    signal.complete(null);
                    waitOn.join();
                    return store.startCommitTransactions(x.getArgument(0), x.getArgument(1),
                            x.getArgument(2), x.getArgument(3), x.getArgument(4));
                }).when(spied).startCommitTransactions(anyString(), anyString(), anyInt(), any(), any());
                break;
            case "completeCommitTransactions":
                doAnswer(x -> {
                    signal.complete(null);
                    waitOn.join();
                    return store.completeCommitTransactions(x.getArgument(0), x.getArgument(1),
                            x.getArgument(2), x.getArgument(3), x.getArgument(4));
                }).when(spied).completeCommitTransactions(anyString(), anyString(), any(), any(), any());
                break;
            case "startRollingTxn":
                doAnswer(x -> {
                    signal.complete(null);
                    waitOn.join();
                    return store.startRollingTxn(x.getArgument(0), x.getArgument(1),
                            x.getArgument(2), x.getArgument(3), x.getArgument(4), x.getArgument(5));
                }).when(spied).startRollingTxn(anyString(), anyString(), anyInt(), any(), any(), any());
                break;
            case "rollingTxnCreateDuplicateEpochs":
                doAnswer(x -> {
                    signal.complete(null);
                    waitOn.join();
                    return store.rollingTxnCreateDuplicateEpochs(x.getArgument(0), x.getArgument(1),
                            x.getArgument(2), x.getArgument(3), x.getArgument(4), x.getArgument(5), x.getArgument(6));
                }).when(spied).rollingTxnCreateDuplicateEpochs(anyString(), anyString(), any(), anyLong(), any(), any(), any());
                break;
            case "completeRollingTxn":
                doAnswer(x -> {
                    signal.complete(null);
                    waitOn.join();
                    return store.completeRollingTxn(x.getArgument(0), x.getArgument(1),
                            x.getArgument(2), x.getArgument(3), x.getArgument(4),
                            x.getArgument(5), x.getArgument(6));
                }).when(spied).completeRollingTxn(anyString(), anyString(), any(), anyLong(), any(), any(), any());
                break;
            case "updateVersionedState":
                doAnswer(x -> {
                    signal.complete(null);
                    waitOn.join();
                    return store.updateVersionedState(x.getArgument(0), x.getArgument(1),
                            x.getArgument(2), x.getArgument(3), x.getArgument(4), x.getArgument(5));
                }).when(spied).updateVersionedState(anyString(), anyString(), any(), any(), any(), any());
                break;
            default:
                break;
        }
    }

    // concurrent update stream
    @SuppressWarnings("unchecked")
    @Test(timeout = 300000)
    public void concurrentUpdateStream() {
        String stream = "update";
        StreamMetadataStore streamStore1 = StreamStoreFactory.createZKStore(zkClient, executor);
        StreamMetadataStore streamStore1Spied = spy(StreamStoreFactory.createZKStore(zkClient, executor));
        StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();

        StreamMetadataStore streamStore2 = StreamStoreFactory.createZKStore(zkClient, executor);

        UpdateStreamTask requestHandler1 = new UpdateStreamTask(streamMetadataTasks, streamStore1Spied, executor);
        UpdateStreamTask requestHandler2 = new UpdateStreamTask(streamMetadataTasks, streamStore2, executor);

        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();

        streamStore1.startUpdateConfiguration(scope, stream, config, null, executor).join();

        UpdateStreamEvent event = new UpdateStreamEvent(scope, stream);

        doAnswer(x -> {
            signal.complete(null);
            wait.join();
            return streamStore1.completeUpdateConfiguration(x.getArgument(0), x.getArgument(1),
                    x.getArgument(2), x.getArgument(3), x.getArgument(4));
        }).when(streamStore1Spied).completeUpdateConfiguration(anyString(), anyString(), any(), any(), any());

        CompletableFuture<Void> future1 = requestHandler1.execute(event);
        signal.join();
        requestHandler2.execute(event).join();
        wait.complete(null);

        AssertExtensions.assertThrows("first update job should fail", () -> future1,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        // validate rolling txn done
        VersionedMetadata<StreamConfigurationRecord> versioned = streamStore1.getConfigurationRecord(scope, stream, null, executor).join();
        assertFalse(versioned.getObject().isUpdating());
        assertEquals(2, versioned.getVersion().asIntVersion().getIntValue().intValue());
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
    }

    // concurrent truncate stream
    @SuppressWarnings("unchecked")
    @Test(timeout = 300000)
    public void concurrentTruncateStream() {
        String stream = "update";
        StreamMetadataStore streamStore1 = StreamStoreFactory.createZKStore(zkClient, executor);
        StreamMetadataStore streamStore1Spied = spy(StreamStoreFactory.createZKStore(zkClient, executor));
        StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();

        StreamMetadataStore streamStore2 = StreamStoreFactory.createZKStore(zkClient, executor);

        TruncateStreamTask requestHandler1 = new TruncateStreamTask(streamMetadataTasks, streamStore1Spied, executor);
        TruncateStreamTask requestHandler2 = new TruncateStreamTask(streamMetadataTasks, streamStore2, executor);

        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();

        Map<Long, Long> map = new HashMap<>();
        map.put(0L, 100L);

        streamStore1.startTruncation(scope, stream, map, null, executor).join();

        TruncateStreamEvent event = new TruncateStreamEvent(scope, stream);

        doAnswer(x -> {
            signal.complete(null);
            wait.join();
            return streamStore1.completeTruncation(x.getArgument(0), x.getArgument(1),
                    x.getArgument(2), x.getArgument(3), x.getArgument(4));
        }).when(streamStore1Spied).completeTruncation(anyString(), anyString(), any(), any(), any());

        CompletableFuture<Void> future1 = requestHandler1.execute(event);
        signal.join();
        requestHandler2.execute(event).join();
        wait.complete(null);

        AssertExtensions.assertThrows("first truncate job should fail", () -> future1,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        // validate rolling txn done
        VersionedMetadata<StreamTruncationRecord> versioned = streamStore1.getTruncationRecord(scope, stream, null, executor).join();
        assertFalse(versioned.getObject().isUpdating());
        assertEquals(2, versioned.getVersion().asIntVersion().getIntValue().intValue());
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
    }
}
