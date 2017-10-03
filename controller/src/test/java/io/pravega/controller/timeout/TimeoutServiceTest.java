/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.timeout;

import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for TimeoutService.
 */
@Slf4j
public class TimeoutServiceTest {

    private final static String SCOPE = "SCOPE";
    private final static String STREAM = "STREAM";

    private final static long LEASE = 2000;
    private final static long MAX_EXECUTION_TIME = 5000;
    private final static long SCALE_GRACE_PERIOD = 30000;

    private final StreamMetadataStore streamStore;
    private final TimerWheelTimeoutService timeoutService;
    private final ControllerService controllerService;
    private final ScheduledExecutorService executor;
    private final TestingServer zkTestServer;
    private final CuratorFramework client;
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final StoreClient storeClient;

    public TimeoutServiceTest() throws Exception {

        final String hostId = "host";

        // Instantiate test ZK service.
        zkTestServer = new TestingServerStarter().start();
        String connectionString = zkTestServer.getConnectString();

        // Initialize the executor service.
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "testtaskpool");

        // Initialize ZK client.
        client = CuratorFrameworkFactory.newClient(connectionString, new RetryOneTime(2000));
        client.start();

        // Create STREAM store, host store, and task metadata store.
        storeClient = StoreClientFactory.createZKStoreClient(client);
        streamStore = StreamStoreFactory.createZKStore(client, executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                new SegmentHelper(), executor, hostId, connectionFactory);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, hostStore,
                SegmentHelperMock.getSegmentHelperMock(), executor, hostId, TimeoutServiceConfig.defaultConfig(),
                new LinkedBlockingQueue<>(5), connectionFactory);
        streamTransactionMetadataTasks.initializeStreamWriters("commitStream", new EventStreamWriterMock<>(),
                "abortStream", new EventStreamWriterMock<>());

        // Create TimeoutService
        timeoutService = (TimerWheelTimeoutService) streamTransactionMetadataTasks.getTimeoutService();

        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, new SegmentHelper(), executor, null);

        // Create scope and stream
        streamStore.createScope(SCOPE).join();

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(STREAM)
                .scalingPolicy(ScalingPolicy.fixed(1)).build();

        streamStore.createStream(SCOPE, STREAM, streamConfiguration, System.currentTimeMillis(), null, executor)
                .thenCompose(x -> streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor)).join();
    }

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        executor.shutdown();
        client.close();
        storeClient.close();
        zkTestServer.close();
    }

    @Test(timeout = 10000)
    public void testTimeout() throws InterruptedException {
        UUID txnId = UUID.randomUUID();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD, null, executor).join();

        long begin = System.currentTimeMillis();
        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (1.3 * LEASE), TimeUnit.MILLISECONDS);
        long end = System.currentTimeMillis();
        Assert.assertNotNull(result);

        log.info("Delay until timeout = " + (end - begin));
        Assert.assertTrue((end - begin) >= LEASE);

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);

    }

    @Test(timeout = 10000)
    public void testControllerTimeout() throws InterruptedException {
        long begin = System.currentTimeMillis();
        TxnId txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD)
                .thenApply(x -> ModelHelper.decode(x.getKey()))
                .join();

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (1.3 * LEASE), TimeUnit.MILLISECONDS);
        long end = System.currentTimeMillis();
        Assert.assertNotNull(result);

        log.info("Delay until timeout = " + (end - begin));
        Assert.assertTrue((end - begin) >= LEASE);

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.ABORTING, txnState.getState());
    }

    @Test(timeout = 10000)
    public void testPingSuccess() throws InterruptedException {
        UUID txnId = UUID.randomUUID();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD, null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        PingTxnStatus pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE);
        Assert.assertEquals(PingTxnStatus.Status.OK, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.8 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);
    }

    @Test(timeout = 10000)
    public void testControllerPingSuccess() throws InterruptedException {
        TxnId txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD)
                .thenApply(x -> ModelHelper.decode(x.getKey()))
                .join();

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        PingTxnStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, LEASE).join();
        Assert.assertEquals(PingTxnStatus.Status.OK, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.8 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.ABORTING, txnState.getState());
    }

    @Test(timeout = 30000)
    public void testPingOwnershipTransfer() throws Exception {
        StreamMetadataStore streamStore2 = StreamStoreFactory.createZKStore(client, executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        @Cleanup
        StreamMetadataTasks streamMetadataTasks2 = new StreamMetadataTasks(streamStore2, hostStore, taskMetadataStore,
                new SegmentHelper(), executor, "2", connectionFactory);
        @Cleanup
        StreamTransactionMetadataTasks streamTransactionMetadataTasks2 = new StreamTransactionMetadataTasks(streamStore2, hostStore,
                SegmentHelperMock.getSegmentHelperMock(), executor, "2", TimeoutServiceConfig.defaultConfig(),
                new LinkedBlockingQueue<>(5), connectionFactory);
        streamTransactionMetadataTasks2.initializeStreamWriters("commitStream", new EventStreamWriterMock<>(),
                "abortStream", new EventStreamWriterMock<>());

        // Create TimeoutService
        TimerWheelTimeoutService timeoutService2 = (TimerWheelTimeoutService) streamTransactionMetadataTasks2.getTimeoutService();

        ControllerService controllerService2 = new ControllerService(streamStore2, hostStore, streamMetadataTasks2,
                streamTransactionMetadataTasks2, new SegmentHelper(), executor, null);

        TxnId txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, 2 * MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD)
                .thenApply(x -> ModelHelper.decode(x.getKey()))
                .join();

        VersionedTransactionData txnData = streamStore.getTransactionData(SCOPE, STREAM, ModelHelper.encode(txnId), null, executor).join();
        Assert.assertEquals(txnData.getVersion(), 0);

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        // increasing lease -> total effective lease = 3 * LEASE
        PingTxnStatus pingStatus = controllerService2.pingTransaction(SCOPE, STREAM, txnId, 2 * LEASE).join();
        Assert.assertEquals(PingTxnStatus.Status.OK, pingStatus.getStatus());

        txnData = streamStore.getTransactionData(SCOPE, STREAM, ModelHelper.encode(txnId), null, executor).join();
        Assert.assertEquals(txnData.getVersion(), 1);

        // timeoutService1 should believe that LEASE has expired and should get non empty completion tasks
        result = timeoutService.getTaskCompletionQueue().poll((long) (1.3 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        // the txn may have been attempted to be aborted by timeoutService1 but would have failed. So txn to remain open
        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        // timeoutService2 should continue to wait on lease expiry and should get empty completion tasks
        result = timeoutService2.getTaskCompletionQueue().poll(0L, TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        result = timeoutService2.getTaskCompletionQueue().poll(2 * LEASE, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        // now txn should have moved to aborting because timeoutservice2 has initiated abort
        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.ABORTING, txnState.getState());
    }

    @Test(timeout = 10000)
    public void testPingLeaseTooLarge() {
        UUID txnId = UUID.randomUUID();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD, null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        int version = txData.getVersion();
        PingTxnStatus pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), version, SCALE_GRACE_PERIOD + 1);
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), version, Config.MAX_LEASE_VALUE + 1);
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), version, Config.MAX_SCALE_GRACE_PERIOD + 1);
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), version, MAX_EXECUTION_TIME + 1);
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());
    }

    @Test(timeout = 10000)
    public void testControllerPingLeaseTooLarge() {
        TxnId txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD)
                .thenApply(x -> ModelHelper.decode(x.getKey()))
                .join();

        PingTxnStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, SCALE_GRACE_PERIOD + 1).join();
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, Config.MAX_LEASE_VALUE + 1).join();
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, Config.MAX_SCALE_GRACE_PERIOD + 1).join();
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, MAX_EXECUTION_TIME + 1).join();
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, UUID.randomUUID(), LEASE,
                MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD, null, executor).join();

        txnId = ModelHelper.decode(txData.getId());

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, SCALE_GRACE_PERIOD + 1).join();
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, Config.MAX_LEASE_VALUE + 1).join();
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, Config.MAX_SCALE_GRACE_PERIOD + 1).join();
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, MAX_EXECUTION_TIME + 1).join();
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());
    }

    @Test(timeout = 10000)
    public void testControllerCreateTxnLeaseTooLarge() {
        checkError(controllerService.createTransaction(SCOPE, STREAM, SCALE_GRACE_PERIOD + 1, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD), IllegalArgumentException.class);

        checkError(controllerService.createTransaction(SCOPE, STREAM, MAX_EXECUTION_TIME + 1, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD), IllegalArgumentException.class);

        checkError(controllerService.createTransaction(SCOPE, STREAM, Config.MAX_LEASE_VALUE + 1, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD), IllegalArgumentException.class);

        checkError(controllerService.createTransaction(SCOPE, STREAM, Config.MAX_SCALE_GRACE_PERIOD + 1,
                MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD), IllegalArgumentException.class);

        checkError(controllerService.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                Config.MAX_SCALE_GRACE_PERIOD + 1), IllegalArgumentException.class);
    }

    @Test(timeout = 10000)
    public void testPingFailureMaxExecutionTimeExceeded() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, UUID.randomUUID(), LEASE,
                MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD, null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        // 3 * LEASE > MAX_EXECUTION_TIME
        PingTxnStatus pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), 3 * LEASE);
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);
    }

    @Test(timeout = 10000)
    public void testControllerPingFailureMaxExecutionTimeExceeded() throws InterruptedException {
        TxnId txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD)
                .thenApply(x -> ModelHelper.decode(x.getKey()))
                .join();

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        // 3 * LEASE > MAX_EXECUTION_TIME
        PingTxnStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, 3 * LEASE).join();
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());
    }

    @Test(timeout = 10000)
    public void testPingFailureDisconnected() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, UUID.randomUUID(), LEASE,
                MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD, null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        // Stop timeoutService, and then try pinging the transaction.
        timeoutService.stopAsync();

        PingTxnStatus pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE);
        Assert.assertEquals(PingTxnStatus.Status.DISCONNECTED, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        // Check that the transaction status is still open, since timeoutService has been stopped.
        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);
    }

    @Test(timeout = 10000)
    public void testControllerPingFailureDisconnected() throws InterruptedException {

        TxnId txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD)
                .thenApply(x -> ModelHelper.decode(x.getKey()))
                .join();

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnState status = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.OPEN, status.getState());

        // Stop timeoutService, and then try pinging the transaction.
        timeoutService.stopAsync();

        PingTxnStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, LEASE).join();
        Assert.assertEquals(PingTxnStatus.Status.DISCONNECTED, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        // Check that the transaction status is still open, since timeoutService has been stopped.
        status = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.OPEN, status.getState());
    }

    @Test(timeout = 10000)
    public void testTimeoutTaskFailureInvalidVersion() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, UUID.randomUUID(), LEASE,
                MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD, null, executor).join();

        // Submit transaction to TimeoutService with incorrect tx version identifier.
        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion() + 1, LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (1.25 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(StoreException.WriteConflictException.class, result.get().getClass());

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

    }

    @Test(timeout = 5000)
    public void testCloseUnknownTxn() {
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, UUID.randomUUID(), LEASE,
                MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD, null, executor).join();
        TxnId txnId = convert(txData.getId());

        Controller.TxnState state = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.OPEN, state.getState());

        Controller.TxnStatus.Status status = controllerService.abortTransaction(SCOPE, STREAM, txnId).join().getStatus();
        Assert.assertEquals(Controller.TxnStatus.Status.SUCCESS, status);
    }

    @Test(timeout = 10000)
    public void testUnknownTxnPingSuccess() throws InterruptedException {
        UUID txnId = UUID.randomUUID();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD, null, executor).join();

        TxnId tx = TxnId.newBuilder()
                .setHighBits(txnId.getMostSignificantBits())
                .setLowBits(txnId.getLeastSignificantBits())
                .build();

        controllerService.pingTransaction(SCOPE, STREAM, tx, LEASE);

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);
    }

    private TxnId convert(UUID uuid) {
        return TxnId.newBuilder()
                .setHighBits(uuid.getMostSignificantBits())
                .setLowBits(uuid.getLeastSignificantBits())
                .build();
    }

    private <T> void checkError(CompletableFuture<T> future, Class<? extends Throwable> expectedException) {
        AssertExtensions.assertThrows("Failed future", future, e -> e.getClass().equals(expectedException));
    }
}
