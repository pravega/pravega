/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.timeout;

import com.emc.pravega.common.util.ZKCuratorUtils;
import com.emc.pravega.controller.mocks.MockStreamTransactionMetadataTasks;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreClientFactory;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.stream.TxnStatus;
import com.emc.pravega.controller.store.stream.VersionedTransactionData;
import com.emc.pravega.controller.store.stream.WriteConflictException;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    private final MockStreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final StoreClient storeClient;

    public TimeoutServiceTest() throws Exception {

        final String hostId = "host";

        // Instantiate test ZK service.
        zkTestServer = ZKCuratorUtils.createTestServer();
        String connectionString = zkTestServer.getConnectString();

        // Initialize the executor service.
        this.executor = Executors.newScheduledThreadPool(4,
                new ThreadFactoryBuilder().setNameFormat("testtaskpool-%d").build());

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
        streamTransactionMetadataTasks = new MockStreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, new SegmentHelper(), executor, hostId, connectionFactory);

        // Create TimeoutService
        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                TimeoutServiceConfig.defaultConfig(), new LinkedBlockingQueue<>(5));

        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService, new SegmentHelper(), executor);

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

    @Test
    public void testTimeout() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
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
        Assert.assertEquals(TxnStatus.ABORTED, status);

    }

    @Test
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
        Assert.assertEquals(TxnState.State.ABORTED, txnState.getState());
    }

    @Test
    public void testPingSuccess() throws InterruptedException {
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD, null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        PingTxnStatus pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), LEASE);
        Assert.assertEquals(PingTxnStatus.Status.OK, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.8 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.ABORTED, status);
    }

    @Test
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
        Assert.assertEquals(TxnState.State.ABORTED, txnState.getState());
    }

    @Test
    public void testPingLeaseTooLarge() {
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD, null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        PingTxnStatus pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), SCALE_GRACE_PERIOD + 1);
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), Config.MAX_LEASE_VALUE + 1);
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), Config.MAX_SCALE_GRACE_PERIOD + 1);
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), MAX_EXECUTION_TIME + 1);
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());
    }

    @Test
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

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD, null, executor).join();

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

    @Test
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

    @Test
    public void testPingFailureMaxExecutionTimeExceeded() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD, null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        PingTxnStatus pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), LEASE);
        Assert.assertEquals(PingTxnStatus.Status.OK, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), LEASE + 1);
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.ABORTED, status);
    }

    @Test
    public void testControllerPingFailureMaxExecutionTimeExceeded() throws InterruptedException {
        TxnId txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD)
                .thenApply(x -> ModelHelper.decode(x.getKey()))
                .join();

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        PingTxnStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, LEASE).join();
        Assert.assertEquals(PingTxnStatus.Status.OK, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, LEASE + 1).join();
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.State.ABORTED, txnState.getState());
    }

    @Test
    public void testPingFailureDisconnected() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD, null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        // Stop timeoutService, and then try pinging the transaction.
        timeoutService.stopAsync();

        PingTxnStatus pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), LEASE);
        Assert.assertEquals(PingTxnStatus.Status.DISCONNECTED, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        // Check that the transaction status is still open, since timeoutService has been stopped.
        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);
    }

    @Test
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

    @Test
    public void testTimeoutTaskFailureInvalidVersion() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD, null, executor).join();

        // Submit transaction to TimeoutService with incorrect tx version identifier.
        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion() + 1, LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (1.25 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(WriteConflictException.class, result.get().getClass());

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

    }

    private <T> void checkError(CompletableFuture<T> future, Class<? extends Throwable> expectedException) {
        try {
            future.join();
            Assert.assertTrue(false);
        } catch (CompletionException ce) {
            Assert.assertEquals(expectedException, ce.getCause().getClass());
        }
    }
}
