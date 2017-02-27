/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.timeout;

import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.VersionedTransactionData;
import com.emc.pravega.controller.store.stream.WriteConflictException;
import com.emc.pravega.controller.store.stream.ZKStreamMetadataStore;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.v1.PingStatus;
import com.emc.pravega.controller.stream.api.v1.TxnId;
import com.emc.pravega.controller.stream.api.v1.TxnState;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.impl.TxnStatus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Test class for TimeoutService.
 */
@Slf4j
public class TimeoutServiceTest {

    private final static String SCOPE = "SCOPE";
    private final static String STREAM = "STREAM";

    private final static long LEASE = 4000;
    private final static long MAX_EXECUTION_TIME = 10000;
    private final static long SCALE_GRACE_PERIOD = 30000;

    private final StreamMetadataStore streamStore;
    private final TimerWheelTimeoutService timeoutService;
    private final ControllerService controllerService;

    @Slf4j
    private static class DummyStreamTransactionTasks extends StreamTransactionMetadataTasks {

        private final StreamMetadataStore streamMetadataStore;

        public DummyStreamTransactionTasks(final StreamMetadataStore streamMetadataStore,
                                           final HostControllerStore hostControllerStore,
                                           final TaskMetadataStore taskMetadataStore,
                                           final ScheduledExecutorService executor, String hostId) {
            super(streamMetadataStore, hostControllerStore, taskMetadataStore, executor, hostId);
            this.streamMetadataStore = streamMetadataStore;
        }

        @Override
        public CompletableFuture<VersionedTransactionData> createTx(final String scope, final String stream,
                                                                    final long lease, final long maxExecutionTime,
                                                                    final long scaleGracePeriod) {
            return streamMetadataStore.createTransaction(scope, stream, lease, maxExecutionTime, scaleGracePeriod)
                    .thenApply(txData -> {
                        log.info("Created transaction {} with version {}", txData.getId(), txData.getVersion());
                        return txData;
                    });
        }

        @Override
        public CompletableFuture<TxnStatus> abortTx(final String scope, final String stream, final UUID txId,
                                                    final Optional<Integer> version) {
            return this.streamMetadataStore.sealTransaction(scope, stream, txId, false, version)
                    .thenApply(status -> {
                        log.info("Sealed:abort transaction {} with version {}", txId, version);
                        return status;
                    });
        }

        @Override
        public CompletableFuture<VersionedTransactionData> pingTx(final String scope, final String stream,
                                                                  final UUID txId, final long lease) {
            return streamMetadataStore.pingTransaction(scope, stream, txId, lease)
                    .thenApply(txData -> {
                        log.info("Pinged transaction {} with version {}", txId, txData.getVersion());
                        return txData;
                    });
        }

        @Override
        public CompletableFuture<TxnStatus> commitTx(final String scope, final String stream, final UUID txId) {
            return this.streamMetadataStore.sealTransaction(scope, stream, txId, true, Optional.<Integer>empty())
                    .thenApply(status -> {
                        log.info("Sealed:commit transaction {} with version {}", txId, null);
                        return status;
                    });
        }
    }

    public TimeoutServiceTest() throws Exception {

        final String hostId = "host";

        // Instantiate test ZK service.
        TestingServer zkTestServer = new TestingServer();
        String connectionString = zkTestServer.getConnectString();

        // Initialize the executor service.
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4,
                new ThreadFactoryBuilder().setNameFormat("testtaskpool-%d").build());

        // Initialize ZK client.
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, new RetryOneTime(2000));
        client.start();

        // Create STREAM store, host store, and task metadata store.
        StoreClient storeClient = new ZKStoreClient(client);
        streamStore = new ZKStreamMetadataStore(client, executor);
        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new DummyStreamTransactionTasks(streamStore,
                hostStore, taskMetadataStore, executor, hostId);

        // Create TimeoutService
        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks, new LinkedBlockingQueue<>(5));

        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService);
    }

    @Test
    public void testTimeout() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD).join();

        long begin = System.currentTimeMillis();
        timeoutService.addTx(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (1.3 * LEASE), TimeUnit.MILLISECONDS);
        long end = System.currentTimeMillis();
        Assert.assertNotNull(result);

        log.info("Delay until timeout = " + (end - begin));
        Assert.assertTrue((end - begin) >= LEASE);

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId()).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);

    }

    @Test
    public void testControllerTimeout() throws InterruptedException {
        long begin = System.currentTimeMillis();
        TxnId txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD)
                .join();

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (1.3 * LEASE), TimeUnit.MILLISECONDS);
        long end = System.currentTimeMillis();
        Assert.assertNotNull(result);

        log.info("Delay until timeout = " + (end - begin));
        Assert.assertTrue((end - begin) >= LEASE);

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.ABORTING, txnState);
    }

    @Test
    public void testPingSuccess() throws InterruptedException {
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD).join();

        timeoutService.addTx(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        PingStatus pingStatus = timeoutService.pingTx(SCOPE, STREAM, txData.getId(), LEASE);
        Assert.assertEquals(PingStatus.OK, pingStatus);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.8 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId()).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);
    }

    @Test
    public void testControllerPingSuccess() throws InterruptedException {
        TxnId txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD)
                .join();

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.OPEN, txnState);

        PingStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, LEASE).join();
        Assert.assertEquals(PingStatus.OK, pingStatus);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.OPEN, txnState);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.8 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.ABORTING, txnState);
    }

    @Test
    public void testPingFailureMaxExecutionTimeExceeded() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD).join();

        timeoutService.addTx(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        PingStatus pingStatus = timeoutService.pingTx(SCOPE, STREAM, txData.getId(), LEASE);
        Assert.assertEquals(PingStatus.OK, pingStatus);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        pingStatus = timeoutService.pingTx(SCOPE, STREAM, txData.getId(), LEASE + 1);
        Assert.assertEquals(PingStatus.MAX_EXECUTION_TIME_EXCEEDED, pingStatus);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId()).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);
    }

    @Test
    public void testControllerPingFailureMaxExecutionTimeExceeded() throws InterruptedException {
        TxnId txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD)
                .join();

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.OPEN, txnState);

        PingStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, LEASE).join();
        Assert.assertEquals(PingStatus.OK, pingStatus);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.OPEN, txnState);

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, LEASE + 1).join();
        Assert.assertEquals(PingStatus.MAX_EXECUTION_TIME_EXCEEDED, pingStatus);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.ABORTING, txnState);
    }

    @Test
    public void testPingFailureDisconnected() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD).join();

        timeoutService.addTx(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        // Stop timeoutService, and then try pinging the transaction.
        timeoutService.stopAsync();

        PingStatus pingStatus = timeoutService.pingTx(SCOPE, STREAM, txData.getId(), LEASE);
        Assert.assertEquals(PingStatus.DISCONNECTED, pingStatus);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        // Check that the transaction status is still open, since timeoutService has been stopped.
        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);
    }

    @Test
    public void testControllerPingFailureDisconnected() throws InterruptedException {

        TxnId txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME, SCALE_GRACE_PERIOD)
                .join();

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnState status = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.OPEN, status);

        // Stop timeoutService, and then try pinging the transaction.
        timeoutService.stopAsync();

        PingStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, LEASE).join();
        Assert.assertEquals(PingStatus.DISCONNECTED, pingStatus);

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        // Check that the transaction status is still open, since timeoutService has been stopped.
        status = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId).join();
        Assert.assertEquals(TxnState.OPEN, status);
    }

    @Test
    public void testTimeoutTaskFailureInvalidVersion() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, LEASE, MAX_EXECUTION_TIME,
                SCALE_GRACE_PERIOD).join();

        // Submit transaction to TimeoutService with incorrect tx version identifier.
        timeoutService.addTx(SCOPE, STREAM, txData.getId(), txData.getVersion() + 1, LEASE,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (1.25 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(WriteConflictException.class, result.get().getClass());

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

    }
}
