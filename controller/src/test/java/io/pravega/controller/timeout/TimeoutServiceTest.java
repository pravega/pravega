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
package io.pravega.controller.timeout;

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.test.common.AssertExtensions;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.RetryOneTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.mockito.Mock;

/**
 * Test class for TimeoutService.
 */
@Slf4j
public abstract class TimeoutServiceTest {

    private static final  RetryPolicy RETRY_POLICY = new RetryOneTime(2000);
    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource(RETRY_POLICY);
    private final static String SCOPE = "SCOPE";
    private final static String STREAM = "STREAM";

    private final static long LEASE = 2000;
    private final static int RETRY_DELAY = 1000;

    protected ScheduledExecutorService executor;
    protected CuratorFramework client;
    protected SegmentHelper segmentHelper;

    private StreamMetadataStore streamStore;
    private TimerWheelTimeoutService timeoutService;
    private ControllerService controllerService;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private RequestTracker requestTracker = new RequestTracker(true);
    @Mock
    private KVTableMetadataStore kvtStore;
    @Mock
    private TableMetadataTasks kvtMetadataTasks;

    @Before
    public void setUp() throws Exception {
        final String hostId = "host";

        // Initialize the executor service.
        executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");
        segmentHelper = getSegmentHelper();
        streamStore = getStore();
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(PRAVEGA_ZK_CURATOR_RESOURCE.storeClient, executor);

        StreamMetrics.initialize();
        TransactionMetrics.initialize();
        streamMetadataTasks = new StreamMetadataTasks(streamStore, StreamStoreFactory.createInMemoryBucketStore(), taskMetadataStore,
                SegmentHelperMock.getSegmentHelperMock(), executor, hostId, GrpcAuthHelper.getDisabledAuthHelper());
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                SegmentHelperMock.getSegmentHelperMock(), executor, hostId, TimeoutServiceConfig.defaultConfig(),
                new LinkedBlockingQueue<>(5), GrpcAuthHelper.getDisabledAuthHelper());
        streamTransactionMetadataTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());

        // Create TimeoutService
        timeoutService = (TimerWheelTimeoutService) streamTransactionMetadataTasks.getTimeoutService();
        BucketStore bucketStore = StreamStoreFactory.createInMemoryBucketStore();

        controllerService = new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks,
                streamTransactionMetadataTasks, SegmentHelperMock.getSegmentHelperMock(), executor, null, requestTracker);

        // Create scope and stream
        streamStore.createScope(SCOPE, null, executor).join();

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1)).build();

        streamStore.createStream(SCOPE, STREAM, streamConfiguration, System.currentTimeMillis(), null, executor)
                .thenCompose(x -> streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor)).join();
    }

    abstract SegmentHelper getSegmentHelper();

    abstract StreamMetadataStore getStore();

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        streamStore.close();
        ExecutorServiceHelpers.shutdown(executor);
        timeoutService.stopAsync();
        timeoutService.awaitTerminated();
        StreamMetrics.reset();
        TransactionMetrics.reset();
    }

    @Test(timeout = 10000)
    public void testTimeout() throws InterruptedException {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId, LEASE, 10 * LEASE,
                null, executor).join();

        long begin = System.currentTimeMillis();
        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime());

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
        UUID txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, 0L)
                .thenApply(x -> x.getKey())
                .join();

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue()
                .poll((long) (1.3 * LEASE), TimeUnit.MILLISECONDS);
        long end = System.currentTimeMillis();
        Assert.assertNotNull(result);

        log.info("Delay until timeout = " + (end - begin));
        Assert.assertTrue((end - begin) >= LEASE);

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 9L).join();
        Assert.assertEquals(TxnState.State.ABORTING, txnState.getState());
    }

    @Test(timeout = 10000)
    public void testPingSuccess() throws InterruptedException {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId, LEASE, 10 * LEASE,
                null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime());

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
        UUID txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, 9L)
                .thenApply(x -> x.getKey())
                .join();

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 9L).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        PingTxnStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, LEASE, 9L).join();
        Assert.assertEquals(PingTxnStatus.Status.OK, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 9L).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.8 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 9L).join();
        Assert.assertEquals(TxnState.State.ABORTING, txnState.getState());
    }

    @Test(timeout = 30000)
    public void testPingOwnershipTransfer() throws Exception {
        StreamMetadataStore streamStore2 = getStore();
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        BucketStore bucketStore = StreamStoreFactory.createInMemoryBucketStore();
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(PRAVEGA_ZK_CURATOR_RESOURCE.storeClient, executor);

        SegmentHelper helperMock = SegmentHelperMock.getSegmentHelperMock();
        @Cleanup
        StreamMetadataTasks streamMetadataTasks2 = new StreamMetadataTasks(streamStore2, bucketStore, taskMetadataStore,
                helperMock, executor, "2", GrpcAuthHelper.getDisabledAuthHelper());
        @Cleanup
        StreamTransactionMetadataTasks streamTransactionMetadataTasks2 = new StreamTransactionMetadataTasks(streamStore2,
                helperMock, executor, "2", TimeoutServiceConfig.defaultConfig(),
                new LinkedBlockingQueue<>(5), GrpcAuthHelper.getDisabledAuthHelper());
        streamTransactionMetadataTasks2.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());

        // Create TimeoutService
        TimerWheelTimeoutService timeoutService2 = (TimerWheelTimeoutService) streamTransactionMetadataTasks2.getTimeoutService();

        ControllerService controllerService2 = new ControllerService(kvtStore, kvtMetadataTasks, streamStore2, bucketStore, streamMetadataTasks2,
                streamTransactionMetadataTasks2, helperMock, executor, null, requestTracker);

        UUID txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, 9L)
                .thenApply(x -> x.getKey())
                .join();

        VersionedTransactionData txnData = streamStore.getTransactionData(SCOPE, STREAM, txnId, null, executor).join();
        Assert.assertEquals(txnData.getVersion(), getVersion(0));

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 9L).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        // increasing lease -> total effective lease = 3 * LEASE
        PingTxnStatus pingStatus = controllerService2.pingTransaction(SCOPE, STREAM, txnId, 2 * LEASE, 9L).join();
        Assert.assertEquals(PingTxnStatus.Status.OK, pingStatus.getStatus());

        txnData = streamStore.getTransactionData(SCOPE, STREAM, txnId, null, executor).join();
        Assert.assertEquals(txnData.getVersion(), getVersion(1));

        // timeoutService1 should believe that LEASE has expired and should get non empty completion tasks
        result = timeoutService.getTaskCompletionQueue().poll((long) (1.3 * LEASE + RETRY_DELAY), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        // the txn may have been attempted to be aborted by timeoutService1 but would have failed. So txn to remain open
        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 9L).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        // timeoutService2 should continue to wait on lease expiry and should get empty completion tasks
        result = timeoutService2.getTaskCompletionQueue().poll(0L, TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        result = timeoutService2.getTaskCompletionQueue().poll(2 * LEASE + RETRY_DELAY, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);

        // now txn should have moved to aborting because timeoutservice2 has initiated abort
        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 9L).join();
        Assert.assertEquals(TxnState.State.ABORTING, txnState.getState());
    }

    abstract Version getVersion(int i);

    @Test(timeout = 10000)
    public void testPingLeaseTooLarge() {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId, LEASE, 10 * LEASE,
                null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime());

        Version version = txData.getVersion();
        PingTxnStatus pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), version, Config.MAX_LEASE_VALUE + 1);
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), version, 10 * LEASE + 1);
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());
    }

    @Test(timeout = 10000)
    public void testControllerPingLeaseTooLarge() {
        int lease = 10;
        UUID txnId = controllerService.createTransaction(SCOPE, STREAM, lease, 9L)
                .thenApply(x -> x.getKey())
                .join();

        PingTxnStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, Config.MAX_LEASE_VALUE + 1, 9L).join();
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, 1000 * lease + 1, 9L).join();
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());

        UUID txnId1 = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId1, LEASE,
                2 * LEASE, null, executor).join();

        txnId = txData.getId();

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, Config.MAX_LEASE_VALUE + 1, 9L).join();
        Assert.assertEquals(PingTxnStatus.Status.LEASE_TOO_LARGE, pingStatus.getStatus());

        pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, 3 * LEASE, 9L).join();
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());
    }

    @Test(timeout = 10000)
    public void testControllerCreateTxnLeaseTooLarge() {
        checkError(controllerService.createTransaction(SCOPE, STREAM, Config.MAX_LEASE_VALUE + 1, 9L),
                IllegalArgumentException.class);
    }

    @Test(timeout = 10000)
    public void testPingFailureMaxExecutionTimeExceeded() throws InterruptedException {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId, LEASE,
                2 * LEASE, null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime());

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        // 3 * LEASE > 2 * LEASE
        PingTxnStatus pingStatus = timeoutService.pingTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), 3 * LEASE);
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());

        status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);
    }

    @Test(timeout = 10000)
    public void testControllerPingFailureMaxExecutionTimeExceeded() throws InterruptedException {
        int lease = 10;
        UUID txnId = controllerService.createTransaction(SCOPE, STREAM, lease, 9L)
                .thenApply(x -> x.getKey())
                .join();

        TxnState txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 9L).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());

        PingTxnStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, 1000 * lease, 9L).join();
        Assert.assertEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, pingStatus.getStatus());

        txnState = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 9L).join();
        Assert.assertEquals(TxnState.State.OPEN, txnState.getState());
    }

    @Test(timeout = 10000)
    public void testPingFailureDisconnected() throws InterruptedException {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId, LEASE,
                10 * LEASE, null, executor).join();

        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), txData.getVersion(), LEASE,
                txData.getMaxExecutionExpiryTime());

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

        UUID txnId = controllerService.createTransaction(SCOPE, STREAM, LEASE, 0L)
                .thenApply(x -> x.getKey())
                .join();

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (0.75 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        TxnState status = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 0L).join();
        Assert.assertEquals(TxnState.State.OPEN, status.getState());

        // Stop timeoutService, and then try pinging the transaction.
        timeoutService.stopAsync();

        PingTxnStatus pingStatus = controllerService.pingTransaction(SCOPE, STREAM, txnId, LEASE, 0L).join();
        Assert.assertEquals(PingTxnStatus.Status.DISCONNECTED, pingStatus.getStatus());

        result = timeoutService.getTaskCompletionQueue().poll((long) (0.5 * LEASE), TimeUnit.MILLISECONDS);
        Assert.assertNull(result);

        // Check that the transaction status is still open, since timeoutService has been stopped.
        status = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 0L).join();
        Assert.assertEquals(TxnState.State.OPEN, status.getState());
    }

    @Test(timeout = 10000)
    public void testTimeoutTaskFailureInvalidVersion() throws InterruptedException {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId, LEASE,
                10 * LEASE, null, executor).join();

        // Submit transaction to TimeoutService with incorrect tx version identifier.
        timeoutService.addTxn(SCOPE, STREAM, txData.getId(), getNextVersion(txData.getVersion()), LEASE,
                txData.getMaxExecutionExpiryTime());

        Optional<Throwable> result = timeoutService.getTaskCompletionQueue().poll((long) (1.25 * LEASE + RETRY_DELAY), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(StoreException.WriteConflictException.class, result.get().getClass());

        TxnStatus status = streamStore.transactionStatus(SCOPE, STREAM, txData.getId(), null, executor).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

    }

    abstract Version getNextVersion(Version version);

    @Test(timeout = 5000)
    public void testCloseUnknownTxn() {
        UUID txId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txId, LEASE,
                10 * LEASE, null, executor).join();
        UUID txnId = txData.getId();

        Controller.TxnState state = controllerService.checkTransactionStatus(SCOPE, STREAM, txnId, 0L).join();
        Assert.assertEquals(TxnState.State.OPEN, state.getState());

        Controller.TxnStatus.Status status = controllerService.abortTransaction(SCOPE, STREAM, txnId, 0L).join().getStatus();
        Assert.assertEquals(Controller.TxnStatus.Status.SUCCESS, status);
    }

    @Test(timeout = 10000)
    public void testUnknownTxnPingSuccess() throws InterruptedException {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txData = streamStore.createTransaction(SCOPE, STREAM, txnId, LEASE, 10 * LEASE,
                null, executor).join();

        controllerService.pingTransaction(SCOPE, STREAM, txnId, LEASE, 0L).join();

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
        AssertExtensions.assertFutureThrows("Failed future", future, e -> Exceptions.unwrap(e).getClass().equals(expectedException));
    }
}