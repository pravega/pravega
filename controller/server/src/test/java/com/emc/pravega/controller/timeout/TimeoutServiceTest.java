/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.timeout;

import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.VersionedTransactionData;
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
import java.util.concurrent.ScheduledExecutorService;

/**
 * Test class for TimeoutService.
 */
@Slf4j
public class TimeoutServiceTest {

    private static String scope = "scope";
    private static String stream = "stream";

    private static long lease = 4000;
    private static long maxExecutionTime = 10000;
    private static long scaleGracePeriod = 30000;

    private final StreamMetadataStore streamStore;
    private final TimeoutService timeoutService;
    private final ControllerService controllerService;

    @Slf4j
    private static class DummyStreamTransactionTasks extends StreamTransactionMetadataTasks {

        private final StreamMetadataStore streamMetadataStore;

        public DummyStreamTransactionTasks(StreamMetadataStore streamMetadataStore, HostControllerStore hostControllerStore, TaskMetadataStore taskMetadataStore, ScheduledExecutorService executor, String hostId) {
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

        // Create stream store, host store, and task metadata store.
        StoreClient storeClient = new ZKStoreClient(client);
        streamStore = new ZKStreamMetadataStore(client, executor);
        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new DummyStreamTransactionTasks(streamStore,
                hostStore, taskMetadataStore, executor, hostId);

        // Create TimeoutService
        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks);

        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService);
    }

    @Test
    public void testTimeout() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(scope, stream, lease, maxExecutionTime,
                scaleGracePeriod).join();

        timeoutService.addTx(scope, stream, txData.getId(), txData.getVersion(), lease,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Thread.sleep(5200);

        TxnStatus status = streamStore.transactionStatus(scope, stream, txData.getId()).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);

    }

    @Test
    public void testControllerTimeout() throws InterruptedException {
        TxnId txnId = controllerService.createTransaction(scope, stream, lease, maxExecutionTime, scaleGracePeriod)
                .join();

        Thread.sleep(5200);

        TxnState txnState = controllerService.checkTransactionStatus(scope, stream, txnId).join();
        Assert.assertEquals(TxnState.ABORTING, txnState);
    }

    @Test
    public void testPingSuccess() throws InterruptedException {
        VersionedTransactionData txData = streamStore.createTransaction(scope, stream, lease, maxExecutionTime,
                scaleGracePeriod).join();

        timeoutService.addTx(scope, stream, txData.getId(), txData.getVersion(), lease,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Thread.sleep(3000);

        TxnStatus status = streamStore.transactionStatus(scope, stream, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        PingStatus pingStatus = timeoutService.pingTx(scope, stream, txData.getId(), lease);
        Assert.assertEquals(PingStatus.OK, pingStatus);

        Thread.sleep(2000);

        status = streamStore.transactionStatus(scope, stream, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        Thread.sleep(3200);

        status = streamStore.transactionStatus(scope, stream, txData.getId()).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);
    }

    @Test
    public void testControllerPingSuccess() throws InterruptedException {
        TxnId txnId = controllerService.createTransaction(scope, stream, lease, maxExecutionTime, scaleGracePeriod)
                .join();

        Thread.sleep(3000);

        TxnState txnState = controllerService.checkTransactionStatus(scope, stream, txnId).join();
        Assert.assertEquals(TxnState.OPEN, txnState);

        PingStatus pingStatus = controllerService.pingTransaction(scope, stream, txnId, lease).join();
        Assert.assertEquals(PingStatus.OK, pingStatus);

        Thread.sleep(2000);

        txnState = controllerService.checkTransactionStatus(scope, stream, txnId).join();
        Assert.assertEquals(TxnState.OPEN, txnState);

        Thread.sleep(3200);

        txnState = controllerService.checkTransactionStatus(scope, stream, txnId).join();
        Assert.assertEquals(TxnState.ABORTING, txnState);
    }

    @Test
    public void testPingFailureMaxExecutionTimeExceeded() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(scope, stream, lease, maxExecutionTime,
                scaleGracePeriod).join();

        timeoutService.addTx(scope, stream, txData.getId(), txData.getVersion(), lease,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Thread.sleep(3000);

        TxnStatus status = streamStore.transactionStatus(scope, stream, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        PingStatus pingStatus = timeoutService.pingTx(scope, stream, txData.getId(), lease);
        Assert.assertEquals(PingStatus.OK, pingStatus);

        Thread.sleep(3000);

        status = streamStore.transactionStatus(scope, stream, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        pingStatus = timeoutService.pingTx(scope, stream, txData.getId(), lease + 1);
        Assert.assertEquals(PingStatus.MAX_EXECUTION_TIME_EXCEEDED, pingStatus);

        Thread.sleep(2000);

        status = streamStore.transactionStatus(scope, stream, txData.getId()).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);
    }

    @Test
    public void testControllerPingFailureMaxExecutionTimeExceeded() throws InterruptedException {
        TxnId txnId = controllerService.createTransaction(scope, stream, lease, maxExecutionTime, scaleGracePeriod)
                .join();

        Thread.sleep(3000);

        TxnState txnState = controllerService.checkTransactionStatus(scope, stream, txnId).join();
        Assert.assertEquals(TxnState.OPEN, txnState);

        PingStatus pingStatus = controllerService.pingTransaction(scope, stream, txnId, lease).join();
        Assert.assertEquals(PingStatus.OK, pingStatus);

        Thread.sleep(3000);

        txnState = controllerService.checkTransactionStatus(scope, stream, txnId).join();
        Assert.assertEquals(TxnState.OPEN, txnState);

        pingStatus = controllerService.pingTransaction(scope, stream, txnId, lease + 1).join();
        Assert.assertEquals(PingStatus.MAX_EXECUTION_TIME_EXCEEDED, pingStatus);

        Thread.sleep(2000);

        txnState = controllerService.checkTransactionStatus(scope, stream, txnId).join();
        Assert.assertEquals(TxnState.ABORTING, txnState);
    }

    @Test
    public void testPingFailureDisconnected() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(scope, stream, lease, maxExecutionTime,
                scaleGracePeriod).join();

        timeoutService.addTx(scope, stream, txData.getId(), txData.getVersion(), lease,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Thread.sleep(3 * lease / 4);

        TxnStatus status = streamStore.transactionStatus(scope, stream, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

        // Stop timeoutService, and then try pinging the transaction.
        timeoutService.stopAsync();

        PingStatus pingStatus = timeoutService.pingTx(scope, stream, txData.getId(), lease);
        Assert.assertEquals(PingStatus.DISCONNECTED, pingStatus);

        Thread.sleep(lease / 2);

        // Check that the transaction status is still open, since timeoutService has been stopped.
        status = streamStore.transactionStatus(scope, stream, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);
    }

    @Test
    public void testControllerPingFailureDisconnected() throws InterruptedException {

        TxnId txnId = controllerService.createTransaction(scope, stream, lease, maxExecutionTime, scaleGracePeriod)
                .join();

        Thread.sleep(3 * lease / 4);

        TxnState status = controllerService.checkTransactionStatus(scope, stream, txnId).join();
        Assert.assertEquals(TxnState.OPEN, status);

        // Stop timeoutService, and then try pinging the transaction.
        timeoutService.stopAsync();

        PingStatus pingStatus = controllerService.pingTransaction(scope, stream, txnId, lease).join();
        Assert.assertEquals(PingStatus.DISCONNECTED, pingStatus);

        Thread.sleep(lease / 2);

        // Check that the transaction status is still open, since timeoutService has been stopped.
        status = controllerService.checkTransactionStatus(scope, stream, txnId).join();
        Assert.assertEquals(TxnState.OPEN, status);
    }

    @Test
    public void testTimeoutTaskFailureInvalidVersion() throws InterruptedException {

        VersionedTransactionData txData = streamStore.createTransaction(scope, stream, lease, maxExecutionTime,
                scaleGracePeriod).join();

        // Submit transaction to TimeoutService with incorrect tx version identifier.
        timeoutService.addTx(scope, stream, txData.getId(), txData.getVersion() + 1, lease,
                txData.getMaxExecutionExpiryTime(), txData.getScaleGracePeriod());

        Thread.sleep(lease + lease / 4);

        TxnStatus status = streamStore.transactionStatus(scope, stream, txData.getId()).join();
        Assert.assertEquals(TxnStatus.OPEN, status);

    }
}
