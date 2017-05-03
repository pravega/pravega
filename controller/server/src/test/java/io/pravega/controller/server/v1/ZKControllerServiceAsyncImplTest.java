/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.server.v1;

import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.timeout.TimeoutService;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.timeout.TimerWheelTimeoutService;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.client.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

/**
 * Zookeeper stream store configuration.
 */
public class ZKControllerServiceAsyncImplTest extends ControllerServiceImplTest {

    private TestingServer zkServer;
    private CuratorFramework zkClient;
    private StoreClient storeClient;
    private StreamMetadataTasks streamMetadataTasks;
    private ScheduledExecutorService executorService;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private TimeoutService timeoutService;
    private Cluster cluster;

    @Override
    public void setup() throws Exception {
        final StreamMetadataStore streamStore;
        final HostControllerStore hostStore;
        final TaskMetadataStore taskMetadataStore;
        final SegmentHelper segmentHelper;

        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        storeClient = StoreClientFactory.createZKStoreClient(zkClient);
        executorService = Executors.newScheduledThreadPool(20,
                new ThreadFactoryBuilder().setNameFormat("testpool-%d").build());
        taskMetadataStore = TaskStoreFactory.createStore(storeClient, executorService);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        streamStore = StreamStoreFactory.createZKStore(zkClient, executorService);
        segmentHelper = SegmentHelperMock.getSegmentHelperMock();

        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelper,
                executorService, "host", connectionFactory);

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStore, hostStore, segmentHelper, executorService, "host", connectionFactory);
        streamTransactionMetadataTasks.initializeStreamWriters("commitStream", new EventStreamWriterMock<>(),
                "abortStream", new EventStreamWriterMock<>());
        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                TimeoutServiceConfig.defaultConfig());

        cluster = new ClusterZKImpl(zkClient, ClusterType.CONTROLLER);
        final CountDownLatch latch = new CountDownLatch(1);
        cluster.addListener((type, host) -> latch.countDown());
        cluster.registerHost(new Host("localhost", 9090, null));
        latch.await();

        ControllerService controller = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService, new SegmentHelper(), executorService, cluster);
        controllerService = new ControllerServiceImpl(controller);
    }

    @Override
    public void tearDown() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
        }
        if (timeoutService != null) {
            timeoutService.stopAsync();
            timeoutService.awaitTerminated();
        }
        if (streamMetadataTasks != null) {
            streamMetadataTasks.close();
        }
        if (streamTransactionMetadataTasks != null) {
            streamTransactionMetadataTasks.close();
        }
        if (cluster != null) {
            cluster.close();
        }
        storeClient.close();
        zkClient.close();
        zkServer.close();
    }

    @Test
    public void createTransactionSuccessTest() {
        int segmentsCount = 4;
        createScopeAndStream(SCOPE1, STREAM1, ScalingPolicy.fixed(segmentsCount));
        Controller.CreateTxnResponse response = createTransaction(SCOPE1, STREAM1, 10000, 10000, 10000);
        assertEquals(segmentsCount, response.getActiveSegmentsCount());
    }

    @Test
    public void transactionTests() {
        createScopeAndStream(SCOPE1, STREAM1, ScalingPolicy.fixed(4));
        Controller.TxnId txnId1 = createTransaction(SCOPE1, STREAM1, 10000, 10000, 10000).getTxnId();
        Controller.TxnId txnId2 = createTransaction(SCOPE1, STREAM1, 10000, 10000, 10000).getTxnId();

        // Abort first txn.
        Controller.TxnStatus status = closeTransaction(SCOPE1, STREAM1, txnId1, true);
        Assert.assertEquals(Controller.TxnStatus.Status.SUCCESS, status.getStatus());

        // Commit second txn.
        status = closeTransaction(SCOPE1, STREAM1, txnId2, false);
        Assert.assertEquals(Controller.TxnStatus.Status.SUCCESS, status.getStatus());
    }

    private Controller.TxnStatus closeTransaction(final String scope,
                                                  final String stream,
                                                  final Controller.TxnId txnId,
                                                  final boolean abort) {
        Controller.StreamInfo streamInfo = ModelHelper.createStreamInfo(scope, stream);
        Controller.TxnRequest request = Controller.TxnRequest.newBuilder()
                .setStreamInfo(streamInfo)
                .setTxnId(txnId)
                .build();
        ResultObserver<Controller.TxnStatus> resultObserver = new ResultObserver<>();
        if (abort) {
            this.controllerService.abortTransaction(request, resultObserver);
        } else {
            this.controllerService.commitTransaction(request, resultObserver);
        }
        Controller.TxnStatus status = resultObserver.get();
        Assert.assertNotNull(status);
        return resultObserver.get();
    }

    private Controller.CreateTxnResponse createTransaction(final String scope, final String stream, final long lease,
                                                           final long maxExecutionTime, final long scaleGracePeriod) {
        Controller.StreamInfo streamInfo = ModelHelper.createStreamInfo(scope, stream);
        Controller.CreateTxnRequest request = Controller.CreateTxnRequest.newBuilder()
                .setStreamInfo(streamInfo)
                .setLease(lease)
                .setMaxExecutionTime(maxExecutionTime)
                .setScaleGracePeriod(scaleGracePeriod).build();
        ResultObserver<Controller.CreateTxnResponse> resultObserver = new ResultObserver<>();
        this.controllerService.createTransaction(request, resultObserver);
        Controller.CreateTxnResponse response = resultObserver.get();
        Assert.assertTrue(response != null);
        return response;
    }
}
