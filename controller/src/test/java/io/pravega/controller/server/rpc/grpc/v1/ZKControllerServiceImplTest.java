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
package io.pravega.controller.server.rpc.grpc.v1;

import io.grpc.StatusRuntimeException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.common.Exceptions;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.ControllerEventStreamWriterMock;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.mocks.ControllerEventTableWriterMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.bucket.BucketManager;
import io.pravega.controller.server.bucket.BucketServiceFactory;
import io.pravega.controller.server.bucket.PeriodicRetention;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.CreateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteScopeTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.CreateTableTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.DeleteTableTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.TableRequestHandler;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.kvtable.AbstractKVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableStoreFactory;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactoryForTests;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Zookeeper stream store configuration.
 */
public class ZKControllerServiceImplTest extends ControllerServiceImplTest {

    private TestingServer zkServer;
    private CuratorFramework zkClient;
    private StoreClient storeClient;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamRequestHandler streamRequestHandler;
    private TaskMetadataStore taskMetadataStore;

    private ScheduledExecutorService executorService;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private Cluster cluster;
    private StreamMetadataStore streamStore;

    private KVTableMetadataStore kvtStore;
    private TableMetadataTasks kvtMetadataTasks;
    private TableRequestHandler tableRequestHandler;
    private BucketManager retentionService;
    private BucketStore bucketStore;

    @Override
    public ControllerService getControllerService() throws Exception {
        final HostControllerStore hostStore;
        final SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        StreamMetrics.initialize();
        TransactionMetrics.initialize();

        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        storeClient = StoreClientFactory.createZKStoreClient(zkClient);
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(20, "testpool");
        taskMetadataStore = TaskStoreFactoryForTests.createStore(storeClient, executorService);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        streamStore = StreamStoreFactory.createZKStore(zkClient, executorService);
        kvtStore = KVTableStoreFactory.createZKStore(zkClient, executorService);
        EventHelper tableEventHelper = EventHelperMock.getEventHelperMock(executorService, "host",
                ((AbstractKVTableMetadataStore) kvtStore).getHostTaskIndex());
        this.kvtStore = KVTableStoreFactory.createZKStore(zkClient, executorService);
        this.kvtMetadataTasks = new TableMetadataTasks(kvtStore, segmentHelper, executorService, executorService,
                "host", GrpcAuthHelper.getDisabledAuthHelper(), tableEventHelper);
        this.tableRequestHandler = new TableRequestHandler(new CreateTableTask(this.kvtStore, this.kvtMetadataTasks,
                executorService), new DeleteTableTask(this.kvtStore, this.kvtMetadataTasks,
                executorService), this.kvtStore, executorService);
        bucketStore = StreamStoreFactory.createZKBucketStore(zkClient, executorService);
        EventHelper helperMock = EventHelperMock.getEventHelperMock(executorService, "host", ((AbstractStreamMetadataStore) streamStore).getHostTaskIndex());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelper,
                executorService, "host", GrpcAuthHelper.getDisabledAuthHelper(), helperMock);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelper,
                executorService, "host", GrpcAuthHelper.getDisabledAuthHelper());
        this.streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executorService),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executorService),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, executorService),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executorService),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executorService),
                new TruncateStreamTask(streamMetadataTasks, streamStore, executorService),
                new CreateReaderGroupTask(streamMetadataTasks, streamStore, executorService),
                new DeleteReaderGroupTask(streamMetadataTasks, streamStore, executorService),
                new UpdateReaderGroupTask(streamMetadataTasks, streamStore, executorService),
                streamStore,
                new DeleteScopeTask(streamMetadataTasks, streamStore, kvtStore, kvtMetadataTasks, executorService),
                executorService);

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, executorService));

        streamTransactionMetadataTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());

        tableEventHelper.setRequestEventWriter(new ControllerEventTableWriterMock(tableRequestHandler, executorService));

        cluster = new ClusterZKImpl(zkClient, ClusterType.CONTROLLER);
        final CountDownLatch latch = new CountDownLatch(1);
        cluster.addListener((type, host) -> latch.countDown());
        cluster.registerHost(new Host("localhost", 9090, null));
        latch.await();

        BucketServiceFactory bucketServiceFactory = new BucketServiceFactory("host", bucketStore, 1000,
                1);
        PeriodicRetention retentionWork = new PeriodicRetention(streamStore, streamMetadataTasks, executorService, requestTracker);
        retentionService = bucketServiceFactory.createRetentionService(Duration.ofMinutes(30), retentionWork::retention, executorService);

        return new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks,
                streamTransactionMetadataTasks, segmentHelper, executorService, cluster, requestTracker);
    }

    @Override
    BucketManager getBucketManager() {
        return retentionService;
    }

    @After
    public void tearDown() throws Exception {
        if (executorService != null) {
            ExecutorServiceHelpers.shutdown(executorService);
        }
        if (streamMetadataTasks != null) {
            streamMetadataTasks.close();
        }
        if (streamTransactionMetadataTasks != null) {
            streamTransactionMetadataTasks.close();
        }
        streamStore.close();
        kvtStore.close();
        if (cluster != null) {
            cluster.close();
        }
        storeClient.close();
        zkClient.close();
        zkServer.close();
        StreamMetrics.reset();
        TransactionMetrics.reset();
    }

    @Test
    public void createTransactionSuccessTest() {
        int segmentsCount = 4;
        createScopeAndStream(SCOPE1, STREAM1, ScalingPolicy.fixed(segmentsCount));
        Controller.CreateTxnResponse response = createTransaction(SCOPE1, STREAM1, 10000);
        assertEquals(segmentsCount, response.getActiveSegmentsCount());
    }

    @Test
    public void transactionTests() {
        createScopeAndStream(SCOPE1, STREAM1, ScalingPolicy.fixed(4));
        Controller.TxnId txnId1 = createTransaction(SCOPE1, STREAM1, 10000).getTxnId();
        Controller.TxnId txnId2 = createTransaction(SCOPE1, STREAM1, 10000).getTxnId();

        // Abort first txn.
        Controller.TxnStatus status = closeTransaction(SCOPE1, STREAM1, txnId1, true);
        Assert.assertEquals(Controller.TxnStatus.Status.SUCCESS, status.getStatus());

        // Commit second txn.
        status = closeTransaction(SCOPE1, STREAM1, txnId2, false);
        Assert.assertEquals(Controller.TxnStatus.Status.SUCCESS, status.getStatus());
    }

    @Test
    @Override
    public void testListScopes() {
        ResultObserver<Controller.ScopesResponse> list = new ResultObserver<>();
        this.controllerService.listScopes(Controller.ScopesRequest.newBuilder().setContinuationToken(
                Controller.ContinuationToken.newBuilder().build()).build(), list);
        AssertExtensions.assertThrows("", list::get, 
                e -> Exceptions.unwrap(e) instanceof StatusRuntimeException);
    }

    @Override
    protected BucketStore getBucketStore() {
        return bucketStore;
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

    private Controller.CreateTxnResponse createTransaction(final String scope, final String stream, final long lease) {
        Controller.StreamInfo streamInfo = ModelHelper.createStreamInfo(scope, stream);
        Controller.CreateTxnRequest request = Controller.CreateTxnRequest.newBuilder()
                .setStreamInfo(streamInfo)
                .setLease(lease)
                .build();
        ResultObserver<Controller.CreateTxnResponse> resultObserver = new ResultObserver<>();
        this.controllerService.createTransaction(request, resultObserver);
        Controller.CreateTxnResponse response = resultObserver.get();
        Assert.assertTrue(response != null);
        return response;
    }

    @Test
    @Override
    public void createKeyValueTableTests() {
        // TODO: consider implementing ZK metadata support or removing altogether (https://github.com/pravega/pravega/issues/4922).
        // Key-Value Tables are not implemented in ZK Metadata.
    }

    @Test
    @Override
    public void getCurrentSegmentsKeyValueTableTest() {
        // TODO: consider implementing ZK metadata support or removing altogether (https://github.com/pravega/pravega/issues/4922).
        // Key-Value Tables are not implemented in ZK Metadata.
    }

    @Test
    @Override
    public void kvtablesInScopeTest() {
        // TODO: consider implementing ZK metadata support or removing altogether (https://github.com/pravega/pravega/issues/4922).
        // Key-Value Tables are not implemented in ZK Metadata.
    }

    @Test
    @Override
    public void deleteKeyValueTableTests() {
        // TODO: consider implementing ZK metadata support or removing altogether (https://github.com/pravega/pravega/issues/4922).
        // Key-Value Tables are not implemented in ZK Metadata.
    }

    @Test
    @Override
    public void updateSubscriberStreamCutTests() {
        // TODO: consider implementing ZK metadata support or removing altogether (https://github.com/pravega/pravega/issues/4922).
        // Subscribers are not implemented in ZK Metadata.
    }

    @Test
    @Override
    public void deleteReaderGroupTests() {
        // TODO: consider implementing ZK metadata support or removing altogether (https://github.com/pravega/pravega/issues/4922).
        // Subscribers are not implemented in ZK Metadata.
    }

    @Test
    @Override
    public void createReaderGroupTests() {
        // TODO: consider implementing ZK metadata support or removing altogether (https://github.com/pravega/pravega/issues/4922).
        // Subscribers are not implemented in ZK Metadata.
    }

    @Test
    @Override
    public void updateReaderGroupTests() {
        // TODO: consider implementing ZK metadata support or removing altogether (https://github.com/pravega/pravega/issues/4922).
        // Subscribers are not implemented in ZK Metadata.
    }
}
