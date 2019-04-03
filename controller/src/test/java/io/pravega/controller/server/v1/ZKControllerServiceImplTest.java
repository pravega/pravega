/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.v1;

import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.mocks.ControllerEventStreamWriterMock;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.test.common.TestingServerStarter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
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

    private ScheduledExecutorService executorService;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private Cluster cluster;
    private StreamMetadataStore streamStore;

    @Override
    public void setup() throws Exception {
        final HostControllerStore hostStore;
        final TaskMetadataStore taskMetadataStore;
        final SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        final RequestTracker requestTracker = new RequestTracker(true);

        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        storeClient = StoreClientFactory.createZKStoreClient(zkClient);
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(20, "testpool");
        taskMetadataStore = TaskStoreFactory.createStore(storeClient, executorService);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        streamStore = StreamStoreFactory.createZKStore(zkClient, executorService);
        BucketStore bucketStore = StreamStoreFactory.createZKBucketStore(zkClient, executorService);

        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelper,
                executorService, "host", AuthHelper.getDisabledAuthHelper(), requestTracker);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelper,
                executorService, "host", AuthHelper.getDisabledAuthHelper());
        this.streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executorService),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executorService),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, executorService),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executorService),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executorService),
                new TruncateStreamTask(streamMetadataTasks, streamStore, executorService),
                streamStore,
                executorService);

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, executorService));

        streamTransactionMetadataTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());

        cluster = new ClusterZKImpl(zkClient, ClusterType.CONTROLLER);
        final CountDownLatch latch = new CountDownLatch(1);
        cluster.addListener((type, host) -> latch.countDown());
        cluster.registerHost(new Host("localhost", 9090, null));
        latch.await();

        ControllerService controller = new ControllerService(streamStore, streamMetadataTasks,
                streamTransactionMetadataTasks, segmentHelper, executorService, cluster);
        controllerService = new ControllerServiceImpl(controller, AuthHelper.getDisabledAuthHelper(), requestTracker, true, 2);
    }

    @Override
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
}
