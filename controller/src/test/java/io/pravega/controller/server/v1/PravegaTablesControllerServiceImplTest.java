/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.v1;

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
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Zookeeper stream store configuration.
 */
public class PravegaTablesControllerServiceImplTest extends ControllerServiceImplTest {

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
        final SegmentHelper segmentHelper;
        final RequestTracker requestTracker = new RequestTracker(true);

        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        storeClient = StoreClientFactory.createZKStoreClient(zkClient);
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(20, "testpool");
        segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executorService);
        taskMetadataStore = TaskStoreFactory.createStore(storeClient, executorService);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        streamStore = StreamStoreFactory.createPravegaTablesStore(segmentHelper, AuthHelper.getDisabledAuthHelper(), 
                zkClient, executorService);
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

        ControllerService controller = new ControllerService(streamStore, bucketStore, streamMetadataTasks,
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
}
