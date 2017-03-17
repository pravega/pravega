/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.v1;

import com.emc.pravega.controller.mocks.SegmentHelperMock;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreClientFactory;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.timeout.TimeoutService;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
import com.emc.pravega.controller.timeout.TimerWheelTimeoutService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Zookeeper stream store configuration.
 */
public class ZKControllerServiceAsyncImplTest extends ControllerServiceImplTest {

    private TestingServer zkServer;
    private CuratorFramework zkClient;
    private StoreClient storeClient;
    private TaskMetadataStore taskMetadataStore;
    private HostControllerStore hostStore;
    private StreamMetadataTasks streamMetadataTasks;
    private ScheduledExecutorService executorService;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private StreamMetadataStore streamStore;
    private SegmentHelper segmentHelper;
    private TimeoutService timeoutService;

    @Override
    public void setupStore() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        storeClient = StoreClientFactory.createZKStoreClient(zkClient);
        executorService = Executors.newScheduledThreadPool(20,
                new ThreadFactoryBuilder().setNameFormat("testpool-%d").build());
        taskMetadataStore = TaskStoreFactory.createStore(storeClient, executorService);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.defaultConfig());
        streamStore = StreamStoreFactory.createZKStore(zkClient, executorService);
        segmentHelper = SegmentHelperMock.getSegmentHelperMock();

        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelper,
                executorService, "host");

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStore, hostStore, taskMetadataStore, segmentHelper, executorService, "host");
        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                TimeoutServiceConfig.defaultConfig());

        controllerService = new ControllerServiceImpl(
                new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks,
                                      timeoutService, new SegmentHelper(), executorService));
    }

    @Override
    public void cleanupStore() throws IOException {
        executorService.shutdown();
        zkClient.close();
        zkServer.close();
    }
}
