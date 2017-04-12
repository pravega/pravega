/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.v1;

import com.emc.pravega.shared.common.cluster.Cluster;
import com.emc.pravega.shared.common.cluster.Host;
import com.emc.pravega.controller.mocks.SegmentHelperMock;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
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
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * InMemory stream store configuration.
 */
public class InMemoryControllerServiceAsyncImplTest extends ControllerServiceImplTest {

    private TaskMetadataStore taskMetadataStore;
    private HostControllerStore hostStore;
    private StreamMetadataTasks streamMetadataTasks;
    private ScheduledExecutorService executorService;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private StreamMetadataStore streamStore;
    private SegmentHelper segmentHelper;
    private TimeoutService timeoutService;

    @Override
    public void setup() throws Exception {

        executorService = Executors.newScheduledThreadPool(20,
                new ThreadFactoryBuilder().setNameFormat("testpool-%d").build());
        taskMetadataStore = TaskStoreFactory.createInMemoryStore(executorService);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        streamStore = StreamStoreFactory.createInMemoryStore(executorService);
        segmentHelper = SegmentHelperMock.getSegmentHelperMock();

        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelper,
                executorService, "host", connectionFactory);

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStore, hostStore, taskMetadataStore, segmentHelper, executorService, "host", connectionFactory);

        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                TimeoutServiceConfig.defaultConfig());

        Cluster mockCluster = mock(Cluster.class);
        when(mockCluster.getClusterMembers()).thenReturn(Collections.singleton(new Host("localhost", 9090, null)));
        controllerService = new ControllerServiceImpl(
                new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks,
                                      timeoutService, new SegmentHelper(), executorService, mockCluster));
    }

    @Override
    public void tearDown() throws Exception {
        executorService.shutdown();
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
    }
}
