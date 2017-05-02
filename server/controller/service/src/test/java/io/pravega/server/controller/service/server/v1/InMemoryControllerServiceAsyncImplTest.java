/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.server.v1;

import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.Host;
import io.pravega.server.controller.service.mocks.SegmentHelperMock;
import io.pravega.server.controller.service.server.ControllerService;
import io.pravega.server.controller.service.server.SegmentHelper;
import io.pravega.server.controller.service.server.rpc.grpc.v1.ControllerServiceImpl;
import io.pravega.server.controller.service.store.host.HostControllerStore;
import io.pravega.server.controller.service.store.host.HostStoreFactory;
import io.pravega.server.controller.service.store.host.impl.HostMonitorConfigImpl;
import io.pravega.server.controller.service.store.stream.StreamMetadataStore;
import io.pravega.server.controller.service.store.stream.StreamStoreFactory;
import io.pravega.server.controller.service.store.task.TaskMetadataStore;
import io.pravega.server.controller.service.store.task.TaskStoreFactory;
import io.pravega.server.controller.service.task.Stream.StreamMetadataTasks;
import io.pravega.server.controller.service.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.server.controller.service.timeout.TimeoutService;
import io.pravega.server.controller.service.timeout.TimeoutServiceConfig;
import io.pravega.server.controller.service.timeout.TimerWheelTimeoutService;
import io.pravega.client.stream.impl.netty.ConnectionFactoryImpl;
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
