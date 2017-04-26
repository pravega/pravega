/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.stream.impl.Controller;
import io.pravega.stream.impl.netty.ConnectionFactory;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.mock;

/**
 * Controller Event ProcessorTests.
 */
public class ControllerEventProcessorsTest {
    private ScheduledExecutorService executor;
    private StreamMetadataStore streamStore;
    private HostControllerStore hostStore;
    private TestingServer zkServer;
    private SegmentHelper segmentHelperMock;
    private CuratorFramework zkClient;
    private ControllerEventProcessors controllerEventProcessors;
    private Controller localControllerMock;
    private CheckpointStore checkpointStore;
    private ConnectionFactory connectionFactory;
    private StreamMetadataTasks streamMetadataTasks;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newScheduledThreadPool(10);

        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        zkClient.start();

        streamStore = mock(StreamMetadataStore.class);
        checkpointStore = mock(CheckpointStore.class);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        localControllerMock = mock(Controller.class);
        connectionFactory = mock(ConnectionFactory.class);

        controllerEventProcessors = new ControllerEventProcessors(UUID.randomUUID().toString(),
                ControllerEventProcessorConfigImpl.withDefault(), localControllerMock, checkpointStore, streamStore,
                hostStore, segmentHelperMock, connectionFactory, streamMetadataTasks, executor);
    }

    @After
    public void tearDown() throws Exception {
        zkClient.close();
        zkServer.close();
        executor.shutdown();
    }

    @Test
    public void testFailureRetry() {
        controller
    }
}
