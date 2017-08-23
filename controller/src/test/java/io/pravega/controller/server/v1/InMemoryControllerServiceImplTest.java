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

import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.ScaleEventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * InMemory stream store configuration.
 */
public class InMemoryControllerServiceImplTest extends ControllerServiceImplTest {

    private TaskMetadataStore taskMetadataStore;
    private HostControllerStore hostStore;
    private StreamMetadataTasks streamMetadataTasks;
    private ScheduledExecutorService executorService;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private StreamMetadataStore streamStore;
    private SegmentHelper segmentHelper;

    @Override
    public void setup() throws Exception {
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(20, "testpool");
        taskMetadataStore = TaskStoreFactory.createInMemoryStore(executorService);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        streamStore = StreamStoreFactory.createInMemoryStore(executorService);
        segmentHelper = SegmentHelperMock.getSegmentHelperMock();

        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelper,
                executorService, "host", connectionFactory);
        streamMetadataTasks.setRequestEventWriter(new ScaleEventStreamWriterMock(streamMetadataTasks, executorService));
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStore, hostStore, segmentHelper, executorService, "host", connectionFactory);
        streamTransactionMetadataTasks.initializeStreamWriters("commitStream", new EventStreamWriterMock<>(),
                "abortStream", new EventStreamWriterMock<>());

        Cluster mockCluster = mock(Cluster.class);
        when(mockCluster.getClusterMembers()).thenReturn(Collections.singleton(new Host("localhost", 9090, null)));
        controllerService = new ControllerServiceImpl(
                new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks,
                                      new SegmentHelper(), executorService, mockCluster));
    }

    @Override
    public void tearDown() throws Exception {
        executorService.shutdown();
        if (streamMetadataTasks != null) {
            streamMetadataTasks.close();
        }
        if (streamTransactionMetadataTasks != null) {
            streamTransactionMetadataTasks.close();
        }
    }
}
