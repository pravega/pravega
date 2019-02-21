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
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.Host;
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
import java.net.URI;
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
    private StreamRequestHandler streamRequestHandler;

    private ScheduledExecutorService executorService;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private StreamMetadataStore streamStore;
    private SegmentHelper segmentHelper;
    private RequestTracker requestTracker;

    @Override
    public void setup() throws Exception {
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(20, "testpool");
        taskMetadataStore = TaskStoreFactory.createInMemoryStore(executorService);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        streamStore = StreamStoreFactory.createInMemoryStore(executorService);
        BucketStore bucketStore = StreamStoreFactory.createInMemoryBucketStore();
        requestTracker = new RequestTracker(true);

        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                        .controllerURI(URI.create("tcp://localhost"))
                                                                                        .build());
        AuthHelper disabledAuthHelper = AuthHelper.getDisabledAuthHelper();
        segmentHelper = SegmentHelperMock.getSegmentHelperMock(hostStore, connectionFactory, disabledAuthHelper);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelper,
                executorService, "host", requestTracker);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelper,
                executorService, "host");
        this.streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executorService),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executorService),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, executorService),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executorService),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executorService),
                new TruncateStreamTask(streamMetadataTasks, streamStore, executorService),
                streamStore,
                executorService);

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, executorService));
        streamTransactionMetadataTasks.initializeStreamWriters("commitStream", new EventStreamWriterMock<>(),
                "abortStream", new EventStreamWriterMock<>());

        Cluster mockCluster = mock(Cluster.class);
        when(mockCluster.getClusterMembers()).thenReturn(Collections.singleton(new Host("localhost", 9090, null)));
        controllerService = new ControllerServiceImpl(
                new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks,
                                      new SegmentHelper(hostStore, connectionFactory, disabledAuthHelper), executorService, mockCluster), disabledAuthHelper, requestTracker, true, 2);
    }

    @Override
    public void tearDown() throws Exception {
        ExecutorServiceHelpers.shutdown(executorService);
        if (streamMetadataTasks != null) {
            streamMetadataTasks.close();
        }
        if (streamTransactionMetadataTasks != null) {
            streamTransactionMetadataTasks.close();
        }
        streamStore.close();
    }
}
