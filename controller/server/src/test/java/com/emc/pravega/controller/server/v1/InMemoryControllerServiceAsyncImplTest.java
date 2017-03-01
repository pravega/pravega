/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.v1;

import com.emc.pravega.controller.mocks.SegmentHelperMock;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.store.StoreClientFactory;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * InMemory stream store configuration.
 */
public class InMemoryControllerServiceAsyncImplTest extends ControllerServiceAsyncImplTest {

    private TaskMetadataStore taskMetadataStore;
    private HostControllerStore hostStore;
    private StreamMetadataTasks streamMetadataTasks;
    private ScheduledExecutorService executorService;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private StreamMetadataStore streamStore;
    private SegmentHelper segmentHelper;

    @Override
    public void setupStore() throws Exception {

        executorService = Executors.newScheduledThreadPool(20,
                new ThreadFactoryBuilder().setNameFormat("testpool-%d").build());
        taskMetadataStore = TaskStoreFactory.createStore(
                StoreClientFactory.createStoreClient(StoreClientFactory.StoreType.InMemory), executorService);
        hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);
        streamStore = StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, executorService);
        segmentHelper = SegmentHelperMock.getSegmentHelperMock();

        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelper,
                executorService, "host");

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStore, hostStore, taskMetadataStore, segmentHelper, executorService, "host");

        controllerService = new ControllerServiceAsyncImpl(
                new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks,
                        new SegmentHelper(), executorService));
    }

    @Override
    public void cleanupStore() throws IOException {
        executorService.shutdown();
    }
}
