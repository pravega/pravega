/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.controller.requesthandler.RequestHandlersInit;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import com.emc.pravega.controller.server.eventProcessor.LocalController;
import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.ZKStreamMetadataStore;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.impl.Controller;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

class ControllerWrapper {

    static ControllerService controllerService;

    static Controller getController(String connectionString, boolean disableEventProcessors) throws Exception {
        String hostId;
        try {
            // On each controller process restart, it gets a fresh hostId,
            // which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            hostId = UUID.randomUUID().toString();
        }

        // initialize the executor service
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, new RetryOneTime(2000));
        client.start();

        StoreClient storeClient = new ZKStoreClient(client);

        ZKStreamMetadataStore streamStore = new ZKStreamMetadataStore(client, executor);

        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        SegmentHelper segmentHelper = new SegmentHelper();

        //2) start RPC server with v1 implementation. Enable other versions if required.
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                segmentHelper, executor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, segmentHelper, executor, hostId);

        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks,
                segmentHelper, executor);
        RPCServer.start(new ControllerServiceAsyncImpl(controllerService));

        RequestHandlersInit.bootstrapRequestHandlers(controllerService, executor);

        //region Setup Event Processors
        LocalController localController = new LocalController(controllerService);

        if (!disableEventProcessors) {
            streamTransactionMetadataTasks.initializeStreamWriters(localController);

            ControllerEventProcessors controllerEventProcessors = new ControllerEventProcessors(hostId, localController,
                    client, streamStore, hostStore, segmentHelper);

            controllerEventProcessors.initialize();
            //endregion
        }

        return new LocalController(controllerService);
    }
}

