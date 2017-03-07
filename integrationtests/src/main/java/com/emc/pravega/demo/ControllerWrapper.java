/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.controller.requesthandler.RequestHandlersInit;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import com.emc.pravega.controller.server.eventProcessor.LocalController;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServer;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.ZKStreamMetadataStore;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.timeout.TimeoutService;
import com.emc.pravega.controller.timeout.TimerWheelTimeoutService;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.stream.impl.Controller;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ControllerWrapper implements AutoCloseable {

    @Getter
    private final ControllerService controllerService;
    @Getter
    private final Controller controller;
    private final GRPCServer rpcServer;
    private final ControllerEventProcessors controllerEventProcessors;
    private final TimeoutService timeoutService;

    public ControllerWrapper(final String connectionString) throws Exception {
        this(connectionString, false, false, Config.RPC_SERVER_PORT, Config.SERVICE_HOST, Config.SERVICE_PORT,
                Config.HOST_STORE_CONTAINER_COUNT);
    }

    public ControllerWrapper(final String connectionString, final boolean disableEventProcessor) throws Exception {
        this(connectionString, disableEventProcessor, false, Config.RPC_SERVER_PORT, Config.SERVICE_HOST,
                Config.SERVICE_PORT, Config.HOST_STORE_CONTAINER_COUNT);
    }

    public ControllerWrapper(final String connectionString, final boolean disableEventProcessor,
                             final boolean disableRequestHandler,
                             final int controllerPort, final String serviceHost, final int servicePort,
                             final int containerCount) throws Exception {
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

        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(serviceHost, servicePort, containerCount);

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        SegmentHelper segmentHelper = new SegmentHelper();

        //2) start RPC server with v1 implementation. Enable other versions if required.
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                segmentHelper, executor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, segmentHelper, executor, hostId);

        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks, 100000, 100000);

        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService, segmentHelper, executor);

        if (!disableRequestHandler) {
            RequestHandlersInit.bootstrapRequestHandlers(controllerService, streamStore, executor);
        }

        //region Setup Event Processors
        LocalController localController = new LocalController(controllerService);

        if (!disableEventProcessor) {
            controllerEventProcessors = new ControllerEventProcessors(hostId, localController,
                    client, streamStore, hostStore, segmentHelper);

            controllerEventProcessors.startAsync();

            streamTransactionMetadataTasks.initializeStreamWriters(localController);
        } else {
            controllerEventProcessors = null;
        }
        //endregion

        GRPCServerConfig gRPCServerConfig = GRPCServerConfig.builder()
                .port(controllerPort)
                .build();
        rpcServer = new GRPCServer(controllerService, gRPCServerConfig);
        rpcServer.startAsync();

        controller = new LocalController(controllerService);
    }

    @Override
    public void close() throws Exception {
        rpcServer.stopAsync();
        controllerEventProcessors.stopAsync();
        timeoutService.stopAsync();
    }
}
