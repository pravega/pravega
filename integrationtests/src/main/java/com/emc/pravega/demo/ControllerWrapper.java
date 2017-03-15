/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.controller.server.ControllerServiceConfig;
import com.emc.pravega.controller.server.ControllerServiceStarter;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreClientFactory;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.stream.impl.Controller;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ControllerWrapper implements AutoCloseable {

    private final ControllerServiceStarter controllerServiceStarter;

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

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, new RetryOneTime(2000));
        client.start();

        StoreClient storeClient = StoreClientFactory.createZKStoreClient(client);

        ControllerServiceConfig.HostMonitorConfig hostMonitorConfig = ControllerServiceConfig.HostMonitorConfig.builder()
                .hostMonitorEnabled(false)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .sssHost(serviceHost)
                .sssPort(servicePort)
                .containerCount(containerCount)
                .build();

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .maxScaleGracePeriod(Config.MAX_SCALE_GRACE_PERIOD)
                .build();

        GRPCServerConfig grpcServerConfig = GRPCServerConfig.builder()
                .port(controllerPort)
                .build();

        ControllerServiceConfig serviceConfig = ControllerServiceConfig.builder()
                .host(hostId)
                .threadPoolSize(6)
                .storeClient(storeClient)
                .hostMonitorConfig(hostMonitorConfig)
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorsEnabled(!disableEventProcessor)
                .requestHandlersEnabled(!disableRequestHandler)
                .gRPCServerEnabled(true)
                .grpcServerConfig(grpcServerConfig)
                .restServerEnabled(false)
                .build();

        controllerServiceStarter = new ControllerServiceStarter(serviceConfig);
        controllerServiceStarter.startAsync();
    }

    public boolean awaitTasksModuleInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return this.controllerServiceStarter.awaitTasksModuleInitialization(timeout, timeUnit);
    }

    public ControllerService getControllerService() {
        return this.controllerServiceStarter.getControllerService();
    }

    public Controller getController() {
        return this.controllerServiceStarter.getController();
    }

    @Override
    public void close() throws Exception {
        this.controllerServiceStarter.stopAsync();
    }
}
