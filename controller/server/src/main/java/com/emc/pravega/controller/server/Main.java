/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.controller.server.rest.RESTServerConfig;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreClientFactory;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.controller.util.ZKUtils;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * Entry point of controller server.
 */
@Slf4j
public class Main {

    public static void main(String[] args) {
        String hostId;
        try {
            //On each controller process restart, it gets a fresh hostId,
            //which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + "-" + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            log.warn("Failed to get host address.", e);
            hostId = UUID.randomUUID().toString();
        }

        log.info("Creating store client");
        StoreClient storeClient = StoreClientFactory.createStoreClient(
                StoreClientFactory.StoreType.valueOf(Config.STORE_TYPE), ZKUtils.getCuratorClient());

        ControllerServiceConfig.HostMonitorConfig hostMonitorConfig = ControllerServiceConfig.HostMonitorConfig.builder()
                .hostMonitorEnabled(Config.HOST_MONITOR_ENABLED)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .sssHost(Config.SERVICE_HOST)
                .sssPort(Config.SERVICE_PORT)
                .containerCount(Config.HOST_STORE_CONTAINER_COUNT)
                .build();

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .maxScaleGracePeriod(Config.MAX_SCALE_GRACE_PERIOD)
                .build();

        GRPCServerConfig grpcServerConfig = GRPCServerConfig.builder()
                .port(Config.RPC_SERVER_PORT)
                .build();
        RESTServerConfig restServerConfig = RESTServerConfig.builder()
                .host(Config.REST_SERVER_IP)
                .port(Config.REST_SERVER_PORT)
                .build();

        ControllerServiceConfig serviceConfig = ControllerServiceConfig.builder()
                .host(hostId)
                .threadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .storeClient(storeClient)
                .hostMonitorConfig(hostMonitorConfig)
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorsEnabled(true)
                .requestHandlersEnabled(true)
                .gRPCServerEnabled(true)
                .grpcServerConfig(grpcServerConfig)
                .restServerEnabled(true)
                .restServerConfig(restServerConfig)
                .build();

        ControllerServiceStarter controllerServiceStarter = new ControllerServiceStarter(serviceConfig);
        controllerServiceStarter.startAsync();
        controllerServiceStarter.awaitTerminated();
    }
}
