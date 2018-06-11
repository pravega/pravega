/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.server.rest.RESTServerConfig;
import io.pravega.controller.server.rest.impl.RESTServerConfigImpl;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.util.Config;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Entry point of controller server.
 */
@Slf4j
public class Main {

    public static void main(String[] args) {

        try {
            //0. Initialize metrics provider
            MetricsProvider.initialize(Config.getMetricsConfig());

            ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder()
                    .connectionString(Config.ZK_URL)
                    .namespace("pravega/" + Config.CLUSTER_NAME)
                    .initialSleepInterval(Config.ZK_RETRY_SLEEP_MS)
                    .maxRetries(Config.ZK_MAX_RETRIES)
                    .sessionTimeoutMs(Config.ZK_SESSION_TIMEOUT_MS)
                    .build();

            StoreClientConfig storeClientConfig = StoreClientConfigImpl.withZKClient(zkClientConfig);

            HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                    .hostMonitorEnabled(Config.HOST_MONITOR_ENABLED)
                    .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                    .containerCount(Config.HOST_STORE_CONTAINER_COUNT)
                    .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(Config.SERVICE_HOST,
                            Config.SERVICE_PORT, Config.HOST_STORE_CONTAINER_COUNT))
                    .build();

            TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                    .maxLeaseValue(Config.MAX_LEASE_VALUE)
                    .build();

            ControllerEventProcessorConfig eventProcessorConfig = ControllerEventProcessorConfigImpl.withDefault();

            GRPCServerConfig grpcServerConfig = Config.getGRPCServerConfig();

            RESTServerConfig restServerConfig = RESTServerConfigImpl.builder()
                    .host(Config.REST_SERVER_IP)
                    .port(Config.REST_SERVER_PORT)
                    .build();

            ControllerServiceConfig serviceConfig = ControllerServiceConfigImpl.builder()
                    .threadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                    .storeClientConfig(storeClientConfig)
                    .hostMonitorConfig(hostMonitorConfig)
                    .controllerClusterListenerEnabled(true)
                    .timeoutServiceConfig(timeoutServiceConfig)
                    .eventProcessorConfig(Optional.of(eventProcessorConfig))
                    .grpcServerConfig(Optional.of(grpcServerConfig))
                    .restServerConfig(Optional.of(restServerConfig))
                    .build();

            ControllerServiceMain controllerServiceMain = new ControllerServiceMain(serviceConfig);
            controllerServiceMain.startAsync();
            controllerServiceMain.awaitTerminated();

            log.info("Controller service exited");
            System.exit(0);
        } catch (Throwable e) {
            log.error("Controller service failed", e);
            System.exit(-1);
        }
    }
}
