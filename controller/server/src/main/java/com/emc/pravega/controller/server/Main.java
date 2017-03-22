/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.common.metrics.MetricsProvider;
import com.emc.pravega.controller.fault.ControllerClusterListenerConfig;
import com.emc.pravega.controller.fault.impl.ControllerClusterListenerConfigImpl;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import com.emc.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import com.emc.pravega.controller.server.impl.ControllerServiceConfigImpl;
import com.emc.pravega.controller.server.rest.RESTServerConfig;
import com.emc.pravega.controller.server.rest.impl.RESTServerConfigImpl;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.emc.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import com.emc.pravega.controller.store.client.StoreClientConfig;
import com.emc.pravega.controller.store.client.ZKClientConfig;
import com.emc.pravega.controller.store.client.impl.StoreClientConfigImpl;
import com.emc.pravega.controller.store.client.impl.ZKClientConfigImpl;
import com.emc.pravega.controller.store.host.HostMonitorConfig;
import com.emc.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
import com.emc.pravega.controller.util.Config;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Entry point of controller server.
 */
@Slf4j
public class Main {

    public static void main(String[] args) {

        //0. Initialize metrics provider
        MetricsProvider.initialize(Config.getMetricsConfig());

        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder()
                .connectionString(Config.ZK_URL)
                .namespace("pravega/" + Config.CLUSTER_NAME)
                .initialSleepInterval(Config.ZK_RETRY_SLEEP_MS)
                .maxRetries(Config.ZK_MAX_RETRIES)
                .build();

        StoreClientConfig storeClientConfig = StoreClientConfigImpl.withZKClient(zkClientConfig);

        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(Config.HOST_MONITOR_ENABLED)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .containerCount(Config.HOST_STORE_CONTAINER_COUNT)
                .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(Config.SERVICE_HOST,
                        Config.SERVICE_PORT, Config.HOST_STORE_CONTAINER_COUNT))
                .build();

        ControllerClusterListenerConfig controllerClusterListenerConfig = ControllerClusterListenerConfigImpl.builder()
                .minThreads(2)
                .maxThreads(10)
                .idleTime(10)
                .idleTimeUnit(TimeUnit.SECONDS)
                .maxQueueSize(512)
                .build();

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .maxScaleGracePeriod(Config.MAX_SCALE_GRACE_PERIOD)
                .build();

        ControllerEventProcessorConfig eventProcessorConfig = ControllerEventProcessorConfigImpl.withDefault();

        GRPCServerConfig grpcServerConfig = GRPCServerConfigImpl.builder()
                .port(Config.RPC_SERVER_PORT)
                .build();

        RESTServerConfig restServerConfig = RESTServerConfigImpl.builder()
                .host(Config.REST_SERVER_IP)
                .port(Config.REST_SERVER_PORT)
                .build();

        ControllerServiceConfig serviceConfig = ControllerServiceConfigImpl.builder()
                .serviceThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .taskThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .storeThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .eventProcThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE / 2)
                .requestHandlerThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE / 2)
                .storeClientConfig(storeClientConfig)
                .hostMonitorConfig(hostMonitorConfig)
                .controllerClusterListenerConfig(Optional.of(controllerClusterListenerConfig))
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorConfig(Optional.of(eventProcessorConfig))
                .requestHandlersEnabled(true)
                .grpcServerConfig(Optional.of(grpcServerConfig))
                .restServerConfig(Optional.of(restServerConfig))
                .build();

        ControllerServiceMain controllerServiceMain = new ControllerServiceMain(serviceConfig);
        controllerServiceMain.startAsync();
        controllerServiceMain.awaitTerminated();
    }
}
