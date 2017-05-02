/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.server;

import io.pravega.server.controller.service.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.server.controller.service.server.rest.impl.RESTServerConfigImpl;
import io.pravega.server.controller.service.store.client.impl.StoreClientConfigImpl;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.server.controller.service.fault.ControllerClusterListenerConfig;
import io.pravega.server.controller.service.fault.impl.ControllerClusterListenerConfigImpl;
import io.pravega.server.controller.service.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.server.controller.service.server.impl.ControllerServiceConfigImpl;
import io.pravega.server.controller.service.server.rest.RESTServerConfig;
import io.pravega.server.controller.service.server.rpc.grpc.GRPCServerConfig;
import io.pravega.server.controller.service.store.client.StoreClientConfig;
import io.pravega.server.controller.service.store.client.ZKClientConfig;
import io.pravega.server.controller.service.store.client.impl.ZKClientConfigImpl;
import io.pravega.server.controller.service.store.host.HostMonitorConfig;
import io.pravega.server.controller.service.store.host.impl.HostMonitorConfigImpl;
import io.pravega.server.controller.service.timeout.TimeoutServiceConfig;
import io.pravega.server.controller.service.util.Config;
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
                .minThreads(1)
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

        GRPCServerConfig grpcServerConfig = Config.getGRPCServerConfig();

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
                .grpcServerConfig(Optional.of(grpcServerConfig))
                .restServerConfig(Optional.of(restServerConfig))
                .build();

        ControllerServiceMain controllerServiceMain = new ControllerServiceMain(serviceConfig);
        controllerServiceMain.startAsync();
        controllerServiceMain.awaitTerminated();
    }
}
