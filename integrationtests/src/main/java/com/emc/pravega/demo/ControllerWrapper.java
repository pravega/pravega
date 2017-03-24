/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.fault.ControllerClusterListenerConfig;
import com.emc.pravega.controller.fault.impl.ControllerClusterListenerConfigImpl;
import com.emc.pravega.controller.server.ControllerServiceConfig;
import com.emc.pravega.controller.server.ControllerServiceMain;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import com.emc.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import com.emc.pravega.controller.server.impl.ControllerServiceConfigImpl;
import com.emc.pravega.controller.server.rest.RESTServerConfig;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.emc.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreClientConfig;
import com.emc.pravega.controller.store.client.ZKClientConfig;
import com.emc.pravega.controller.store.client.impl.StoreClientConfigImpl;
import com.emc.pravega.controller.store.client.impl.ZKClientConfigImpl;
import com.emc.pravega.controller.store.host.HostMonitorConfig;
import com.emc.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.impl.Controller;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


@Slf4j
public class ControllerWrapper implements AutoCloseable {

    private final ControllerServiceMain controllerServiceMain;

    public ControllerWrapper(final String connectionString, final int servicePort) throws Exception {
        this(connectionString, false, false, Config.RPC_SERVER_PORT, Config.SERVICE_HOST, servicePort,
                Config.HOST_STORE_CONTAINER_COUNT);
    }

    public ControllerWrapper(final String connectionString, final int servicePort,
            final boolean disableEventProcessor) throws Exception {
        this(connectionString, disableEventProcessor, false, Config.RPC_SERVER_PORT, Config.SERVICE_HOST, servicePort,
             Config.HOST_STORE_CONTAINER_COUNT);
    }

    public ControllerWrapper(final String connectionString, final boolean disableEventProcessor,
                             final boolean disableRequestHandler,
                             final int controllerPort, final String serviceHost, final int servicePort,
                             final int containerCount) {
        this(connectionString, disableEventProcessor, disableRequestHandler, true, controllerPort, serviceHost,
                servicePort, containerCount);
    }

    public ControllerWrapper(final String connectionString, final boolean disableEventProcessor,
                             final boolean disableRequestHandler, final boolean disableControllerCluster,
                             final int controllerPort, final String serviceHost, final int servicePort,
                             final int containerCount) {

        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder().connectionString(connectionString)
                .initialSleepInterval(500)
                .maxRetries(10)
                .namespace("pravega/" + UUID.randomUUID())
                .build();
        StoreClientConfig storeClientConfig = StoreClientConfigImpl.withZKClient(zkClientConfig);

        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(false)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .containerCount(containerCount)
                .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(serviceHost, servicePort, containerCount))
                .build();

        Optional<ControllerClusterListenerConfig> controllerClusterListenerConfig;
        if (!disableControllerCluster) {
            controllerClusterListenerConfig = Optional.of(ControllerClusterListenerConfigImpl.builder()
                    .minThreads(2)
                    .maxThreads(10)
                    .idleTime(10)
                    .idleTimeUnit(TimeUnit.SECONDS)
                    .maxQueueSize(512)
                    .build());
        } else {
            controllerClusterListenerConfig = Optional.empty();
        }

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .maxScaleGracePeriod(Config.MAX_SCALE_GRACE_PERIOD)
                .build();

        Optional<ControllerEventProcessorConfig> eventProcessorConfig;
        if (!disableEventProcessor) {
            eventProcessorConfig = Optional.of(ControllerEventProcessorConfigImpl.builder()
                    .scopeName("system")
                    .commitStreamName("commitStream")
                    .abortStreamName("abortStream")
                    .commitStreamScalingPolicy(ScalingPolicy.fixed(2))
                    .abortStreamScalingPolicy(ScalingPolicy.fixed(2))
                    .commitReaderGroupName("commitStreamReaders")
                    .commitReaderGroupSize(1)
                    .abortReaderGrouopName("abortStreamReaders")
                    .abortReaderGroupSize(1)
                    .commitCheckpointConfig(CheckpointConfig.periodic(10, 10))
                    .abortCheckpointConfig(CheckpointConfig.periodic(10, 10))
                    .build());
        } else {
            eventProcessorConfig = Optional.empty();
        }

        GRPCServerConfig grpcServerConfig = GRPCServerConfigImpl.builder().port(controllerPort).build();

        ControllerServiceConfig serviceConfig = ControllerServiceConfigImpl.builder()
                .serviceThreadPoolSize(3)
                .taskThreadPoolSize(3)
                .storeThreadPoolSize(3)
                .eventProcThreadPoolSize(3)
                .requestHandlerThreadPoolSize(3)
                .storeClientConfig(storeClientConfig)
                .controllerClusterListenerConfig(controllerClusterListenerConfig)
                .hostMonitorConfig(hostMonitorConfig)
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorConfig(eventProcessorConfig)
                .requestHandlersEnabled(!disableRequestHandler)
                .grpcServerConfig(Optional.of(grpcServerConfig))
                .restServerConfig(Optional.<RESTServerConfig>empty())
                .build();

        controllerServiceMain = new ControllerServiceMain(serviceConfig);
        controllerServiceMain.startAsync();
    }

    public boolean awaitTasksModuleInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return this.controllerServiceMain.getStarter().awaitTasksModuleInitialization(timeout, timeUnit);
    }

    public ControllerService getControllerService() throws InterruptedException {
        return this.controllerServiceMain.getStarter().getControllerService();
    }

    public Controller getController() throws InterruptedException {
        return this.controllerServiceMain.getStarter().getController();
    }

    public void awaitRunning() throws InterruptedException {
        this.controllerServiceMain.getStarter().awaitRunning();
    }

    public void awaitTerminated() {
        this.controllerServiceMain.awaitTerminated();
    }

    public StoreClient getStoreClient() throws InterruptedException {
        return this.controllerServiceMain.getStoreClient();
    }

    public ControllerServiceMain getControllerServiceMain() {
        return this.controllerServiceMain;
    }

    @Override
    public void close() throws Exception {
        this.controllerServiceMain.stopAsync();
        this.controllerServiceMain.getStarter().awaitTerminated();
    }
}
