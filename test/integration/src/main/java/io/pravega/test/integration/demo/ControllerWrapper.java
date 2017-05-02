/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.test.integration.demo;

import io.pravega.shared.NameUtils;
import io.pravega.server.controller.service.eventProcessor.CheckpointConfig;
import io.pravega.server.controller.service.fault.ControllerClusterListenerConfig;
import io.pravega.server.controller.service.fault.impl.ControllerClusterListenerConfigImpl;
import io.pravega.server.controller.service.server.ControllerServiceConfig;
import io.pravega.server.controller.service.server.ControllerServiceMain;
import io.pravega.server.controller.service.server.ControllerService;
import io.pravega.server.controller.service.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.server.controller.service.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.server.controller.service.server.impl.ControllerServiceConfigImpl;
import io.pravega.server.controller.service.server.rest.RESTServerConfig;
import io.pravega.server.controller.service.server.rest.impl.RESTServerConfigImpl;
import io.pravega.server.controller.service.server.rpc.grpc.GRPCServerConfig;
import io.pravega.server.controller.service.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.server.controller.service.store.client.StoreClientConfig;
import io.pravega.server.controller.service.store.client.ZKClientConfig;
import io.pravega.server.controller.service.store.client.impl.StoreClientConfigImpl;
import io.pravega.server.controller.service.store.client.impl.ZKClientConfigImpl;
import io.pravega.server.controller.service.store.host.HostMonitorConfig;
import io.pravega.server.controller.service.store.host.impl.HostMonitorConfigImpl;
import io.pravega.server.controller.service.timeout.TimeoutServiceConfig;
import io.pravega.server.controller.service.util.Config;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.impl.Controller;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


@Slf4j
public class ControllerWrapper implements AutoCloseable {

    private final ControllerServiceMain controllerServiceMain;

    public ControllerWrapper(final String connectionString, final int servicePort) throws Exception {
        this(connectionString, false, Config.RPC_SERVER_PORT, Config.SERVICE_HOST, servicePort,
                Config.HOST_STORE_CONTAINER_COUNT);
    }

    public ControllerWrapper(final String connectionString, final int servicePort,
            final boolean disableEventProcessor) throws Exception {
        this(connectionString, disableEventProcessor, Config.RPC_SERVER_PORT, Config.SERVICE_HOST, servicePort,
             Config.HOST_STORE_CONTAINER_COUNT);
    }

    public ControllerWrapper(final String connectionString, final boolean disableEventProcessor,
                             final int controllerPort, final String serviceHost, final int servicePort,
                             final int containerCount) {
        this(connectionString, disableEventProcessor, true, controllerPort, serviceHost,
                servicePort, containerCount, -1);
    }

    public ControllerWrapper(final String connectionString, final boolean disableEventProcessor,
                             final boolean disableControllerCluster,
                             final int controllerPort, final String serviceHost, final int servicePort,
                             final int containerCount, int restPort) {

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
                    .scopeName(NameUtils.INTERNAL_SCOPE_NAME)
                    .commitStreamName(NameUtils.getInternalNameForStream("commitStream"))
                    .abortStreamName(NameUtils.getInternalNameForStream("abortStream"))
                    .commitStreamScalingPolicy(ScalingPolicy.fixed(2))
                    .abortStreamScalingPolicy(ScalingPolicy.fixed(2))
                    .scaleStreamScalingPolicy(ScalingPolicy.fixed(2))
                    .commitReaderGroupName("commitStreamReaders")
                    .commitReaderGroupSize(1)
                    .abortReaderGroupName("abortStreamReaders")
                    .abortReaderGroupSize(1)
                    .commitCheckpointConfig(CheckpointConfig.periodic(10, 10))
                    .abortCheckpointConfig(CheckpointConfig.periodic(10, 10))
                    .build());
        } else {
            eventProcessorConfig = Optional.empty();
        }

        GRPCServerConfig grpcServerConfig = GRPCServerConfigImpl.builder().port(controllerPort)
                .publishedRPCHost("localhost").publishedRPCPort(controllerPort).build();

        Optional<RESTServerConfig> restServerConfig = restPort > 0 ?
                Optional.of(RESTServerConfigImpl.builder().host("localhost").port(restPort).build()) :
                Optional.<RESTServerConfig>empty();

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
                .grpcServerConfig(Optional.of(grpcServerConfig))
                .restServerConfig(restServerConfig)
                .build();

        controllerServiceMain = new ControllerServiceMain(serviceConfig);
        controllerServiceMain.startAsync();
    }

    public boolean awaitTasksModuleInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return this.controllerServiceMain.awaitServiceStarting().awaitTasksModuleInitialization(timeout, timeUnit);
    }

    public ControllerService getControllerService() throws InterruptedException {
        return this.controllerServiceMain.awaitServiceStarting().getControllerService();
    }

    public Controller getController() throws InterruptedException {
        return this.controllerServiceMain.awaitServiceStarting().getController();
    }

    public void awaitRunning() {
        this.controllerServiceMain.awaitServiceStarting().awaitRunning();
    }

    public void awaitPaused() {
        this.controllerServiceMain.awaitServicePausing().awaitTerminated();
    }

    public void awaitTerminated() {
        this.controllerServiceMain.awaitTerminated();
    }

    public void forceClientSessionExpiry() throws Exception {
        this.controllerServiceMain.forceClientSessionExpiry();
    }

    @Override
    public void close() throws Exception {
        this.controllerServiceMain.stopAsync();
        this.controllerServiceMain.awaitTerminated();
    }
}
