/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo;

import io.pravega.shared.NameUtils;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.server.ControllerServiceConfig;
import io.pravega.controller.server.ControllerServiceMain;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.server.rest.RESTServerConfig;
import io.pravega.controller.server.rest.impl.RESTServerConfigImpl;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.util.Config;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.control.impl.Controller;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

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

        this(connectionString, disableEventProcessor, disableControllerCluster, controllerPort,
                serviceHost, servicePort, containerCount, restPort,
                false, null, null);
    }

    public ControllerWrapper(final String connectionString, final boolean disableEventProcessor,
                             final boolean disableControllerCluster,
                             final int controllerPort, final String serviceHost, final int servicePort,
                             final int containerCount, int restPort,
                             boolean enableAuth, String passwordAuthHandlerInputFilePath, String tokenSigningKey) {

        this(connectionString, disableEventProcessor, disableControllerCluster, controllerPort,
                serviceHost, servicePort, containerCount, restPort,
                enableAuth, passwordAuthHandlerInputFilePath, tokenSigningKey, 600);
    }

    public ControllerWrapper(final String connectionString, final boolean disableEventProcessor,
                             final boolean disableControllerCluster,
                             final int controllerPort, final String serviceHost, final int servicePort,
                             final int containerCount, int restPort,
                             boolean enableAuth, String passwordAuthHandlerInputFilePath,
                             String tokenSigningKey, int accessTokenTtlInSeconds) {
        this(connectionString, disableEventProcessor, disableControllerCluster, controllerPort,
                serviceHost, servicePort, containerCount, restPort,
                enableAuth, passwordAuthHandlerInputFilePath, tokenSigningKey,
                true, 600);
    }

    public ControllerWrapper(final String connectionString, final boolean disableEventProcessor,
                             final boolean disableControllerCluster,
                             final int controllerPort, final String serviceHost, final int servicePort,
                             final int containerCount, int restPort,
                             boolean enableAuth, String passwordAuthHandlerInputFilePath,
                             String tokenSigningKey, boolean isRGWritesWithReadPermEnabled,
                             int accessTokenTtlInSeconds) {
        this (connectionString, disableEventProcessor, disableControllerCluster, controllerPort, serviceHost,
                servicePort, containerCount, restPort, enableAuth, passwordAuthHandlerInputFilePath, tokenSigningKey,
                isRGWritesWithReadPermEnabled, accessTokenTtlInSeconds, false, "", "", "", "");
    }

    @Builder
    public ControllerWrapper(final String connectionString, final boolean disableEventProcessor,
                             final boolean disableControllerCluster,
                             final int controllerPort, final String serviceHost, final int servicePort,
                             final int containerCount, int restPort,
                             boolean enableAuth, String passwordAuthHandlerInputFilePath,
                             String tokenSigningKey, boolean isRGWritesWithReadPermEnabled,
                             int accessTokenTtlInSeconds, boolean enableTls, String serverCertificatePath,
                             String serverKeyPath, String serverKeystorePath, String serverKeystorePasswordPath) {

        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder().connectionString(connectionString)
                .initialSleepInterval(500)
                .maxRetries(10)
                .sessionTimeoutMs(10 * 1000)
                .namespace("pravega/" + UUID.randomUUID())
                .build();
        StoreClientConfig storeClientConfig = StoreClientConfigImpl.withPravegaTablesClient(zkClientConfig);

        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(false)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .containerCount(containerCount)
                .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(serviceHost, servicePort, containerCount))
                .build();

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .build();

        Optional<ControllerEventProcessorConfig> eventProcessorConfig;
        if (!disableEventProcessor) {
            eventProcessorConfig = Optional.of(ControllerEventProcessorConfigImpl.builder()
                    .scopeName(NameUtils.INTERNAL_SCOPE_NAME)
                    .commitStreamName(NameUtils.getInternalNameForStream("commitStream"))
                    .abortStreamName(NameUtils.getInternalNameForStream("abortStream"))
                    .kvtStreamName(NameUtils.getInternalNameForStream("kvTableStream"))
                    .commitStreamScalingPolicy(ScalingPolicy.fixed(2))
                    .abortStreamScalingPolicy(ScalingPolicy.fixed(2))
                    .scaleStreamScalingPolicy(ScalingPolicy.fixed(2))
                    .kvtStreamScalingPolicy(ScalingPolicy.fixed(5))
                    .commitReaderGroupName("commitStreamReaders")
                    .commitReaderGroupSize(1)
                    .abortReaderGroupName("abortStreamReaders")
                    .abortReaderGroupSize(1)
                    .kvtReaderGroupName("kvtStreamReaders")
                    .kvtReaderGroupSize(1)
                    .commitCheckpointConfig(CheckpointConfig.periodic(10, 10))
                    .abortCheckpointConfig(CheckpointConfig.periodic(10, 10))
                    .build());
        } else {
            eventProcessorConfig = Optional.empty();
        }

        GRPCServerConfig grpcServerConfig = GRPCServerConfigImpl.builder()
                .port(controllerPort)
                .publishedRPCHost("localhost")
                .publishedRPCPort(controllerPort)
                .replyWithStackTraceOnError(false)
                .requestTracingEnabled(true)
                .authorizationEnabled(enableAuth)
                .tokenSigningKey(tokenSigningKey)
                .accessTokenTTLInSeconds(accessTokenTtlInSeconds)
                .isRGWritesWithReadPermEnabled(isRGWritesWithReadPermEnabled)
                .userPasswordFile(passwordAuthHandlerInputFilePath)
                .tlsEnabled(enableTls)
                .tlsTrustStore(serverCertificatePath)
                .tlsCertFile(serverCertificatePath)
                .tlsKeyFile(serverKeyPath)
                .build();

        Optional<RESTServerConfig> restServerConfig = restPort > 0 ?
                Optional.of(RESTServerConfigImpl.builder().host("localhost").port(restPort)
                        .tlsEnabled(enableTls)
                        .keyFilePath(serverKeystorePath)
                        .keyFilePasswordPath(serverKeystorePasswordPath)
                        .build()) :
                Optional.<RESTServerConfig>empty();

        ControllerServiceConfig serviceConfig = ControllerServiceConfigImpl.builder()
                .threadPoolSize(15)
                .storeClientConfig(storeClientConfig)
                .controllerClusterListenerEnabled(!disableControllerCluster)
                .hostMonitorConfig(hostMonitorConfig)
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorConfig(eventProcessorConfig)
                .grpcServerConfig(Optional.of(grpcServerConfig))
                .restServerConfig(restServerConfig)
                .retentionFrequency(Duration.ofSeconds(1))
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
