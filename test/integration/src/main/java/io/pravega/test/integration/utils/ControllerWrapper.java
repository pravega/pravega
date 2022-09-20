/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.utils;

import io.pravega.shared.NameUtils;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.server.ControllerServiceConfig;
import io.pravega.controller.server.ControllerServiceMain;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
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
import io.pravega.common.Exceptions;
import io.pravega.client.control.impl.Controller;
import io.pravega.shared.rest.RESTServerConfig;
import io.pravega.shared.rest.impl.RESTServerConfigImpl;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.pravega.test.common.SecurityConfigDefaults;
import lombok.Builder;
import lombok.SneakyThrows;
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
        this(connectionString, disableEventProcessor, false, controllerPort, serviceHost,
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
                true, accessTokenTtlInSeconds);
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
                isRGWritesWithReadPermEnabled, accessTokenTtlInSeconds, false, SecurityConfigDefaults.TLS_PROTOCOL_VERSION, "", "", "", "");
    }

    @Builder
    public ControllerWrapper(final String connectionString, final boolean disableEventProcessor,
                             final boolean disableControllerCluster,
                             final int controllerPort, final String serviceHost, final int servicePort,
                             final int containerCount, int restPort,
                             boolean enableAuth, String passwordAuthHandlerInputFilePath,
                             String tokenSigningKey, boolean isRGWritesWithReadPermEnabled,
                             int accessTokenTtlInSeconds, boolean enableTls, String[] tlsProtocolVersion, String serverCertificatePath,
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
                .tlsProtocolVersion(tlsProtocolVersion)
                .tlsTrustStore(serverCertificatePath)
                .tlsCertFile(serverCertificatePath)
                .tlsKeyFile(serverKeyPath)
                .build();

        Optional<RESTServerConfig> restServerConfig = restPort > 0 ?
                Optional.of(RESTServerConfigImpl.builder().host("localhost").port(restPort)
                        .tlsEnabled(enableTls)
                        .tlsProtocolVersion(tlsProtocolVersion)
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
                .minBucketRedistributionIntervalInSeconds(Config.MIN_BUCKET_REDISTRIBUTION_INTERVAL_IN_SECONDS)
                .build();

        controllerServiceMain = new ControllerServiceMain(serviceConfig);
        controllerServiceMain.startAsync();
    }

    public ControllerService getControllerService() {
        return Exceptions.handleInterruptedCall(() -> this.controllerServiceMain.awaitServiceStarting().getControllerService());
    }

    public Controller getController() {
        return Exceptions.handleInterruptedCall(() -> this.controllerServiceMain.awaitServiceStarting().getController());
    }

    @SneakyThrows
    public void awaitRunning() {
        this.controllerServiceMain.awaitServiceStarting().awaitRunning(30, TimeUnit.SECONDS);
    }

    @SneakyThrows
    public void awaitPaused() {
        this.controllerServiceMain.awaitServicePausing().awaitTerminated(60, TimeUnit.SECONDS);
    }

    @SneakyThrows
    public void awaitTerminated() {
        this.controllerServiceMain.awaitTerminated(30, TimeUnit.SECONDS);
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
