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
package io.pravega.controller.server.impl;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.controller.server.ControllerServiceConfig;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.StoreType;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.shared.rest.RESTServerConfig;
import io.pravega.controller.util.Config;
import java.time.Duration;
import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Controller Service Configuration.
 */
@ToString
@Getter
public class ControllerServiceConfigImpl implements ControllerServiceConfig {

    private final int threadPoolSize;
    private final StoreClientConfig storeClientConfig;
    private final HostMonitorConfig hostMonitorConfig;
    private final boolean controllerClusterListenerEnabled;
    private final TimeoutServiceConfig timeoutServiceConfig;

    private final String tlsEnabledForSegmentStore;

    private final Optional<ControllerEventProcessorConfig> eventProcessorConfig;

    private final Optional<GRPCServerConfig> gRPCServerConfig;

    private final Optional<RESTServerConfig> restServerConfig;

    private final Duration healthCheckFrequency;

    private final Duration retentionFrequency;
    @Getter
    private final Duration shutdownTimeout;
    private final int minBucketRedistributionIntervalInSeconds;

    @Builder
    ControllerServiceConfigImpl(final int threadPoolSize,
                                final StoreClientConfig storeClientConfig,
                                final HostMonitorConfig hostMonitorConfig,
                                final boolean controllerClusterListenerEnabled,
                                final String tlsEnabledForSegmentStore,
                                final TimeoutServiceConfig timeoutServiceConfig,
                                final Optional<ControllerEventProcessorConfig> eventProcessorConfig,
                                final Optional<GRPCServerConfig> grpcServerConfig,
                                final Optional<RESTServerConfig> restServerConfig,
                                final Duration retentionFrequency,
                                final Duration healthCheckFrequency,
                                final Duration shutdownTimeout,
                                final int minBucketRedistributionIntervalInSeconds) {
        Exceptions.checkArgument(threadPoolSize > 0, "threadPoolSize", "Should be positive integer");
        Preconditions.checkNotNull(storeClientConfig, "storeClientConfig");
        Preconditions.checkNotNull(hostMonitorConfig, "hostMonitorConfig");
        Preconditions.checkNotNull(timeoutServiceConfig, "timeoutServiceConfig");
        Preconditions.checkNotNull(storeClientConfig, "storeClientConfig");
        Preconditions.checkNotNull(hostMonitorConfig, "hostMonitorConfig");
        Preconditions.checkArgument(minBucketRedistributionIntervalInSeconds > 0,
                "minBucketRedistributionIntervalInSeconds", "Should be positive integer");
        if (controllerClusterListenerEnabled) {
            Preconditions.checkArgument(storeClientConfig.getStoreType() == StoreType.Zookeeper ||
                            storeClientConfig.getStoreType() == StoreType.PravegaTable,
                    "If controllerCluster is enabled, store type should be Zookeeper");
        }

        if (eventProcessorConfig.isPresent()) {
            Preconditions.checkNotNull(eventProcessorConfig.get());
        }
        if (grpcServerConfig.isPresent()) {
            Preconditions.checkNotNull(grpcServerConfig.get());
        }
        if (restServerConfig.isPresent()) {
            Preconditions.checkNotNull(restServerConfig.get());
        }

        this.threadPoolSize = threadPoolSize;
        this.storeClientConfig = storeClientConfig;
        this.hostMonitorConfig = hostMonitorConfig;
        this.controllerClusterListenerEnabled = controllerClusterListenerEnabled;
        this.tlsEnabledForSegmentStore = tlsEnabledForSegmentStore;
        this.timeoutServiceConfig = timeoutServiceConfig;
        this.eventProcessorConfig = eventProcessorConfig;
        this.gRPCServerConfig = grpcServerConfig;
        this.restServerConfig = restServerConfig;
        this.retentionFrequency = retentionFrequency == null ? Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES)
                : retentionFrequency;
        this.healthCheckFrequency = healthCheckFrequency == null ? Duration.ofSeconds(Config.HEALTH_CHECK_FREQUENCY) : healthCheckFrequency;
        this.shutdownTimeout = shutdownTimeout == null ? Duration.ofSeconds(10) : shutdownTimeout;
        this.minBucketRedistributionIntervalInSeconds = minBucketRedistributionIntervalInSeconds;
    }
}
