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
package io.pravega.controller.server;

import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.shared.rest.RESTServerConfig;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.timeout.TimeoutServiceConfig;

import java.time.Duration;
import java.util.Optional;

/**
 * Configuration of the controller service.
 */
public interface ControllerServiceConfig {
    /**
     * Fetches the size of the thread pool used by controller.
     *
     * @return The size of the thread pool used by controller.
     */
    int getThreadPoolSize();

    /**
     * Fetches the configuration of the store client used for accessing stream metadata store.
     *
     * @return The configuration of the store client used for accessing stream metadata store.
     */
    StoreClientConfig getStoreClientConfig();

    /**
     * Fetches the configuration of HostMonitor module.
     *
     * @return The configuration of HostMonitor module.
     */
    HostMonitorConfig getHostMonitorConfig();

    /**
     * Fetches whether the controller cluster listener is enabled.
     *
     * @return Whether the controller cluster listener is enabled.
     */
    boolean isControllerClusterListenerEnabled();

    /**
     * Fetches the optional configuration item that, if specified, represents whether segment store TLS is enabled. This
     * is useful only in configuration where the Controller is not TLS enabled, but the segment store is. In the
     * configuration where both Controller and Segment Store have TLS enabled, this value is expected to be left
     * unspecified/empty.
     *
     * The returned value may take these values:
     *
     * - true/yes/y (case insensitive) - Indicating TLS is enabled for Segment Store (even if it is disabled for
     *                                   the Controller based on Controller URI)
     * - false/no/n (case insensitive) - Indicating TLS is disabled for Segment Store (even if it is enabled for the
     *                                   Controller, based on Controller URI)
     * - "" - Indicating the value was not set.  Should be interpreted from Controller URI.
     * - Any other non-empty value - Should be interpreted from Controller URI.
     *
     * @return Specified configuration value
     */
    String getTlsEnabledForSegmentStore();

    /**
     * Fetches the configuration of service managing transaction timeouts.
     *
     * @return The configuration of service managing transaction timeouts.
     */
    TimeoutServiceConfig getTimeoutServiceConfig();

    /**
     * Fetches whether the event processors are enabled, and their configuration if they are enabled.
     *
     * @return Whether the event processors are enabled, and their configuration if they are enabled.
     */
    Optional<ControllerEventProcessorConfig> getEventProcessorConfig();

    /**
     * Fetches whether gRPC server is enabled, and its configuration if it is enabled.
     *
     * @return Whether gRPC server is enabled, and its configuration if it is enabled.
     */
    Optional<GRPCServerConfig> getGRPCServerConfig();

    /**
     * Fetches whether REST server is enabled, and its configuration if it is enabled.
     *
     * @return Whether REST server is enabled, and its configuration if it is enabled.
     */
    Optional<RESTServerConfig> getRestServerConfig();

    /**
     * Frequency at which periodic retention jobs should be performed for each stream. 
     * @return Duration for retention frequency.
     */
    Duration getRetentionFrequency();

    /**
     * Frequency at which the {@link io.pravega.shared.health.HealthServiceUpdater} will update the health state of
     * the Controller service(s).
     */
    Duration getHealthCheckFrequency();

    /**
     * How long to await a graceful shutdown.
     * @return Graceful shutdown timeout.
     */
    Duration getShutdownTimeout();

    /**
     * Fetches the minimum interval between two consecutive controller bucket redistribution operations.
     *
     * @return The minimum interval between two consecutive controller bucket redistribution operations.
     */
    int getMinBucketRedistributionIntervalInSeconds();
}
