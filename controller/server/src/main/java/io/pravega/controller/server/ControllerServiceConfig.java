/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import io.pravega.controller.fault.ControllerClusterListenerConfig;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.rest.RESTServerConfig;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.timeout.TimeoutServiceConfig;

import java.util.Optional;

/**
 * Configuration of the controller service.
 */
public interface ControllerServiceConfig {
    /**
     * Fetches the size of the thread pool used by controller service API handler.
     *
     * @return The size of the thread pool used by controller service API handler.
     */
    int getServiceThreadPoolSize();

    /**
     * Fetches the size of the thread pool used by controller's task processor.
     *
     * @return The size of the thread pool used by controller's task processor.
     */
    int getTaskThreadPoolSize();

    /**
     * Fetches the size of the thread pool used by controller's stream metadata store.
     *
     * @return The size of the thread pool used by controller's stream metadata store.
     */
    int getStoreThreadPoolSize();

    /**
     * Fetches the size of the thread pool used by controller's event processors.
     *
     * @return The size of the thread pool used by controller's event processors.
     */
    int getEventProcThreadPoolSize();

    /**
     * Fetches the size of the thread pool used by controller's request handlers.
     *
     * @return The size of the thread pool used by controller's request handlers.
     */
    int getRequestHandlerThreadPoolSize();

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
     * Fetches whether the controller cluster listener is enabled, and its configuration if enabled.
     *
     * @return Whether the controller cluster listener is enabled, and its configuration if enabled.
     */
    Optional<ControllerClusterListenerConfig> getControllerClusterListenerConfig();

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
}
