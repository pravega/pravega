/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

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
