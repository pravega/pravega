/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.server;

import io.pravega.server.controller.service.fault.ControllerClusterListenerConfig;
import io.pravega.server.controller.service.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.server.controller.service.server.rest.RESTServerConfig;
import io.pravega.server.controller.service.server.rpc.grpc.GRPCServerConfig;
import io.pravega.server.controller.service.store.client.StoreClientConfig;
import io.pravega.server.controller.service.store.host.HostMonitorConfig;
import io.pravega.server.controller.service.timeout.TimeoutServiceConfig;

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
