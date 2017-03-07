/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rpc.grpc;

import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import com.google.common.util.concurrent.AbstractService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.Lombok;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * gRPC based RPC Server for the Controller.
 */
@Slf4j
public class GRPCServer extends AbstractService {

    private final Server server;
    private final GRPCServerConfig config;

    /**
     * Create gRPC server on the specified port.
     *
     * @param controllerService The controller service implementation.
     * @param serverConfig      The RPC Server config.
     */
    public GRPCServer(ControllerService controllerService, GRPCServerConfig serverConfig) {
        this.config = serverConfig;
        this.server = ServerBuilder
                .forPort(serverConfig.getPort())
                .addService(new ControllerServiceImpl(controllerService))
                .build();
    }

    /**
     * Start gRPC server.
     */
    @Override
    protected void doStart() {
        try {
            log.info("Starting gRPC server listening on port: {}", this.config.getPort());
            this.server.start();
            notifyStarted();
        } catch (IOException e) {
            log.error("Failed to start gRPC server on port: {}. Error: {}", this.config.getPort(), e);
            // Throwing this error will mark the service as FAILED.
            throw Lombok.sneakyThrow(e);
        }
    }

    /**
     * Gracefully stop gRPC server.
     */
    @Override
    protected void doStop() {
        log.info("Stopping gRPC server listening on port: {}", this.config.getPort());
        this.server.shutdown();
    }
}
