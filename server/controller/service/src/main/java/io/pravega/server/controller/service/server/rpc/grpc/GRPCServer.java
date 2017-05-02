/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.server.rpc.grpc;

import io.pravega.common.LoggerHelpers;
import io.pravega.server.controller.service.server.ControllerService;
import io.pravega.server.controller.service.server.rpc.grpc.v1.ControllerServiceImpl;
import com.google.common.util.concurrent.AbstractIdleService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * gRPC based RPC Server for the Controller.
 */
@Slf4j
public class GRPCServer extends AbstractIdleService {

    private final String objectId;
    private final Server server;
    private final GRPCServerConfig config;

    /**
     * Create gRPC server on the specified port.
     *
     * @param controllerService The controller service implementation.
     * @param serverConfig      The RPC Server config.
     */
    public GRPCServer(ControllerService controllerService, GRPCServerConfig serverConfig) {
        this.objectId = "gRPCServer";
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
    protected void startUp() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        try {
            log.info("Starting gRPC server listening on port: {}", this.config.getPort());
            this.server.start();
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    /**
     * Gracefully stop gRPC server.
     */
    @Override
    protected void shutDown() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "shutDown");
        try {
            log.info("Stopping gRPC server listening on port: {}", this.config.getPort());
            this.server.shutdown();
            log.info("Awaiting termination of gRPC server");
            this.server.awaitTermination();
            log.info("gRPC server terminated");
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
    }
}
