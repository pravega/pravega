/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rpc.grpc;

import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * gRPC based RPC Server for the Controller.
 */
@Slf4j
public class GRPCServer {
    public static void start(ControllerService controllerService, int rpcPort) throws IOException {
        ServerBuilder
                .forPort(rpcPort)
                .addService(new ControllerServiceImpl(controllerService))
                .build()
                .start();
        log.info("gRPC server listening on port: " + rpcPort);
    }
}
