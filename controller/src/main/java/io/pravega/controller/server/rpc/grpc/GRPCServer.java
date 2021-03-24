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
package io.pravega.controller.server.rpc.grpc;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.server.security.auth.handler.AuthHandlerManager;
import io.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import io.pravega.shared.controller.tracing.RPCTracingHelpers;
import java.io.File;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * gRPC based RPC Server for the Controller.
 */
@Slf4j
public class GRPCServer extends AbstractIdleService {

    private final String objectId;
    private final Server server;
    private final GRPCServerConfig config;

    @Getter
    private final AuthHandlerManager authHandlerManager;

    /**
     * Create gRPC server on the specified port.
     *
     * @param controllerService The controller service implementation.
     * @param serverConfig      The RPC Server config.
     * @param requestTracker    Cache to track and access to client request identifiers.
     */
    public GRPCServer(ControllerService controllerService, GRPCServerConfig serverConfig, RequestTracker requestTracker) {
        this.objectId = "gRPCServer";
        this.config = serverConfig;
        GrpcAuthHelper authHelper = new GrpcAuthHelper(serverConfig.isAuthorizationEnabled(),
                serverConfig.getTokenSigningKey(), serverConfig.getAccessTokenTTLInSeconds());
        ServerBuilder<?> builder = ServerBuilder
                .forPort(serverConfig.getPort())
                .addService(ServerInterceptors.intercept(new ControllerServiceImpl(controllerService, authHelper, requestTracker,
                                serverConfig.isReplyWithStackTraceOnError(), serverConfig.isRGWritesWithReadPermEnabled()),
                        RPCTracingHelpers.getServerInterceptor(requestTracker)));
        if (serverConfig.isAuthorizationEnabled()) {
            this.authHandlerManager = new AuthHandlerManager(serverConfig);
            this.authHandlerManager.registerInterceptors(builder);
        } else {
            this.authHandlerManager = null;
        }

        if (serverConfig.isTlsEnabled() && !Strings.isNullOrEmpty(serverConfig.getTlsCertFile())) {
            builder = builder.useTransportSecurity(new File(serverConfig.getTlsCertFile()),
                    new File(serverConfig.getTlsKeyFile()));
        }
        this.server = builder.build();
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
