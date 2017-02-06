/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
