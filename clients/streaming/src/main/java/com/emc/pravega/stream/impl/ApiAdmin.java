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
package com.emc.pravega.stream.impl;

import com.emc.pravega.controller.stream.api.v1.AdminService;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.model.ModelHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.util.concurrent.CompletableFuture;

/**
 * RPC based implementation of Stream Controller Admin V1 API
 */
@Slf4j
public class ApiAdmin extends BaseClient implements ControllerApi.Admin {
    public static final String ADMIN_SERVICE = "adminService";

    private final AdminService.Client client;

    public ApiAdmin(final String hostName, final int port) {
        super(hostName, port, ADMIN_SERVICE);
        client = new AdminService.Client(getTProtocol());
    }

    @Override
    public CompletableFuture<Status> createStream(StreamConfiguration streamConfig) {
        //Use RPC client to invoke createStream
        log.info("Invoke AdminService.Client.createStream() with streamConfiguration: {}", streamConfig);

        CompletableFuture<Status> resultFinal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                //invoke RPC client
                Status status = client.createStream(ModelHelper.decode(streamConfig));

                log.debug("Received the following status from the controller {}", status);
                resultFinal.complete(status);
            } catch (TException e) {
                resultFinal.completeExceptionally(e);
            }
        }, service);

        return resultFinal;
    }

    @Override
    public CompletableFuture<Status> alterStream(StreamConfiguration streamConfig) {
        //Use RPC client to invoke getPositions
        log.info("Invoke AdminService.Client.alterStream() with streamConfiguration: {}", streamConfig);

        CompletableFuture<Status> resultFinal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                //invoke RPC client
                Status status = client.alterStream(ModelHelper.decode(streamConfig));
                log.debug("Received the following status from the controller {}", status);
                resultFinal.complete(status);
            } catch (TException e) {
                resultFinal.completeExceptionally(e);
            }
        }, service);

        return resultFinal;
    }
}
