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
package com.emc.pravega.controller.server;

import com.emc.pravega.controller.contract.v1.api.Api;
import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.AdminServiceImpl;
import com.emc.pravega.controller.server.rpc.v1.ConsumerServiceImpl;
import com.emc.pravega.controller.server.rpc.v1.ProducerServiceImpl;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamSegments;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Entry point of controller server.
 */
public class Main {

    public static void main(String[] args) {
        //1) LOAD configuration.

        //2) initialize implementation objects, with right parameters/configuration.

        //2.1) initialize implementation of Api.Admin
        Api.Admin adminApi = new Api.Admin() { //sample implementation
            @Override
            public CompletableFuture<Status> createStream(StreamConfiguration streamConfig) {
                return null;
            }

            @Override
            public CompletableFuture<Status> alterStream(StreamConfiguration streamConfig) {
                return null;
            }
        };
        //2.2) initialize implementation of Api.Consumer
        Api.Consumer consumerApi = new Api.Consumer() { //sample implementation
            @Override
            public CompletableFuture<List<Position>> getPositions(String stream, long timestamp, int count) {
                return null;
            }

            @Override
            public CompletableFuture<List<Position>> updatePositions(List<Position> positions) {
                return null;
            }
        };
        //2.3) initialize implementation of Api.Producer
        Api.Producer producerApi = new Api.Producer() { //sample implementation
            @Override
            public CompletableFuture<List<StreamSegments>> getCurrentSegments(String stream) {
                return null;
            }

            @Override
            public CompletableFuture<URI> getURI(SegmentId id) {
                return null;
            }
        };

        //3) start the Server implementations.
        //3.1) start RPC server with v1 implementation. Enable other versions if required.
        RPCServer.start(new AdminServiceImpl(adminApi), new ConsumerServiceImpl(consumerApi), new ProducerServiceImpl(producerApi));

    }
}
