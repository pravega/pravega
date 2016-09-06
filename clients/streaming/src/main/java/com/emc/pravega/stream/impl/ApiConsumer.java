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

import com.emc.pravega.controller.stream.api.v1.ConsumerService;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.impl.model.ModelHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * RPC based implementation of Stream Controller Consumer V1 API
 */
@Slf4j
public class ApiConsumer extends BaseClient implements ControllerApi.Consumer {
    public static final String CONSUMER_SERVICE = "consumerService";

    private final ConsumerService.Client client;

    public ApiConsumer(final String hostName, final int port) {
        super(hostName, port, CONSUMER_SERVICE);
        client = new ConsumerService.Client(getTProtocol());
    }

    @Override
    public CompletableFuture<List<Position>> getPositions(String stream, long timestamp, int count) {
        //Use RPC client to invoke getPositions
        log.info("Invoke ConsumerService.Client.getPositions() for stream: {}, timestamp: {}, count: {}", stream, timestamp, count);

        CompletableFuture<List<Position>> resultFinal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                //invoke RPC client
                List<com.emc.pravega.controller.stream.api.v1.Position> result = client.getPositions(stream, timestamp, count);
                //encode the result back to Model class
                List<Position> resultModel = result.parallelStream().map(ModelHelper::encode)
                        .collect(Collectors.toList());
                log.debug("Received the following data from the controller {}", result);
                resultFinal.complete(resultModel);
            } catch (TException e) {
                resultFinal.completeExceptionally(e);
            }
        }, service);

        return resultFinal;
    }

    @Override
    public CompletableFuture<List<Position>> updatePositions(String stream, List<Position> positions) {
        //Use RPC client to invoke updatePositions
        log.info("Invoke ConsumerService.Client.updatePositions() for positions: {} ", positions);

        CompletableFuture<List<Position>> resultFinal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                //invoke RPC client
                List<com.emc.pravega.controller.stream.api.v1.Position> result =
                        client.updatePositions(stream, positions.stream().map(p -> ModelHelper.decode(p)).collect(Collectors.toList()));
                //encode the result back to Model class
                List<Position> resultModel = result.parallelStream().map(ModelHelper::encode).collect(Collectors.toList());
                log.debug("Received the following data from the controller {}", result);
                resultFinal.complete(resultModel);
            } catch (TException e) {
                resultFinal.completeExceptionally(e);
            }
        }, service);

        return resultFinal;
    }
}
