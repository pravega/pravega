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

import com.emc.pravega.controller.stream.api.v1.ControllerService;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.controller.util.ThriftAsyncCallback;
import com.emc.pravega.controller.util.ThriftHelper;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.PositionInternal;
import com.emc.pravega.stream.SegmentUri;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamSegments;
import com.emc.pravega.stream.impl.model.ModelHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * RPC based implementation of Stream Controller V1 API.
 */
@Slf4j
public class ApiController implements ControllerApi.Admin, ControllerApi.Consumer, ControllerApi.Producer {

    private final ControllerService.AsyncClient client;


    public ApiController(String host, int port) {
        try {
            // initialize transport, protocol factory, and async client manager
            final TNonblockingTransport transport = new TNonblockingSocket(host, port);
            TAsyncClientManager asyncClientManager = new TAsyncClientManager();
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

            // create client
            client = new ControllerService.AsyncClient(protocolFactory, asyncClientManager, transport);
        } catch (IOException ioe) {
            log.error("Exception" + ioe.getMessage());
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public CompletableFuture<Status> createStream(StreamConfiguration streamConfig) {
        log.info("Invoke AdminService.Client.createStream() with streamConfiguration: {}", streamConfig);

        ThriftAsyncCallback<ControllerService.AsyncClient.createStream_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.createStream(ModelHelper.decode(streamConfig), callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult));
    }

    @Override
    public CompletableFuture<Status> alterStream(StreamConfiguration streamConfig) {
        log.info("Invoke AdminService.Client.alterStream() with streamConfiguration: {}", streamConfig);

        ThriftAsyncCallback<ControllerService.AsyncClient.alterStream_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.alterStream(ModelHelper.decode(streamConfig), callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult));
    }

    @Override
    public CompletableFuture<List<PositionInternal>> getPositions(String stream, long timestamp, int count) {
        log.info("Invoke ConsumerService.Client.getPositions() for stream: {}, timestamp: {}, count: {}", stream, timestamp, count);

        ThriftAsyncCallback<ControllerService.AsyncClient.getPositions_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.getPositions(stream, timestamp, count, callback);
            return null;
        });
        return callback
                .getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult))
                .thenApply(result -> {
                    log.debug("Received the following data from the controller {}", result);
                    return result.stream().map(ModelHelper::encode).collect(Collectors.toList());
                });
    }

    @Override
    public CompletableFuture<List<PositionInternal>> updatePositions(String stream, List<PositionInternal> positions) {
        log.info("Invoke ConsumerService.Client.updatePositions() for positions: {} ", positions);

        List<com.emc.pravega.controller.stream.api.v1.Position> transformed =
                positions.stream().map(ModelHelper::decode).collect(Collectors.toList());

        ThriftAsyncCallback<ControllerService.AsyncClient.updatePositions_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.updatePositions(stream, transformed, callback);
            return null;
        });
        return callback
                .getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult))
                .thenApply(result -> {
                    log.debug("Received the following data from the controller {}", result);
                    return result.stream().map(ModelHelper::encode).collect(Collectors.toList());
                });
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(String stream) {
        //Use RPC client to invoke getPositions
        log.info("Invoke ProducerService.Client.getCurrentSegments() for stream: {}", stream);

        ThriftAsyncCallback<ControllerService.AsyncClient.getCurrentSegments_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.getCurrentSegments(stream, callback);
            return callback.getResult();
        });
        return callback
                .getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult))
                .thenApply(result ->
                                new StreamSegments(
                                        result.stream().map(ModelHelper::encode).collect(Collectors.toList()),
                                        System.currentTimeMillis())
                );
    }

    @Override
    public CompletableFuture<SegmentUri> getURI(String stream, int segmentNumber) {
        ThriftAsyncCallback<ControllerService.AsyncClient.getURI_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.getURI(stream, segmentNumber, callback);
            return callback.getResult();
        });
        return callback
                .getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult))
                .thenApply(ModelHelper::encode);
    }
}
