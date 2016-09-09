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

import com.emc.pravega.controller.stream.api.v1.ProducerService;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.SegmentUri;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.StreamSegments;
import com.emc.pravega.stream.impl.model.ModelHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * RPC based implementation of Stream Controller Producer V1 API
 */
@Slf4j
public class ApiProducer extends BaseClient implements ControllerApi.Producer {
    public static final String PRODUCER_SERVICE = "producerService";

    private final ProducerService.Client client;

    public ApiProducer(final String hostName, final int port) {
        super(hostName, port, PRODUCER_SERVICE);
        client = new ProducerService.Client(getTProtocol());
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(String stream) {
        //Use RPC client to invoke getPositions
        log.info("Invoke ProducerService.Client.getCurrentSegments() for stream: {}", stream);

        CompletableFuture<StreamSegments> resultFinal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                //invoke RPC client and encode
                resultFinal.complete(new StreamSegments(client.getCurrentSegments(stream).
                        stream().map(ModelHelper::encode).collect(Collectors.toList()),
                        System.currentTimeMillis()));
            } catch (TException e) {
                resultFinal.completeExceptionally(e);
            }
        }, service);

        return resultFinal;
    }

    @Override
    public CompletableFuture<SegmentUri> getURI(SegmentId id) {
        CompletableFuture<SegmentUri> resultFinal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                resultFinal.complete(ModelHelper.encode(client.getURI(ModelHelper.decode(id))));
            } catch (TException e) {
                resultFinal.completeExceptionally(e);
            }
        }, service);

        return resultFinal;
    }
}