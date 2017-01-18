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
package com.emc.pravega.controller.server.rest.resources;

import com.emc.pravega.controller.server.rest.ModelHelper;
import com.emc.pravega.controller.server.rest.contract.request.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.request.UpdateStreamRequest;
import com.emc.pravega.controller.server.rest.v1.ApiV1;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.jersey.server.ManagedAsync;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
public class StreamMetaDataResourceImpl implements ApiV1.StreamMetaData {

    private ControllerService controllerService;

    public StreamMetaDataResourceImpl(ControllerService controllerService) {
        this.controllerService = controllerService;
    }

    @ManagedAsync
    @Override
    public void createStream(final AsyncResponse asyncResponse, CreateStreamRequest createStreamRequest, String scope) {
        StreamConfigurationImpl streamConfiguration = ModelHelper.getCreateStreamConfig(createStreamRequest, scope);
        log.info("CreateStream called for: \nStream name: {}, \nScope name: {}, \nScaling policy: {}, \nRetention policy: {} ", streamConfiguration.getName(),
                streamConfiguration.getScope(), streamConfiguration.getScalingPolicy(), streamConfiguration.getRetentionPolicy());

        CompletableFuture<CreateStreamStatus> createStreamStatus = controllerService.createStream(streamConfiguration, System.currentTimeMillis());

        CompletableFuture<Response> response = createStreamStatus.thenApply(x -> {
                    if (x == CreateStreamStatus.SUCCESS ) {
                        return Response.ok(streamConfiguration).status(201).build();
                    } else if (x == CreateStreamStatus.STREAM_EXISTS) {
                        return Response.status(409).build();
                    } else {
                        return Response.status(500).build();
                    }
                }
        );

        try {
            asyncResponse.resume(response.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void updateStreamConfig(final AsyncResponse asyncResponse, UpdateStreamRequest updateStreamRequest, String scope, String stream) {
        StreamConfigurationImpl streamConfiguration = ModelHelper.getUpdateStreamConfig(updateStreamRequest, scope, stream);

        log.info("UpdateStream called for: \nStream name: {}, \nScope name: {}, \nScaling policy: {}, \nRetention policy: {} ", streamConfiguration.getName(),
                streamConfiguration.getScope(), streamConfiguration.getScalingPolicy(), streamConfiguration.getRetentionPolicy());

        CompletableFuture<UpdateStreamStatus> updateStreamStatus = controllerService.alterStream(streamConfiguration);

        CompletableFuture<Response> response = updateStreamStatus.thenApply(x -> {
                    if (x == UpdateStreamStatus.SUCCESS ) {
                        return Response.ok(streamConfiguration).status(201).build();
                    } else if (x == UpdateStreamStatus.STREAM_NOT_FOUND) {
                        return Response.status(404).build();
                    } else {
                        return Response.status(500).build();
                    }
                }
        );

        try {
            asyncResponse.resume(response.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void getStreamConfig(final AsyncResponse asyncResponse, String scope, String stream) {
        log.info("GetStreamConfig called for: \nStream name: {}, \nScope name: {} ", stream, scope);

        StreamMetadataStore streamStore = controllerService.getStreamStore();
        CompletableFuture<StreamConfiguration>  streamConfig = streamStore.getConfiguration(stream);

        CompletableFuture<Response> response = streamConfig.thenApply( x -> {
                    if (x != null) {
                        return Response.ok(x).status(200).build();
                    } else {
                        return Response.status(404).build();
                    }
                }
        );

        try {
            //System.out.println("streamConfig get "+streamConfig.get());
            asyncResponse.resume(response.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
