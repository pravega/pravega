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

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.controller.server.rest.ModelHelper;
import com.emc.pravega.controller.server.rest.contract.request.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.request.UpdateStreamRequest;
import com.emc.pravega.controller.server.rest.v1.ApiV1;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.stream.StreamConfiguration;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class StreamMetaDataResourceImpl implements ApiV1.StreamMetaData {

    private final ControllerService controllerService;

    public StreamMetaDataResourceImpl(ControllerService controllerService) {
        this.controllerService = controllerService;
    }


    @Override
    public void createStream(final String scope, final CreateStreamRequest createStreamRequest,
                             final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "createStream");

        StreamConfiguration streamConfiguration = ModelHelper.getCreateStreamConfig(createStreamRequest, scope);
        CompletableFuture<CreateStreamStatus> createStreamStatus = controllerService.createStream(streamConfiguration,
                System.currentTimeMillis());

        createStreamStatus.thenApply(streamStatus -> {
                    if (streamStatus == CreateStreamStatus.SUCCESS) {
                        return Response.ok(ModelHelper.encodeStreamResponse(streamConfiguration))
                                .status(Status.CREATED).build();
                    } else if (streamStatus == CreateStreamStatus.STREAM_EXISTS) {
                        return Response.status(Status.CONFLICT).entity("Stream Exists").build();
                    } else {
                        return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error")
                                .build();
                    }
                }
        ).thenApply(response -> asyncResponse.resume(response));

        LoggerHelpers.traceLeave(log, "createStream", traceId);
    }

    @Override
    public void updateStreamConfig(final String scope, final String stream,
                                   final UpdateStreamRequest updateStreamRequest,
                                   final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "updateStreamConfig");

        StreamConfiguration streamConfiguration = ModelHelper.getUpdateStreamConfig(updateStreamRequest, scope, stream);
        CompletableFuture<UpdateStreamStatus> updateStreamStatus = controllerService.alterStream(streamConfiguration);

        updateStreamStatus.thenApply(streamStatus -> {
                    if (streamStatus == UpdateStreamStatus.SUCCESS) {
                        return Response.ok(ModelHelper.encodeStreamResponse(streamConfiguration))
                                .status(Status.CREATED).build();
                    } else if (streamStatus == UpdateStreamStatus.STREAM_NOT_FOUND) {
                        return Response.status(Status.NOT_FOUND).entity("Stream Not Found").build();
                    } else {
                        return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error").build();
                    }
                }
        ).thenApply(response -> asyncResponse.resume(response));

        LoggerHelpers.traceLeave(log, "updateStreamConfig", traceId);
    }

    @Override
    public void getStreamConfig(String scope, String stream, final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamConfig");

        StreamMetadataStore streamStore = controllerService.getStreamStore();
        streamStore.getConfiguration(stream)
                .thenApply(streamConfig -> Response.status(Status.OK).entity(ModelHelper.encodeStreamResponse(streamConfig)).build())
                .exceptionally(exception -> {
                    if (exception.getCause() instanceof DataNotFoundException) {
                        return Response.status(Status.NOT_FOUND).entity("Stream Not found").build();
                    } else {
                        return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Internal Server error").build();
                    }
                }).thenApply(response -> asyncResponse.resume(response));

        LoggerHelpers.traceLeave(log, "getStreamConfig", traceId);
    }
}
