/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rest.resources;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.controller.server.rest.ModelHelper;
import com.emc.pravega.controller.server.rest.contract.request.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.request.UpdateStreamRequest;
import com.emc.pravega.controller.server.rest.v1.ApiV1;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.stream.StreamConfiguration;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.concurrent.CompletableFuture;

/**
 * Stream metadata resource implementation.
 */
@Slf4j
public class StreamMetadataResourceImpl implements ApiV1.StreamMetadata {

    private final ControllerService controllerService;

    public StreamMetadataResourceImpl(ControllerService controllerService) {
        this.controllerService = controllerService;
    }

    /**
     * Implementation of createStream REST API.
     *
     * @param scope               The scope of stream
     * @param createStreamRequest The object conforming to createStream request json
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing
     */
    @Override
    public void createStream(final String scope, final CreateStreamRequest createStreamRequest,
                             final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "createStream");

        StreamConfiguration streamConfiguration = ModelHelper.getCreateStreamConfig(createStreamRequest, scope);
        CompletableFuture<CreateStreamStatus> createStreamStatus = controllerService.createStream(streamConfiguration,
                System.currentTimeMillis());

        createStreamStatus.thenApply(streamStatus -> {
                    if (streamStatus == CreateStreamStatus.SUCCESS) {
                        return Response.status(Status.CREATED).
                                entity(ModelHelper.encodeStreamResponse(streamConfiguration)).build();
                    } else if (streamStatus == CreateStreamStatus.STREAM_EXISTS) {
                        return Response.status(Status.CONFLICT).build();
                    } else {
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }
        ).exceptionally(exception -> {
            log.debug("Exception occurred while executing createStream: " + exception);
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(response -> asyncResponse.resume(response));

        LoggerHelpers.traceLeave(log, "createStream", traceId);
    }

    /**
     * Implementation of updateStreamConfig REST API.
     *
     * @param scope               The scope of stream
     * @param stream              The name of stream
     * @param updateStreamRequest The object conforming to updateStreamConfig request json
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing
     */
    @Override
    public void updateStreamConfig(final String scope, final String stream,
                                   final UpdateStreamRequest updateStreamRequest,
                                   final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "updateStreamConfig");

        StreamConfiguration streamConfiguration = ModelHelper.getUpdateStreamConfig(updateStreamRequest, scope, stream);
        CompletableFuture<UpdateStreamStatus> updateStreamStatus = controllerService.alterStream(streamConfiguration);

        updateStreamStatus.thenApply(streamStatus -> {
                    if (streamStatus == UpdateStreamStatus.SUCCESS) {
                        return Response.status(Status.CREATED).
                                entity(ModelHelper.encodeStreamResponse(streamConfiguration)).build();
                    } else if (streamStatus == UpdateStreamStatus.STREAM_NOT_FOUND) {
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }
        ).exceptionally(exception -> {
            log.debug("Exception occurred while executing updateStreamConfig: " + exception);
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(response -> asyncResponse.resume(response));

        LoggerHelpers.traceLeave(log, "updateStreamConfig", traceId);
    }

    /**
     * Implementation of getStreamConfig REST API.
     *
     * @param scope         The scope of stream
     * @param stream        The name of stream
     * @param asyncResponse AsyncResponse provides means for asynchronous server side response processing
     */
    @Override
    public void getStreamConfig(String scope, String stream, final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamConfig");

        controllerService.getStreamConfiguration(scope, stream)
                .thenApply(streamConfig -> {
                    return Response.status(Status.OK).entity(ModelHelper.encodeStreamResponse(streamConfig)).build();
                })
                .exceptionally(exception -> {
                    if (exception.getCause() instanceof DataNotFoundException || exception instanceof DataNotFoundException) {
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.debug("Exception occurred while executing getStreamConfig: " + exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).thenApply(response -> asyncResponse.resume(response));

        LoggerHelpers.traceLeave(log, "getStreamConfig", traceId);
    }
}
