/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rest.resources;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.controller.server.rest.ModelHelper;
import com.emc.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
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
import javax.ws.rs.core.SecurityContext;
import java.util.concurrent.CompletableFuture;

/**
 * Stream metadata resource implementation.
 */
@Slf4j
public class StreamMetadataResourceImpl implements ApiV1.ScopesApi {

    private final ControllerService controllerService;

    public StreamMetadataResourceImpl(ControllerService controllerService) {
        this.controllerService = controllerService;
    }

    /**
     * Implementation of createScope REST API.
     *
     * @param createScopeRequest  The object conforming to createScope request json.
     * @param securityContext     The security for API access.
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void createScope(final CreateScopeRequest createScopeRequest, final SecurityContext securityContext,
            final AsyncResponse asyncResponse) {
        asyncResponse.resume(Response.status(Status.NOT_IMPLEMENTED));
    }

    /**
     * Implementation of createStream REST API.
     *
     * @param scopeName           The scope name of stream.
     * @param createStreamRequest The object conforming to createStream request json.
     * @param securityContext     The security for API access.
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void createStream(final String scopeName, final CreateStreamRequest createStreamRequest,
            final SecurityContext securityContext, final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "createStream");

        StreamConfiguration streamConfiguration = ModelHelper.getCreateStreamConfig(createStreamRequest, scopeName);
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
     * Implementation of deleteScope REST API.
     *
     * @param scopeName           The scope name of stream.
     * @param securityContext     The security for API access.
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void deleteScope(final String scopeName, final SecurityContext securityContext,
            final AsyncResponse asyncResponse) {
        asyncResponse.resume(Response.status(Status.NOT_IMPLEMENTED));
    }

    /**
     * Implementation of deleteStream REST API.
     *
     * @param scopeName           The scope name of stream.
     * @param streamName          The name of stream.
     * @param securityContext     The security for API access.
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void deleteStream(final String scopeName, final String streamName, final SecurityContext securityContext,
            final AsyncResponse asyncResponse) {
        asyncResponse.resume(Response.status(Status.NOT_IMPLEMENTED));
    }

    /**
     * Implementation of getStream REST API.
     *
     * @param scopeName         The scope name of stream.
     * @param streamName        The name of stream.
     * @param securityContext   The security for API access.
     * @param asyncResponse     AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void getStream(final String scopeName, final String streamName, final SecurityContext securityContext,
            final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamConfig");

        StreamMetadataStore streamStore = controllerService.getStreamStore();
        streamStore.getConfiguration(streamName)
                .thenApply(streamConfig -> {
                    return Response.status(Status.OK).entity(ModelHelper.encodeStreamResponse(streamConfig)).build();
                })
                .exceptionally(exception -> {
                    if (exception.getCause() instanceof DataNotFoundException
                            || exception instanceof DataNotFoundException) {
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.debug("Exception occurred while executing getStreamConfig: " + exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).thenApply(response -> asyncResponse.resume(response));

        LoggerHelpers.traceLeave(log, "getStreamConfig", traceId);
    }

    /**
     * Implementation of listScopes REST API.
     *
     * @param securityContext     The security for API access.
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void listScopes(final SecurityContext securityContext, final AsyncResponse asyncResponse) {
        asyncResponse.resume(Response.status(Status.NOT_IMPLEMENTED));
    }

    /**
     * Implementation of listStreams REST API.
     *
     * @param scopeName           The scope name of stream.
     * @param securityContext     The security for API access.
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void listStreams(final String scopeName, final SecurityContext securityContext,
            final AsyncResponse asyncResponse) {
        asyncResponse.resume(Response.status(Status.NOT_IMPLEMENTED));
    }

    /**
     * Implementation of updateStream REST API.
     *
     * @param scopeName           The scope name of stream.
     * @param streamName          The name of stream.
     * @param updateStreamRequest The object conforming to updateStreamConfig request json.
     * @param securityContext     The security for API access.
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void updateStream(final String scopeName, final String streamName,
            final UpdateStreamRequest updateStreamRequest, final SecurityContext securityContext,
            final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "updateStreamConfig");

        StreamConfiguration streamConfiguration = ModelHelper.getUpdateStreamConfig(
                updateStreamRequest, scopeName, streamName);
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
}
