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
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.DeleteScopeStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.stream.StreamConfiguration;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

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
        long traceId = LoggerHelpers.traceEnter(log, "createScope");

        controllerService.createScope(createScopeRequest.getScopeName()).thenApply(scopeStatus -> {
            if (scopeStatus == CreateScopeStatus.SUCCESS) {
                return Response.status(Status.CREATED).entity(createScopeRequest).build();
            } else if (scopeStatus == CreateScopeStatus.SCOPE_EXISTS) {
                return Response.status(Status.CONFLICT).build();
            } else {
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn("Exception occurred while executing createScope: " + exception);
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume);

        LoggerHelpers.traceLeave(log, "createScope", traceId);
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
        controllerService.createStream(streamConfiguration, System.currentTimeMillis())
                .thenApply(streamStatus -> {
                    if (streamStatus == CreateStreamStatus.SUCCESS) {
                        return Response.status(Status.CREATED).
                                entity(ModelHelper.encodeStreamResponse(streamConfiguration)).build();
                    } else if (streamStatus == CreateStreamStatus.STREAM_EXISTS) {
                        return Response.status(Status.CONFLICT).build();
                    } else {
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).exceptionally(exception -> {
                    log.warn("Exception occurred while executing createStream: " + exception);
                    return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                }).thenApply(asyncResponse::resume);

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
        long traceId = LoggerHelpers.traceEnter(log, "deleteScope");

        controllerService.deleteScope(scopeName).thenApply(scopeStatus -> {
            if (scopeStatus == DeleteScopeStatus.SUCCESS) {
                return Response.status(Status.NO_CONTENT).build();
            } else if (scopeStatus == DeleteScopeStatus.SCOPE_NOT_FOUND) {
                return Response.status(Status.NOT_FOUND).build();
            } else if (scopeStatus == DeleteScopeStatus.SCOPE_NOT_EMPTY) {
                return Response.status(Status.PRECONDITION_FAILED).build();
            } else {
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn("Exception occurred while executing deleteScope: " + exception);
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume);

        LoggerHelpers.traceLeave(log, "deleteScope", traceId);
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
        long traceId = LoggerHelpers.traceEnter(log, "getStream");

        controllerService.getStream(scopeName, streamName)
                .thenApply(streamConfig -> Response.status(Status.OK)
                        .entity(ModelHelper.encodeStreamResponse(streamConfig))
                        .build())
                .exceptionally(exception -> {
                    if (exception.getCause() instanceof DataNotFoundException
                            || exception instanceof DataNotFoundException) {
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.debug("Exception occurred while executing getStream: " + exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).thenApply(asyncResponse::resume);

        LoggerHelpers.traceLeave(log, "getStream", traceId);
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
        long traceId = LoggerHelpers.traceEnter(log, "updateStream");

        StreamConfiguration streamConfiguration = ModelHelper.getUpdateStreamConfig(
                updateStreamRequest, scopeName, streamName);
        controllerService.alterStream(streamConfiguration).thenApply(streamStatus -> {
            if (streamStatus == UpdateStreamStatus.SUCCESS) {
                 return Response.status(Status.OK)
                         .entity(ModelHelper.encodeStreamResponse(streamConfiguration)).build();
             } else if (streamStatus == UpdateStreamStatus.STREAM_NOT_FOUND) {
                 return Response.status(Status.NOT_FOUND).build();
             } else {
                 return Response.status(Status.INTERNAL_SERVER_ERROR).build();
             }
         }).exceptionally(exception -> {
            log.debug("Exception occurred while executing updateStream: " + exception);
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume);

        LoggerHelpers.traceLeave(log, "updateStream", traceId);
    }
}
