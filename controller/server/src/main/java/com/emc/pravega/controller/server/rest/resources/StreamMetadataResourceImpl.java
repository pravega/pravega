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
import com.emc.pravega.controller.server.rest.generated.model.ScopeProperty;
import com.emc.pravega.controller.server.rest.generated.model.ScopesList;
import com.emc.pravega.controller.server.rest.generated.model.StreamsList;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import com.emc.pravega.controller.server.rest.v1.ApiV1;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.store.stream.StoreException;
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
                log.info("Successfully created new scope: {}", createScopeRequest.getScopeName());
                return Response.status(Status.CREATED).
                        entity(new ScopeProperty().scopeName(createScopeRequest.getScopeName())).build();
            } else if (scopeStatus == CreateScopeStatus.SCOPE_EXISTS) {
                log.warn("Scope name: {} already exists", createScopeRequest.getScopeName());
                return Response.status(Status.CONFLICT).build();
            } else {
                log.warn("Failed to create scope: {}", createScopeRequest.getScopeName());
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn("createScope for scope: {} failed, exception: {}", createScopeRequest.getScopeName(), exception);
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume)
        .thenAccept(x -> LoggerHelpers.traceLeave(log, "createScope", traceId));
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
                        log.info("Successfully created stream: {}/{}", scopeName, streamConfiguration.getStreamName());
                        return Response.status(Status.CREATED).
                                entity(ModelHelper.encodeStreamResponse(streamConfiguration)).build();
                    } else if (streamStatus == CreateStreamStatus.STREAM_EXISTS) {
                        log.warn("Stream already exists: {}/{}", scopeName, streamConfiguration.getStreamName());
                        return Response.status(Status.CONFLICT).build();
                    } else if (streamStatus == CreateStreamStatus.SCOPE_NOT_FOUND) {
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.warn("createStream failed for : {}/{}", scopeName, streamConfiguration.getStreamName());
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).exceptionally(exception -> {
                    log.warn("createStream for {}/{} failed {}: ", scopeName, streamConfiguration.getStreamName(),
                             exception);
                    return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, "createStream", traceId));
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
                log.info("Successfully deleted scope: {}", scopeName);
                return Response.status(Status.NO_CONTENT).build();
            } else if (scopeStatus == DeleteScopeStatus.SCOPE_NOT_FOUND) {
                log.warn("Scope: {} not found", scopeName);
                return Response.status(Status.NOT_FOUND).build();
            } else if (scopeStatus == DeleteScopeStatus.SCOPE_NOT_EMPTY) {
                log.warn("Cannot delete scope: {} with non-empty streams", scopeName);
                return Response.status(Status.PRECONDITION_FAILED).build();
            } else {
                log.warn("deleteScope for {} failed", scopeName);
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn("deleteScope for {} failed with exception: {}", scopeName, exception);
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume)
        .thenAccept(x -> LoggerHelpers.traceLeave(log, "deleteScope", traceId));
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
     * Implementation of getScope REST API.
     *
     * @param scopeName Scope Name.
     * @param securityContext The security for API access.
     * @param asyncResponse AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void getScope(final String scopeName, final SecurityContext securityContext,
                         final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "getScope");

        controllerService.getScope(scopeName)
                .thenApply(scope -> {
                        return Response.status(Status.OK).entity(new ScopeProperty().scopeName(scope)).build();
                })
                .exceptionally( exception -> {
                    if (exception.getCause() instanceof StoreException.NodeNotFoundException) {
                        log.warn("Scope: {} not found", scopeName);
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.warn("getScope for {} failed with exception: {}", scopeName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, "getScope", traceId));
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
                        log.warn("Stream: {}/{} not found", scopeName, streamName);
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.warn("getStream for {}/{} failed with exception: {}", scopeName, streamName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).thenApply(asyncResponse::resume)
                .thenAccept(x ->  LoggerHelpers.traceLeave(log, "getStream", traceId));
    }

    /**
     * Implementation of listScopes REST API.
     *
     * @param securityContext     The security for API access.
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void listScopes(final SecurityContext securityContext, final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "listScopes");

        controllerService.listScopes()
                .thenApply(scopesList -> {
                    ScopesList scopes = new ScopesList();
                    scopesList.forEach(scope -> scopes.addScopesItem(new ScopeProperty().scopeName(scope)));
                    return Response.status(Status.OK).entity(scopes).build();
                }).exceptionally(exception -> {
                        log.warn("listScopes failed with exception: " + exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                }).thenApply(asyncResponse::resume)
                .thenAccept(x ->  LoggerHelpers.traceLeave(log, "listScopes", traceId));
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
        long traceId = LoggerHelpers.traceEnter(log, "listStreams");

        controllerService.listStreamsInScope(scopeName)
                .thenApply(streamsList -> {
                    StreamsList streams = new StreamsList();
                    streamsList.forEach(stream -> streams.addStreamsItem(ModelHelper.encodeStreamResponse(stream)));
                    log.info("Successfully fetched streams for scope: {}", scopeName);
                    return Response.status(Status.OK).entity(streams).build();
                }).exceptionally(exception -> {
                    if (exception.getCause() instanceof DataNotFoundException
                            || exception instanceof DataNotFoundException
                            || exception.getCause() instanceof StoreException.NodeNotFoundException
                            || exception instanceof StoreException.NodeNotFoundException) {
                        log.warn("Scope name: {} not found", scopeName);
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.warn("listStreams for {} failed with exception: {}", scopeName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, "listStreams", traceId));
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
                log.info("Successfully updated stream config for: {}/{}", scopeName, streamName);
                return Response.status(Status.OK)
                         .entity(ModelHelper.encodeStreamResponse(streamConfiguration)).build();
            } else if (streamStatus == UpdateStreamStatus.STREAM_NOT_FOUND ||
                    streamStatus == UpdateStreamStatus.SCOPE_NOT_FOUND) {
                log.warn("Stream: {}/{} not found", scopeName, streamName);
                return Response.status(Status.NOT_FOUND).build();
            } else {
                log.warn("updateStream failed for {}/{}", scopeName, streamName);
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn("updateStream for {}/{} failed with exception: {}", scopeName, streamName, exception);
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume)
        .thenAccept(x -> LoggerHelpers.traceLeave(log, "updateStream", traceId));
    }
}
