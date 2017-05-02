/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.server.rest.resources;

import io.pravega.common.LoggerHelpers;
import io.pravega.server.controller.service.server.ControllerService;
import io.pravega.server.controller.service.server.rest.ModelHelper;
import io.pravega.server.controller.service.server.rest.v1.ApiV1;
import io.pravega.server.controller.service.store.stream.DataNotFoundException;
import io.pravega.server.controller.service.store.stream.StoreException;
import io.pravega.shared.NameUtils;
import io.pravega.server.controller.service.server.rest.generated.model.CreateScopeRequest;
import io.pravega.server.controller.service.server.rest.generated.model.CreateStreamRequest;
import io.pravega.server.controller.service.server.rest.generated.model.ScopeProperty;
import io.pravega.server.controller.service.server.rest.generated.model.ScopesList;
import io.pravega.server.controller.service.server.rest.generated.model.StreamState;
import io.pravega.server.controller.service.server.rest.generated.model.StreamsList;
import io.pravega.server.controller.service.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.client.stream.StreamConfiguration;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import static io.pravega.shared.NameUtils.INTERNAL_NAME_PREFIX;

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
        try {
            NameUtils.validateUserScopeName(createScopeRequest.getScopeName());
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn("Create scope failed due to invalid scope name {}", createScopeRequest.getScopeName());
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, "createScope", traceId);
            return;
        }

        controllerService.createScope(createScopeRequest.getScopeName()).thenApply(scopeStatus -> {
            if (scopeStatus.getStatus() == CreateScopeStatus.Status.SUCCESS) {
                log.info("Successfully created new scope: {}", createScopeRequest.getScopeName());
                return Response.status(Status.CREATED).
                        entity(new ScopeProperty().scopeName(createScopeRequest.getScopeName())).build();
            } else if (scopeStatus.getStatus() == CreateScopeStatus.Status.SCOPE_EXISTS) {
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

        try {
            NameUtils.validateUserStreamName(createStreamRequest.getStreamName());
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn("Create stream failed due to invalid stream name {}", createStreamRequest.getStreamName());
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, "createStream", traceId);
            return;
        }
        StreamConfiguration streamConfiguration = ModelHelper.getCreateStreamConfig(createStreamRequest, scopeName);
        controllerService.createStream(streamConfiguration, System.currentTimeMillis())
                .thenApply(streamStatus -> {
                    Response resp = null;
                    if (streamStatus.getStatus() == CreateStreamStatus.Status.SUCCESS) {
                        log.info("Successfully created stream: {}/{}", scopeName, streamConfiguration.getStreamName());
                        resp = Response.status(Status.CREATED).
                                entity(ModelHelper.encodeStreamResponse(streamConfiguration)).build();
                    } else if (streamStatus.getStatus() == CreateStreamStatus.Status.STREAM_EXISTS) {
                        log.warn("Stream already exists: {}/{}", scopeName, streamConfiguration.getStreamName());
                        resp = Response.status(Status.CONFLICT).build();
                    } else if (streamStatus.getStatus() == CreateStreamStatus.Status.SCOPE_NOT_FOUND) {
                        log.warn("Scope not found: {}", scopeName);
                        resp = Response.status(Status.NOT_FOUND).build();
                    } else if (streamStatus.getStatus() == CreateStreamStatus.Status.INVALID_STREAM_NAME) {
                        log.warn("Invalid stream name: {}", streamConfiguration.getStreamName());
                        resp = Response.status(Status.BAD_REQUEST).build();
                    } else {
                        log.warn("createStream failed for : {}/{}", scopeName, streamConfiguration.getStreamName());
                        resp = Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                    return resp;
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
            if (scopeStatus.getStatus() == DeleteScopeStatus.Status.SUCCESS) {
                log.info("Successfully deleted scope: {}", scopeName);
                return Response.status(Status.NO_CONTENT).build();
            } else if (scopeStatus.getStatus() == DeleteScopeStatus.Status.SCOPE_NOT_FOUND) {
                log.warn("Scope: {} not found", scopeName);
                return Response.status(Status.NOT_FOUND).build();
            } else if (scopeStatus.getStatus() == DeleteScopeStatus.Status.SCOPE_NOT_EMPTY) {
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
        long traceId = LoggerHelpers.traceEnter(log, "deleteStream");

        controllerService.deleteStream(scopeName, streamName).thenApply(deleteStreamStatus -> {
          if (deleteStreamStatus.getStatus() == DeleteStreamStatus.Status.SUCCESS) {
              log.info("Successfully deleted stream: {}", streamName);
              return Response.status(Status.NO_CONTENT).build();
          } else if (deleteStreamStatus.getStatus() == DeleteStreamStatus.Status.STREAM_NOT_FOUND) {
              log.warn("Scope: {}, Stream {} not found", scopeName, streamName);
              return Response.status(Status.NOT_FOUND).build();
          } else if (deleteStreamStatus.getStatus() == DeleteStreamStatus.Status.STREAM_NOT_SEALED) {
              log.warn("Cannot delete unsealed stream: {}", streamName);
              return Response.status(Status.PRECONDITION_FAILED).build();
          } else {
              log.warn("deleteStream for {} failed", streamName);
              return Response.status(Status.INTERNAL_SERVER_ERROR).build();
          }
        }).exceptionally(exception -> {
           log.warn("deleteStream for {} failed with exception: {}", streamName, exception);
           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume)
        .thenAccept(x -> LoggerHelpers.traceLeave(log, "deleteStream", traceId));
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
    public void listStreams(final String scopeName, final String showInternalStreams,
                            final SecurityContext securityContext, final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "listStreams");

        boolean showOnlyInternalStreams = showInternalStreams != null && showInternalStreams.equals("true");
        controllerService.listStreamsInScope(scopeName)
                .thenApply(streamsList -> {
                    StreamsList streams = new StreamsList();
                    streamsList.forEach(stream -> {
                        // If internal streams are requested select only the ones that have the special stream names
                        // otherwise display the regular user created streams.
                        // TODO: Remove the 200 size limit once issue - https://github.com/pravega/pravega/issues/926
                        // is fixed.
                        if ((!showOnlyInternalStreams ^ stream.getStreamName().startsWith(INTERNAL_NAME_PREFIX)) &&
                                streams.getStreams().size() < 200) {
                            streams.addStreamsItem(ModelHelper.encodeStreamResponse(stream));
                        }
                    });
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
            if (streamStatus.getStatus() == UpdateStreamStatus.Status.SUCCESS) {
                log.info("Successfully updated stream config for: {}/{}", scopeName, streamName);
                return Response.status(Status.OK)
                         .entity(ModelHelper.encodeStreamResponse(streamConfiguration)).build();
            } else if (streamStatus.getStatus() == UpdateStreamStatus.Status.STREAM_NOT_FOUND ||
                    streamStatus.getStatus() == UpdateStreamStatus.Status.SCOPE_NOT_FOUND) {
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

    /**
     * Implementation of updateStreamState REST API.
     *
     * @param scopeName                 The scope name of stream.
     * @param streamName                The name of stream.
     * @param updateStreamStateRequest  The object conforming to updateStreamStateRequest request json.
     * @param securityContext           The security for API access.
     * @param asyncResponse             AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void updateStreamState(final String scopeName, final String streamName,
            final StreamState updateStreamStateRequest, SecurityContext securityContext, AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "updateStreamState");

        // We only support sealed state now.
        if (updateStreamStateRequest.getStreamState() != StreamState.StreamStateEnum.SEALED) {
            log.warn("Received invalid stream state: {} from client for stream {}/{}",
                     updateStreamStateRequest.getStreamState(), scopeName, streamName);
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            return;
        }

        controllerService.sealStream(scopeName, streamName).thenApply(updateStreamStatus  -> {
            if (updateStreamStatus.getStatus() == UpdateStreamStatus.Status.SUCCESS) {
                log.info("Successfully sealed stream: {}", streamName);
                return Response.status(Status.OK).entity(updateStreamStateRequest).build();
            } else if (updateStreamStatus.getStatus() == UpdateStreamStatus.Status.SCOPE_NOT_FOUND ||
                    updateStreamStatus.getStatus() == UpdateStreamStatus.Status.STREAM_NOT_FOUND) {
                log.warn("Scope: {} or Stream {} not found", scopeName, streamName);
                return Response.status(Status.NOT_FOUND).build();
            } else {
                log.warn("updateStreamState for {} failed", streamName);
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn("updateStreamState for {} failed with exception: {}", streamName, exception);
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, "updateStreamState", traceId));
    }
}
