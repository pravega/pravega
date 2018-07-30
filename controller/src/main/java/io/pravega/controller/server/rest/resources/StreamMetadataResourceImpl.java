/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rest.resources;

import io.pravega.auth.AuthHandler;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.auth.AuthException;
import io.pravega.common.auth.AuthenticationException;
import io.pravega.common.auth.AuthorizationException;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.controller.server.rest.ModelHelper;
import io.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.ReaderGroupProperty;
import io.pravega.controller.server.rest.generated.model.ReaderGroupsList;
import io.pravega.controller.server.rest.generated.model.ReaderGroupsListReaderGroups;
import io.pravega.controller.server.rest.generated.model.ScopeProperty;
import io.pravega.controller.server.rest.generated.model.ScopesList;
import io.pravega.controller.server.rest.generated.model.StreamState;
import io.pravega.controller.server.rest.generated.model.StreamsList;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.controller.server.rest.v1.ApiV1;
import io.pravega.controller.server.rpc.auth.PravegaAuthManager;
import io.pravega.controller.store.stream.ScaleMetadata;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.shared.NameUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.auth.AuthHandler.Permissions.READ;
import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;
import static io.pravega.shared.NameUtils.INTERNAL_NAME_PREFIX;
import static io.pravega.shared.NameUtils.READER_GROUP_STREAM_PREFIX;

/**
 * Stream metadata resource implementation.
 */
@Slf4j
public class StreamMetadataResourceImpl implements ApiV1.ScopesApi {

    @Context
    HttpHeaders headers;

    private final ControllerService controllerService;
    private final PravegaAuthManager pravegaAuthManager;
    private final LocalController localController;
    private final ConnectionFactory connectionFactory;

    public StreamMetadataResourceImpl(LocalController localController, ControllerService controllerService, PravegaAuthManager pravegaAuthManager, ConnectionFactory connectionFactory) {
        this.localController = localController;
        this.controllerService = controllerService;
        this.pravegaAuthManager = pravegaAuthManager;
        this.connectionFactory = connectionFactory;
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

        try {
            authenticate(createScopeRequest.getScopeName(), READ_UPDATE);
        } catch (AuthException e) {
            log.warn("Create scope for {} failed due to authentication failure {}.", createScopeRequest.getScopeName(), e);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
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

    private void authenticate(String resourceName, AuthHandler.Permissions level) throws AuthException {
        if (pravegaAuthManager != null ) {
            List<String> authParams = headers.getRequestHeader(HttpHeaders.AUTHORIZATION);

            if (authParams == null || authParams.isEmpty()) {
                throw new AuthenticationException("Auth failed for " + resourceName);
            }

            String credentials = authParams.get(0);
            assert credentials != null;

            if (!pravegaAuthManager.authenticate(resourceName, credentials, level)) {
                throw new AuthorizationException("Auth failed for " + resourceName, 403);
            }
        }
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

        try {
            authenticate(scopeName + "/" + createStreamRequest.getStreamName(), READ_UPDATE);
        } catch (AuthException e) {
            log.warn("Create stream for {} failed due to authentication failure.", createStreamRequest.getStreamName());
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
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

        try {
            authenticate(scopeName, READ_UPDATE);
        } catch (AuthException e) {
            log.warn("Delete scope for {} failed due to authentication failure.", scopeName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "createStream", traceId);
            return;
        }

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

        try {
            authenticate(scopeName + "/" + streamName, READ_UPDATE);
        } catch (AuthException e) {
            log.warn("Delete stream for {} failed due to authentication failure.", streamName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "deleteStream", traceId);
            return;
        }

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

    @Override
    public void getReaderGroup(final String scopeName, final String readerGroupName,
                               final SecurityContext securityContext, final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "getReaderGroup");

        try {
            authenticate(scopeName + "/" + readerGroupName, READ);
        } catch (AuthException e) {
            log.warn("Get reader group for {} failed due to authentication failure.", scopeName + "/" + readerGroupName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "getReaderGroup", traceId);
            return;
        }

        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scopeName, this.localController);
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scopeName, this.localController,
                clientFactory, this.connectionFactory);
        ReaderGroupProperty readerGroupProperty = new ReaderGroupProperty();
        readerGroupProperty.setScopeName(scopeName);
        readerGroupProperty.setReaderGroupName(readerGroupName);
        CompletableFuture.supplyAsync(() -> {
            ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
            readerGroupProperty.setOnlineReaderIds(
                    new ArrayList<>(readerGroup.getOnlineReaders()));
            readerGroupProperty.setStreamList(
                    new ArrayList<>(readerGroup.getStreamNames()));
            return Response.status(Status.OK).entity(readerGroupProperty).build();
        }, controllerService.getExecutor()).exceptionally(exception -> {
            log.warn("getReaderGroup for {} failed with exception: ", readerGroupName, exception);
            if (exception.getCause() instanceof InvalidStreamException) {
                return Response.status(Status.NOT_FOUND).build();
            } else {
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).thenAccept(response -> {
            asyncResponse.resume(response);
            readerGroupManager.close();
            clientFactory.close();
            LoggerHelpers.traceLeave(log, "getReaderGroup", traceId);
        });
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

        try {
            authenticate(scopeName, READ);
        } catch (AuthException e) {
            log.warn("Get scope for {} failed due to authentication failure.", scopeName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "getScope", traceId);
            return;
        }

        controllerService.getScope(scopeName)
                .thenApply(scope -> {
                        return Response.status(Status.OK).entity(new ScopeProperty().scopeName(scope)).build();
                })
                .exceptionally( exception -> {
                    if (exception.getCause() instanceof StoreException.DataNotFoundException) {
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

        try {
            authenticate(scopeName + "/" + streamName, READ);
        } catch (AuthException e) {
            log.warn("Get stream for {} failed due to authentication failure.", scopeName + "/" + streamName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "getStream", traceId);
            return;
        }

        controllerService.getStream(scopeName, streamName)
                .thenApply(streamConfig -> Response.status(Status.OK)
                        .entity(ModelHelper.encodeStreamResponse(streamConfig))
                        .build())
                .exceptionally(exception -> {
                    if (exception.getCause() instanceof StoreException.DataNotFoundException
                            || exception instanceof StoreException.DataNotFoundException) {
                        log.warn("Stream: {}/{} not found", scopeName, streamName);
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.warn("getStream for {}/{} failed with exception: {}", scopeName, streamName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).thenApply(asyncResponse::resume)
                .thenAccept(x ->  LoggerHelpers.traceLeave(log, "getStream", traceId));
    }

    @Override
    public void listReaderGroups(final String scopeName, final SecurityContext securityContext,
                                 final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "listReaderGroups");

        try {
            authenticate(scopeName, READ);
        } catch (AuthException e) {
            log.warn("Get reader groups for {} failed due to authentication failure.", scopeName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "listReaderGroups", traceId);
            return;
        }

        // Each reader group is represented by a stream within the mentioned scope.
        controllerService.listStreamsInScope(scopeName)
                .thenApply(streamsList -> {
                    ReaderGroupsList readerGroups = new ReaderGroupsList();
                    streamsList.forEach(stream -> {
                        if (stream.getStreamName().startsWith(READER_GROUP_STREAM_PREFIX)) {
                            ReaderGroupsListReaderGroups readerGroup = new ReaderGroupsListReaderGroups();
                            readerGroup.setReaderGroupName(stream.getStreamName().substring(
                                    READER_GROUP_STREAM_PREFIX.length()));
                            readerGroups.addReaderGroupsItem(readerGroup);
                        }
                    });
                    log.info("Successfully fetched readerGroups for scope: {}", scopeName);
                    return Response.status(Status.OK).entity(readerGroups).build();
                }).exceptionally(exception -> {
                    if (exception.getCause() instanceof StoreException.DataNotFoundException
                            || exception instanceof StoreException.DataNotFoundException) {
                        log.warn("Scope name: {} not found", scopeName);
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.warn("listReaderGroups for {} failed with exception: ", scopeName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, "listReaderGroups", traceId));
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

        try {
            authenticate("/", READ);
        } catch (AuthException e) {
            log.warn("Get scopes failed due to authentication failure.");
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "listScopes", traceId);
            return;
        }

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

        try {
            authenticate(scopeName, READ);
        } catch (AuthException e) {
            log.warn("List streams for {} failed due to authentication failure.", scopeName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "listStreams", traceId);
            return;
        }
        boolean showOnlyInternalStreams = showInternalStreams != null && showInternalStreams.equals("true");
        controllerService.listStreamsInScope(scopeName)
                .thenApply(streamsList -> {
                    StreamsList streams = new StreamsList();
                    streamsList.forEach(stream -> {
                        // If internal streams are requested select only the ones that have the special stream names
                        // otherwise display the regular user created streams.
                        if (!showOnlyInternalStreams ^ stream.getStreamName().startsWith(INTERNAL_NAME_PREFIX)) {
                            streams.addStreamsItem(ModelHelper.encodeStreamResponse(stream));
                        }
                    });
                    log.info("Successfully fetched streams for scope: {}", scopeName);
                    return Response.status(Status.OK).entity(streams).build();
                }).exceptionally(exception -> {
                    if (exception.getCause() instanceof StoreException.DataNotFoundException
                            || exception instanceof StoreException.DataNotFoundException) {
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

        try {
            authenticate(scopeName + "/" + streamName, READ_UPDATE);
        } catch (AuthException e) {
            log.warn("Update stream for {} failed due to authentication failure.", scopeName + "/" + streamName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "Update stream", traceId);
            return;
        }

        StreamConfiguration streamConfiguration = ModelHelper.getUpdateStreamConfig(
                updateStreamRequest, scopeName, streamName);
        controllerService.updateStream(streamConfiguration).thenApply(streamStatus -> {
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

        try {
            authenticate(scopeName + "/" + streamName, READ_UPDATE);
        } catch (AuthException e) {
            log.warn("Update stream for {} failed due to authentication failure.", scopeName + "/" + streamName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "Update stream", traceId);
            return;
        }

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

    /**
     * Implementation of getScalingEvents REST API.
     *
     * @param scopeName         The scope name of stream.
     * @param streamName        The name of stream.
     * @param from              DateTime from which scaling events should be displayed.
     * @param to                DateTime until which scaling events should be displayed.
     * @param securityContext   The security for API access.
     * @param asyncResponse     AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void getScalingEvents(final String scopeName, final String streamName, final Long from, final Long to,
                                 final SecurityContext securityContext, final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "getScalingEvents");

        try {
            authenticate(scopeName + "/" + streamName, READ);
        } catch (AuthException e) {
            log.warn("Get scaling events for {} failed due to authentication failure.", scopeName + "/" + streamName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "Get scaling events", traceId);
            return;
        }

        if (from < 0 || to < 0 || from > to) {
            log.warn("Received invalid request from client for scopeName/streamName: {}/{} ", scopeName, streamName);
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, "getScalingEvents", traceId);
            return;
        }

        controllerService.getScaleRecords(scopeName, streamName).thenApply(listScaleMetadata -> {
            Iterator<ScaleMetadata> metadataIterator = listScaleMetadata.iterator();
            List<ScaleMetadata> finalScaleMetadataList = new ArrayList<ScaleMetadata>();

            // referenceEvent is the Event used as reference for the events between 'from' and 'to'.
            ScaleMetadata referenceEvent = null;

            while (metadataIterator.hasNext()) {
                ScaleMetadata scaleMetadata = metadataIterator.next();
                if (scaleMetadata.getTimestamp() >= from && scaleMetadata.getTimestamp() <= to) {
                    finalScaleMetadataList.add(scaleMetadata);
                } else if ((scaleMetadata.getTimestamp() < from) &&
                            !(referenceEvent != null && referenceEvent.getTimestamp() > scaleMetadata.getTimestamp())) {
                    // This check is required to store a reference event i.e. an event before the 'from' datetime
                    referenceEvent = scaleMetadata;
                }
            }

            if (referenceEvent != null) {
                finalScaleMetadataList.add(0, referenceEvent);
            }
            log.info("Successfully fetched required scaling events for scope: {}, stream: {}", scopeName, streamName);
            return Response.status(Status.OK).entity(finalScaleMetadataList).build();
        }).exceptionally(exception -> {
            if (exception.getCause() instanceof StoreException.DataNotFoundException
                    || exception instanceof StoreException.DataNotFoundException) {
                log.warn("Stream/Scope name: {}/{} not found", scopeName, streamName);
                return Response.status(Status.NOT_FOUND).build();
            } else {
                log.warn("getScalingEvents for scopeName/streamName: {}/{} failed with exception ",
                        scopeName, streamName, exception);
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, "getScalingEvents", traceId));
    }
}
