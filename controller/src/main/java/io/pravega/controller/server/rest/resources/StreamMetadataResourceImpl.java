/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.server.rest.resources;

import io.pravega.auth.AuthException;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupNotFoundException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
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
import io.pravega.shared.security.auth.AuthorizationResource;
import io.pravega.shared.security.auth.AuthorizationResourceImpl;
import io.pravega.shared.rest.security.AuthHandlerManager;
import io.pravega.shared.rest.security.RESTAuthHelper;
import io.pravega.controller.store.stream.ScaleMetadata;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.shared.NameUtils;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.LoggerFactory;

import static io.pravega.auth.AuthHandler.Permissions.READ;
import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;
import static io.pravega.shared.NameUtils.INTERNAL_NAME_PREFIX;
import static io.pravega.shared.NameUtils.READER_GROUP_STREAM_PREFIX;

/**
 * Stream metadata resource implementation.
 */
public class StreamMetadataResourceImpl implements ApiV1.ScopesApi {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(StreamMetadataResourceImpl.class));

    @Context
    HttpHeaders headers;

    private final ControllerService controllerService;
    private final RESTAuthHelper restAuthHelper;
    private final LocalController localController;
    private final ConnectionFactory connectionFactory;
    private final AuthorizationResource authorizationResource = new AuthorizationResourceImpl();
    private final Random requestIdGenerator = new Random();
    private final ClientConfig clientConfig;

    public StreamMetadataResourceImpl(LocalController localController, ControllerService controllerService,
                                      AuthHandlerManager pravegaAuthManager, ConnectionFactory connectionFactory, ClientConfig clientConfig) {
        this.localController = localController;
        this.controllerService = controllerService;
        this.restAuthHelper = new RESTAuthHelper(pravegaAuthManager);
        this.connectionFactory = connectionFactory;
        this.clientConfig = clientConfig;
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
        long requestId = requestIdGenerator.nextLong();

        long traceId = LoggerHelpers.traceEnter(log, "createScope");
        try {
            NameUtils.validateUserScopeName(createScopeRequest.getScopeName());
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn(requestId, "Create scope failed due to invalid scope name {}", createScopeRequest.getScopeName());
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, "createScope", traceId);
            return;
        }

        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(),
                    authorizationResource.ofScopes(), READ_UPDATE);
        } catch (AuthException e) {
            log.warn(requestId, "Create scope for {} failed due to authentication failure {}.",
                    createScopeRequest.getScopeName(), e);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "createScope", traceId);
            return;
        }

        controllerService.createScope(createScopeRequest.getScopeName(), requestId).thenApply(scopeStatus -> {
            if (scopeStatus.getStatus() == CreateScopeStatus.Status.SUCCESS) {
                log.info(requestId, "Successfully created new scope: {}", createScopeRequest.getScopeName());
                return Response.status(Status.CREATED).
                        entity(new ScopeProperty().scopeName(createScopeRequest.getScopeName())).build();
            } else if (scopeStatus.getStatus() == CreateScopeStatus.Status.SCOPE_EXISTS) {
                log.warn(requestId, "Scope name: {} already exists", createScopeRequest.getScopeName());
                return Response.status(Status.CONFLICT).build();
            } else {
                log.warn(requestId, "Failed to create scope: {}", createScopeRequest.getScopeName());
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn(requestId, "createScope for scope: {} failed, exception: {}", createScopeRequest.getScopeName(),
                    exception);
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume)
        .thenAccept(x -> LoggerHelpers.traceLeave(log, "createScope", traceId));
    }


    /**
     * This is a shortcut for {@code headers.getRequestHeader().get(HttpHeaders.AUTHORIZATION)}.
     *
     * @return a list of read-only values of the HTTP Authorization header
     * @throws IllegalStateException if called outside the scope of the HTTP request
     */
    private List<String> getAuthorizationHeader() {
        return headers.getRequestHeader(HttpHeaders.AUTHORIZATION);
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
        long requestId =  requestIdGenerator.nextLong();

        String streamName = createStreamRequest.getStreamName();
        try {
            NameUtils.validateUserStreamName(streamName);
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn(requestId, "Create stream failed due to invalid stream name {}", streamName);
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, "createStream", traceId);
            return;
        }

        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(),
                    authorizationResource.ofStreamsInScope(scopeName), READ_UPDATE);
        } catch (AuthException e) {
            log.warn(requestId, "Create stream for {} failed due to authentication failure.", streamName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "createStream", traceId);
            return;
        }

        StreamConfiguration streamConfiguration = ModelHelper.getCreateStreamConfig(createStreamRequest);
        controllerService.createStream(scopeName, streamName, streamConfiguration, System.currentTimeMillis(), requestId)
                .thenApply(streamStatus -> {
                    Response resp = null;
                    if (streamStatus.getStatus() == CreateStreamStatus.Status.SUCCESS) {
                        log.info(requestId, "Successfully created stream: {}/{}", scopeName, streamName);
                        resp = Response.status(Status.CREATED).
                                entity(ModelHelper.encodeStreamResponse(scopeName, streamName, streamConfiguration)).build();
                    } else if (streamStatus.getStatus() == CreateStreamStatus.Status.STREAM_EXISTS) {
                        log.warn(requestId, "Stream already exists: {}/{}", scopeName, streamName);
                        resp = Response.status(Status.CONFLICT).build();
                    } else if (streamStatus.getStatus() == CreateStreamStatus.Status.SCOPE_NOT_FOUND) {
                        log.warn(requestId, "Scope not found: {}", scopeName);
                        resp = Response.status(Status.NOT_FOUND).build();
                    } else if (streamStatus.getStatus() == CreateStreamStatus.Status.INVALID_STREAM_NAME) {
                        log.warn(requestId, "Invalid stream name: {}", streamName);
                        resp = Response.status(Status.BAD_REQUEST).build();
                    } else {
                        log.warn(requestId, "createStream failed for : {}/{}", scopeName, streamName);
                        resp = Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                    return resp;
                }).exceptionally(exception -> {
                             log.warn(requestId, "createStream for {}/{} failed: ", scopeName, streamName, exception);
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
        long requestId =  requestIdGenerator.nextLong();

        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(),
                    authorizationResource.ofScopes(), READ_UPDATE);
        } catch (AuthException e) {
            log.warn(requestId, "Delete scope for {} failed due to authentication failure.", scopeName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "createStream", traceId);
            return;
        }

        controllerService.deleteScope(scopeName, requestId).thenApply(scopeStatus -> {
            if (scopeStatus.getStatus() == DeleteScopeStatus.Status.SUCCESS) {
                log.info(requestId, "Successfully deleted scope: {}", scopeName);
                return Response.status(Status.NO_CONTENT).build();
            } else if (scopeStatus.getStatus() == DeleteScopeStatus.Status.SCOPE_NOT_FOUND) {
                log.warn(requestId, "Scope: {} not found", scopeName);
                return Response.status(Status.NOT_FOUND).build();
            } else if (scopeStatus.getStatus() == DeleteScopeStatus.Status.SCOPE_NOT_EMPTY) {
                log.warn(requestId, "Cannot delete scope: {} with non-empty streams", scopeName);
                return Response.status(Status.PRECONDITION_FAILED).build();
            } else {
                log.warn(requestId, "deleteScope for {} failed", scopeName);
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn(requestId, "deleteScope for {} failed with exception: {}", scopeName, exception);
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
        long requestId =  requestIdGenerator.nextLong();

        try {
            restAuthHelper.authenticateAuthorize(
                    getAuthorizationHeader(),
                    authorizationResource.ofStreamInScope(scopeName, streamName),
                    READ_UPDATE);
        } catch (AuthException e) {
            log.warn(requestId, "Delete stream for {} failed due to authentication failure.", streamName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "deleteStream", traceId);
            return;
        }

        controllerService.deleteStream(scopeName, streamName, requestId).thenApply(deleteStreamStatus -> {
          if (deleteStreamStatus.getStatus() == DeleteStreamStatus.Status.SUCCESS) {
              log.info(requestId, "Successfully deleted stream: {}", streamName);
              return Response.status(Status.NO_CONTENT).build();
          } else if (deleteStreamStatus.getStatus() == DeleteStreamStatus.Status.STREAM_NOT_FOUND) {
              log.warn(requestId, "Scope: {}, Stream {} not found", scopeName, streamName);
              return Response.status(Status.NOT_FOUND).build();
          } else if (deleteStreamStatus.getStatus() == DeleteStreamStatus.Status.STREAM_NOT_SEALED) {
              log.warn(requestId, "Cannot delete unsealed stream: {}", streamName);
              return Response.status(Status.PRECONDITION_FAILED).build();
          } else {
              log.warn(requestId, "deleteStream for {} failed", streamName);
              return Response.status(Status.INTERNAL_SERVER_ERROR).build();
          }
        }).exceptionally(exception -> {
           log.warn(requestId, "deleteStream for {} failed with exception: {}", streamName, exception);
           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume)
        .thenAccept(x -> LoggerHelpers.traceLeave(log, "deleteStream", traceId));
    }

    @Override
    public void deleteReaderGroup(final String scopeName, final String readerGroupName,
                                  final SecurityContext securityContext, final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "deleteReaderGroup");
        long requestId =  requestIdGenerator.nextLong();

        try {
            restAuthHelper.authenticateAuthorize(
                    getAuthorizationHeader(),
                    authorizationResource.ofReaderGroupInScope(scopeName, readerGroupName),
                    READ_UPDATE);
        } catch (AuthException e) {
            log.warn(requestId, "delete reader group for {} failed due to authentication failure.",
                    scopeName + "/" + readerGroupName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "deleteReaderGroup", traceId);
            return;
        }
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scopeName, this.localController, this.clientConfig);
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scopeName, this.localController, clientFactory);
        CompletableFuture.supplyAsync(() -> {
            readerGroupManager.deleteReaderGroup(readerGroupName);
            return Response.status(Status.NO_CONTENT).build();
        }, controllerService.getExecutor()).exceptionally(exception -> {
            log.warn(requestId, "deleteReaderGroup for {} failed with exception: ", readerGroupName, exception);
            if (exception.getCause() instanceof ReaderGroupNotFoundException ||
                    exception.getCause().getCause() instanceof ReaderGroupNotFoundException) {
                return Response.status(Status.NOT_FOUND).build();
            } else {
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).thenAccept(response -> {
            asyncResponse.resume(response);
            readerGroupManager.close();
            clientFactory.close();
            LoggerHelpers.traceLeave(log, "deleteReaderGroup", traceId);
        });
    }

    @Override
    public void getReaderGroup(final String scopeName, final String readerGroupName,
                               final SecurityContext securityContext, final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "getReaderGroup");
        long requestId =  requestIdGenerator.nextLong();

        try {
            restAuthHelper.authenticateAuthorize(
                    getAuthorizationHeader(), authorizationResource.ofReaderGroupInScope(scopeName, readerGroupName), READ);
        } catch (AuthException e) {
            log.warn(requestId, "Get reader group for {} failed due to authentication failure.",
                    scopeName + "/" + readerGroupName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "getReaderGroup", traceId);
            return;
        }

        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scopeName, this.localController, this.clientConfig);
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scopeName, this.localController, clientFactory);
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
                log.warn(requestId, "getReaderGroup for {} failed with exception: ", readerGroupName, exception);
            if (exception.getCause() instanceof ReaderGroupNotFoundException) {
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
        long requestId =  requestIdGenerator.nextLong();

        try {
            restAuthHelper.authenticateAuthorize(
                    getAuthorizationHeader(),
                    authorizationResource.ofScope(scopeName), READ);
        } catch (AuthException e) {
            log.warn(requestId, "Get scope for {} failed due to authentication failure.", scopeName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "getScope", traceId);
            return;
        }

        controllerService.getScope(scopeName, requestId)
                .thenApply(scope -> {
                        return Response.status(Status.OK).entity(new ScopeProperty().scopeName(scope)).build();
                })
                .exceptionally( exception -> {
                    if (exception.getCause() instanceof StoreException.DataNotFoundException) {
                        log.warn(requestId, "Scope: {} not found", scopeName);
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.warn(requestId, "getScope for {} failed with exception: {}", scopeName, exception);
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
        long requestId =  requestIdGenerator.nextLong();

        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(),
                    authorizationResource.ofStreamInScope(scopeName, streamName), READ);
        } catch (AuthException e) {
            log.warn(requestId, "Get stream for {} failed due to authentication failure.",
                    scopeName + "/" + streamName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "getStream", traceId);
            return;
        }

        controllerService.getStream(scopeName, streamName, requestId)
                .thenApply(streamConfig -> Response.status(Status.OK)
                        .entity(ModelHelper.encodeStreamResponse(scopeName, streamName, streamConfig))
                        .build())
                .exceptionally(exception -> {
                    if (exception.getCause() instanceof StoreException.DataNotFoundException
                            || exception instanceof StoreException.DataNotFoundException) {
                        log.warn(requestId, "Stream: {}/{} not found", scopeName, streamName);
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.warn(requestId, "getStream for {}/{} failed with exception: {}",
                                scopeName, streamName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).thenApply(asyncResponse::resume)
                .thenAccept(x ->  LoggerHelpers.traceLeave(log, "getStream", traceId));
    }

    @Override
    public void listReaderGroups(final String scopeName, final SecurityContext securityContext,
                                 final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "listReaderGroups");
        long requestId =  requestIdGenerator.nextLong();

        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(),
                    authorizationResource.ofReaderGroupsInScope(scopeName), READ);
        } catch (AuthException e) {
            log.warn(requestId, "Get reader groups for {} failed due to authentication failure.", scopeName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "listReaderGroups", traceId);
            return;
        }

        // Each reader group is represented by a stream within the mentioned scope.
        controllerService.listStreamsInScope(scopeName, requestId)
                .thenApply(streamsList -> {
                    ReaderGroupsList readerGroups = new ReaderGroupsList();
                    streamsList.forEach((stream, config) -> {
                        if (stream.startsWith(READER_GROUP_STREAM_PREFIX)) {
                            ReaderGroupsListReaderGroups readerGroup = new ReaderGroupsListReaderGroups();
                            readerGroup.setReaderGroupName(stream.substring(
                                    READER_GROUP_STREAM_PREFIX.length()));
                            readerGroups.addReaderGroupsItem(readerGroup);
                        }
                    });
                    log.info(requestId, "Successfully fetched readerGroups for scope: {}", scopeName);
                    return Response.status(Status.OK).entity(readerGroups).build();
                }).exceptionally(exception -> {
                    if (exception.getCause() instanceof StoreException.DataNotFoundException
                            || exception instanceof StoreException.DataNotFoundException) {
                        log.warn(requestId, "Scope name: {} not found", scopeName);
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.warn(requestId, "listReaderGroups for {} failed with exception: ", scopeName, exception);
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
        long requestId =  requestIdGenerator.nextLong();

        final Principal principal;
        final List<String> authHeader = getAuthorizationHeader();

        try {
            principal = restAuthHelper.authenticate(authHeader);
            restAuthHelper.authorize(authHeader, authorizationResource.ofScopes(), principal, READ);
        } catch (AuthException e) {
            log.warn(requestId, "Get scopes failed due to authentication failure.", e);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "listScopes", traceId);
            return;
        }

        controllerService.listScopes(requestId)
                         .thenApply(scopesList -> {
                             ScopesList scopes = new ScopesList();
                             scopesList.forEach(scope -> {
                                 try {
                                     if (restAuthHelper.isAuthorized(authHeader,
                                             authorizationResource.ofScope(scope),
                                             principal, READ)) {
                                         scopes.addScopesItem(new ScopeProperty().scopeName(scope));
                                     }
                                 } catch (AuthException e) {
                                     log.warn(requestId, e.getMessage(), e);
                                     // Ignore. This exception occurs under abnormal circumstances and not to determine
                                     // whether the user is authorized. In case it does occur, we assume that the user
                                     // is unauthorized.
                                 }
                             });
                             return Response.status(Status.OK).entity(scopes).build(); })
                         .exceptionally(exception -> {
                             log.warn(requestId, "listScopes failed with exception: ", exception);
                             return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                         .thenApply(response -> {
                             asyncResponse.resume(response);
                             LoggerHelpers.traceLeave(log, "listScopes", traceId);
                             return response;
                         });
    }

    /**
     * Implementation of listStreams REST API.
     *
     * @param scopeName           The scope name of stream.
     * @param securityContext     The security for API access.
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void listStreams(final String scopeName, final String filterType, final String filterValue,
                            final SecurityContext securityContext, final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "listStreams");
        long requestId =  requestIdGenerator.nextLong();

        final Principal principal;
        final List<String> authHeader = getAuthorizationHeader();

        try {
            principal = restAuthHelper.authenticate(authHeader);
            restAuthHelper.authorize(authHeader,
                    authorizationResource.ofStreamsInScope(scopeName), principal, READ);
        } catch (AuthException e) {
            log.warn(requestId, "List streams for {} failed due to authentication failure.", scopeName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "listStreams", traceId);
            return;
        }
        boolean showOnlyInternalStreams = filterType != null && filterType.equals("showInternalStreams");
        boolean showStreamsWithTag = filterType != null && filterType.equals("tag");
        String tag;
        if (showStreamsWithTag && filterValue != null) {
            tag = filterValue;
            List<Stream> streams = new ArrayList<>();
            String finalTag = tag;
            localController.listStreamsForTag(scopeName, tag).collectRemaining(streams::add).thenCompose(v -> {
                List<CompletableFuture<ImmutablePair<Stream, StreamConfiguration>>> streamConfigFutureList = streams.stream().filter(stream -> {
                    boolean isAuthorized = false;
                    try {
                        isAuthorized = restAuthHelper.isAuthorized(authHeader, authorizationResource.ofStreamInScope(scopeName, stream.getStreamName()),
                                principal, READ);
                    } catch (AuthException e) {
                        log.warn(requestId, "List Streams with tag {} for scope {} failed due to authentication failure.",
                                finalTag, scopeName);
                        // Ignore. This exception occurs under abnormal circumstances and not to determine
                        // whether the user is authorized. In case it does occur, we assume that the user
                        // is unauthorized.
                    }
                    return isAuthorized;
                }).map(stream -> localController.getStreamConfiguration(scopeName, stream.getStreamName())
                        .thenApply(config -> new ImmutablePair<>(stream, config)))
                        .collect(Collectors.toList());
                return Futures.allOfWithResults(streamConfigFutureList);
            }).thenApply(streamConfigPairs -> {
                StreamsList responseStreams = new StreamsList();
                responseStreams.setStreams(new ArrayList<>());
                streamConfigPairs.forEach(pair -> responseStreams.addStreamsItem(ModelHelper.encodeStreamResponse(pair.left.getScope(), pair.left.getStreamName(), pair.right)));
                log.info(requestId, "Successfully fetched streams for scope: {} with tag: {}", scopeName, finalTag);
                return Response.status(Status.OK).entity(responseStreams).build();
            }).exceptionally(exception -> {
                if (exception.getCause() instanceof StoreException.DataNotFoundException
                        || exception instanceof StoreException.DataNotFoundException) {
                    log.warn(requestId, "Scope name: {} not found", scopeName);
                    return Response.status(Status.NOT_FOUND).build();
                } else {
                    log.warn(requestId, "listStreams for {} with tag {} failed with exception: {}", scopeName, finalTag, exception);
                    return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                }
            }).thenApply(asyncResponse::resume)
                    .thenAccept(x -> LoggerHelpers.traceLeave(log, "listStreams", traceId));
        } else {
            controllerService.listStreamsInScope(scopeName, requestId)
                    .thenApply(streamsList -> {
                        StreamsList streams = new StreamsList();
                        streams.setStreams(new ArrayList<>());
                        streamsList.forEach((stream, config) -> {

                        try {
                            if (restAuthHelper.isAuthorized(authHeader, authorizationResource.ofStreamInScope(scopeName, stream),
                                    principal, READ)) {
                                // If internal streams are requested select only the ones that have the special stream names
                                // otherwise display the regular user created streams.
                                if (!showOnlyInternalStreams ^ stream.startsWith(INTERNAL_NAME_PREFIX)) {
                                    streams.addStreamsItem(ModelHelper.encodeStreamResponse(scopeName, stream, config));
                                }
                            }
                        } catch (AuthException e) {
                            log.warn(requestId, "Read internal streams for scope {} failed due to authentication failure.",
                                    scopeName);
                            // Ignore. This exception occurs under abnormal circumstances and not to determine
                            // whether the user is authorized. In case it does occur, we assume that the user
                            // is unauthorized.
                        }
                    });
                    log.info(requestId, "Successfully fetched streams for scope: {}", scopeName);
                    return Response.status(Status.OK).entity(streams).build();
                }).exceptionally(exception -> {
                    if (exception.getCause() instanceof StoreException.DataNotFoundException
                            || exception instanceof StoreException.DataNotFoundException) {
                        log.warn(requestId, "Scope name: {} not found", scopeName);
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.warn(requestId, "listStreams for {} failed with exception: {}", scopeName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, "listStreams", traceId));
        }
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
        long requestId =  requestIdGenerator.nextLong();

        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(),
                    authorizationResource.ofStreamInScope(scopeName, streamName),
                    READ_UPDATE);
        } catch (AuthException e) {
            log.warn(requestId, "Update stream for {} failed due to authentication failure.",
                    scopeName + "/" + streamName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "Update stream", traceId);
            return;
        }

        StreamConfiguration streamConfiguration = ModelHelper.getUpdateStreamConfig(
                updateStreamRequest);
        controllerService.updateStream(scopeName, streamName, streamConfiguration, requestId).thenApply(streamStatus -> {
            if (streamStatus.getStatus() == UpdateStreamStatus.Status.SUCCESS) {
                log.info(requestId, "Successfully updated stream config for: {}/{}", scopeName, streamName);
                return Response.status(Status.OK)
                         .entity(ModelHelper.encodeStreamResponse(scopeName, streamName, streamConfiguration)).build();
            } else if (streamStatus.getStatus() == UpdateStreamStatus.Status.STREAM_NOT_FOUND ||
                    streamStatus.getStatus() == UpdateStreamStatus.Status.SCOPE_NOT_FOUND) {
                log.warn(requestId, "Stream: {}/{} not found", scopeName, streamName);
                return Response.status(Status.NOT_FOUND).build();
            } else {
                log.warn(requestId, "updateStream failed for {}/{}", scopeName, streamName);
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn(requestId, "updateStream for {}/{} failed with exception: {}", scopeName, streamName, exception);
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
        long requestId =  requestIdGenerator.nextLong();

        try {
            restAuthHelper.authenticateAuthorize(
                    getAuthorizationHeader(),
                    authorizationResource.ofStreamInScope(scopeName, streamName), READ_UPDATE);
        } catch (AuthException e) {
            log.warn(requestId, "Update stream for {} failed due to authentication failure.",
                    scopeName + "/" + streamName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "Update stream", traceId);
            return;
        }

        // We only support sealed state now.
        if (updateStreamStateRequest.getStreamState() != StreamState.StreamStateEnum.SEALED) {
            log.warn(requestId, "Received invalid stream state: {} from client for stream {}/{}",
                     updateStreamStateRequest.getStreamState(), scopeName, streamName);
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            return;
        }

        controllerService.sealStream(scopeName, streamName, requestId).thenApply(updateStreamStatus  -> {
            if (updateStreamStatus.getStatus() == UpdateStreamStatus.Status.SUCCESS) {
                log.info(requestId, "Successfully sealed stream: {}", streamName);
                return Response.status(Status.OK).entity(updateStreamStateRequest).build();
            } else if (updateStreamStatus.getStatus() == UpdateStreamStatus.Status.SCOPE_NOT_FOUND ||
                    updateStreamStatus.getStatus() == UpdateStreamStatus.Status.STREAM_NOT_FOUND) {
                log.warn(requestId, "Scope: {} or Stream {} not found", scopeName, streamName);
                return Response.status(Status.NOT_FOUND).build();
            } else {
                log.warn(requestId, "updateStreamState for {} failed", streamName);
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn(requestId, "updateStreamState for {} failed with exception: {}", streamName, exception);
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
        long requestId =  requestIdGenerator.nextLong();

        if (from == null || to == null) {
            // Validate the input since there is no mechanism in JAX-RS to validate query params.
            log.warn(requestId, "Received an invalid request with missing query parameters for scopeName/streamName: {}/{}", scopeName, streamName);
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, "getScalingEvents", traceId);
            return;
        }

        try {
            restAuthHelper.authenticateAuthorize(
                    getAuthorizationHeader(),
                    authorizationResource.ofStreamInScope(scopeName, streamName), READ);
        } catch (AuthException e) {
            log.warn(requestId, "Get scaling events for {} failed due to authentication failure.",
                    scopeName + "/" + streamName);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "Get scaling events", traceId);
            return;
        }

        if (from < 0 || to < 0 || from > to) {
            log.warn(requestId, "Received invalid request from client for scopeName/streamName: {}/{} ",
                    scopeName, streamName);
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, "getScalingEvents", traceId);
            return;
        }

        controllerService.getScaleRecords(scopeName, streamName, from, to, requestId).thenApply(listScaleMetadata -> {
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
            log.info(requestId, "Successfully fetched required scaling events for scope: {}, stream: {}",
                    scopeName, streamName);
            return Response.status(Status.OK).entity(finalScaleMetadataList).build();
        }).exceptionally(exception -> {
            if (exception.getCause() instanceof StoreException.DataNotFoundException
                    || exception instanceof StoreException.DataNotFoundException) {
                log.warn(requestId, "Stream/Scope name: {}/{} not found", scopeName, streamName);
                return Response.status(Status.NOT_FOUND).build();
            } else {
                log.warn(requestId, "getScalingEvents for scopeName/streamName: {}/{} failed with exception ",
                        scopeName, streamName, exception);
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, "getScalingEvents", traceId));
    }
}
