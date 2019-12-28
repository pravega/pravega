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

import io.pravega.auth.AuthException;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.common.LoggerHelpers;
import io.pravega.controller.server.AuthResourceRepresentation;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.controller.server.rest.ModelHelper;
import io.pravega.controller.server.rest.generated.model.*;
import io.pravega.controller.server.rest.v1.ApiV1;
import io.pravega.controller.server.rpc.auth.AuthHandlerManager;
import io.pravega.controller.server.rpc.auth.RESTAuthHelper;
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
public class StreamDataResourceImpl implements ApiV1.EventsApi {

    @Context
    HttpHeaders headers;

    private final ControllerService controllerService;
    private final RESTAuthHelper restAuthHelper;
    private final LocalController localController;
    private final ConnectionFactory connectionFactory;

    public StreamDataResourceImpl(LocalController localController, ControllerService controllerService, AuthHandlerManager pravegaAuthManager, ConnectionFactory connectionFactory) {
        this.localController = localController;
        this.controllerService = controllerService;
        this.restAuthHelper = new RESTAuthHelper(pravegaAuthManager);
        this.connectionFactory = connectionFactory;
    }

    /**
     * Implementation of getScope REST API.
     *
     * @param scopeName Scope Name.
     * @param securityContext The security for API access.
     * @param asyncResponse AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void getEvent(String scopeName, String streamName, Long segmentNumber, final SecurityContext securityContext,
                         final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "getEvent");

        try {
            restAuthHelper.authenticateAuthorize(
                    getAuthorizationHeader(),
                    "event", READ);
        } catch (AuthException e) {
            log.warn("Get event failed due to authentication failure.");
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "getEvent", traceId);
            return;
        }

        controllerService.getEvent("", scopeName, streamName, segmentNumber)
                .thenApply(scope -> {
                    return Response.status(Status.OK).entity(new GetEventResponse().scopeName(scopeName)).build();
                })
                .exceptionally( exception -> {
                    if (exception.getCause() instanceof StoreException.DataNotFoundException) {
                        log.warn("Event not found");
                        return Response.status(Status.NOT_FOUND).build();
                    } else {
                        log.warn("getScope  failed with exception: {}", exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                    }
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, "getEvent", traceId));
    }

    /**
     * Implementation of createEvent REST API.
     *
     * @param createEventRequest  The object conforming to createEvent request json.
     * @param securityContext     The security for API access.
     * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing.
     */
    @Override
    public void createEvent(final CreateEventRequest createEventRequest, final SecurityContext securityContext,
                            final AsyncResponse asyncResponse) {
        long traceId = LoggerHelpers.traceEnter(log, "createEvent");
        try {
            NameUtils.validateUserScopeName(createEventRequest.getScopeName());
            NameUtils.validateUserStreamName(createEventRequest.getStreamName());
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn("Create event failed due to invalid scope name {} or stream name {}", 
                    createEventRequest.getScopeName(), createEventRequest.getStreamName());
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, "createEvent", traceId);
            return;
        }

        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(),
                    "/events", READ_UPDATE);
        } catch (AuthException e) {
            log.warn("Create event for scope {} and stream {} failed due to authentication failure {}.",
                    createEventRequest.getScopeName(), createEventRequest.getStreamName(), e);
            asyncResponse.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, "createEvent", traceId);
            return;
        }

        controllerService.createEvent(
                createEventRequest.getRoutingKey(),
                createEventRequest.getScopeName(),
                createEventRequest.getStreamName(),
                createEventRequest.getMessage()).thenApply(scope -> {
                    return Response.status(Status.CREATED).entity(new CreateEventResponse().scopeName(createEventRequest.getScopeName())).build();
/*
            if (response.getStatus() == CreateEventStatus.Status.SUCCESS) {
                log.info("Successfully created new event: {}", createEventRequest.getScopeName());
                return Response.status(Status.CREATED).
                        entity(CreateEventResponse.newBuilder().build()).build();
            } else if (response.getStatus() == CreateEventStatus.Status.EVENT_EXISTS) {
                log.warn("Event name: {} already exists", createEventRequest.getMessage());
                return Response.status(Status.CONFLICT).build();
            } else {
                log.warn("Failed to create event for scope: {} and stream: {}",
                        createEventRequest.getScopeName(), createEventRequest.getStreamName());
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
*/
        }).exceptionally(exception -> {
            log.warn("createEvent for scope: {} stream: {} failed, exception: {}",
                    createEventRequest.getScopeName(), createEventRequest.getStreamName(), exception);
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, "createEvent", traceId));
    }
    private List<String> getAuthorizationHeader() {
        return headers.getRequestHeader(HttpHeaders.AUTHORIZATION);
    }
}
