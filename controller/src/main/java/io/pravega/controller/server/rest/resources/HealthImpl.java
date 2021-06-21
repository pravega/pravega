/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rest.resources;

import io.pravega.auth.AuthException;
import io.pravega.controller.server.rest.generated.api.NotFoundException;
import io.pravega.controller.server.rest.generated.model.HealthDependencies;
import io.pravega.controller.server.rest.generated.model.HealthDetails;
import io.pravega.controller.server.rest.generated.model.HealthResult;
import io.pravega.controller.server.rest.generated.model.HealthStatus;
import io.pravega.controller.server.rest.v1.ApiV1;
import io.pravega.controller.server.security.auth.RESTAuthHelper;
import io.pravega.controller.server.security.auth.handler.AuthHandlerManager;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthService;
import io.pravega.shared.health.Status;
import io.pravega.shared.security.auth.AuthorizationResource;
import io.pravega.shared.security.auth.AuthorizationResourceImpl;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import io.pravega.common.LoggerHelpers;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;

@Slf4j
public class HealthImpl implements ApiV1.HealthApi {

    @Context
    HttpHeaders headers;

    private final HealthService service;

    private final RESTAuthHelper restAuthHelper;
    private final AuthorizationResource authorizationResource = new AuthorizationResourceImpl();

    public HealthImpl(AuthHandlerManager pravegaAuthManager, HealthService service) {
        this.service = service;
        this.restAuthHelper = new RESTAuthHelper(pravegaAuthManager);
    }

    @Override
    public void getContributorHealth(String id, Boolean details, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getHealth(id, securityContext, asyncResponse, "getContributorHealth");
    }

    @Override
    public void getHealth(Boolean details, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getHealth(null, securityContext, asyncResponse, "getHealth");
    }

    private void getHealth(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(), authorizationResource.ofScopes(), READ_UPDATE);
            Health health = id == null ? service.getEndpoint().getHealth() : service.getEndpoint().getHealth(id);
            Response response = Response.status(Response.Status.OK)
                    .entity(adapter(health))
                    .build();
            asyncResponse.resume(response);
        } catch (AuthException e) {
            log.warn("Unable to retrieve Health state '{}' due to authentication failure.", id);
            asyncResponse.resume(Response.status(Response.Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, method, traceId);
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
    }

    @Override
    public void getContributorLiveness(String id, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getLiveness(id, securityContext, asyncResponse, "getContributorLiveness");
    }

    @Override
    public void getLiveness(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getLiveness(null, securityContext, asyncResponse, "getLiveness");
    }

    private void getLiveness(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(), authorizationResource.ofScopes(), READ_UPDATE);
            boolean alive = id == null ? service.getEndpoint().isAlive() : service.getEndpoint().isAlive(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(alive)
                    .build());
        } catch (AuthException e) {
            log.warn("Unable to retrieve Liveness state for '{}' due to authentication failure.", id);
            asyncResponse.resume(Response.status(Response.Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, method, traceId);
        } catch (RuntimeException e) {
            asyncResponse.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
    }

    @Override
    public void getDetails(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getDetails(null, securityContext, asyncResponse, "getDetails");
    }

    private void getDetails(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(), authorizationResource.ofScopes(), READ_UPDATE);
            Map<String, Object> details = id == null ? service.getEndpoint().getDetails() : service.getEndpoint().getDetails(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(adapter(details))
                    .build());
        } catch (AuthException e) {
            log.warn("Unable to retrieve Details for '{}' due to authentication failure.", id);
            asyncResponse.resume(Response.status(Response.Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, method, traceId);
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
    }

    @Override
    public void getContributorReadiness(String id, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getReadiness(id, securityContext, asyncResponse, "getContributorReadiness");
    }

    @Override
    public void getReadiness(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getReadiness(null, securityContext, asyncResponse, "getReadiness");
    }

    private void getReadiness(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(), authorizationResource.ofScopes(), READ_UPDATE);
            boolean ready = id == null ? service.getEndpoint().isReady() : service.getEndpoint().isReady(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(ready)
                    .build());
        } catch (AuthException e) {
            log.warn("Unable to retrieve Readiness state for '{}' due to authentication failure.", id);
            asyncResponse.resume(Response.status(Response.Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, method, traceId);
        } catch (RuntimeException e) {
            asyncResponse.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
    }

    @Override
    public void getContributorStatus(String id, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getStatus(id, securityContext, asyncResponse, "getContributorStatus");
    }

    @Override
    public void getStatus(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getStatus(null, securityContext, asyncResponse, "getStatus");
    }

    private void getStatus(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        try {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(), authorizationResource.ofScopes(), READ_UPDATE);
            Status status = id == null ? service.getEndpoint().getStatus() : service.getEndpoint().getStatus(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(adapter(status))
                    .build());
        } catch (AuthException e) {
            log.warn("Unable to retrieve Status for '{}' due to authentication failure.", id);
            asyncResponse.resume(Response.status(Response.Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, method, traceId);
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
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

    // The follow methods provide a means to cast the HealthService framework models, to the generated models.
    private static HealthResult adapter(Health health) {
        return new HealthResult()
                .name(health.getName())
                .status(adapter(health.getStatus()))
                .liveness(health.isAlive())
                .readiness(health.isReady())
                .details(adapter(health.getDetails()))
                .children(health.getChildren().entrySet().stream()
                        .map(entry -> adapter(entry.getValue()))
                        .collect(Collectors.toList()));
    }

    private static HealthDetails adapter(Map<String, Object> details) {
        HealthDetails result = new HealthDetails();
        details.forEach((key, val) -> {
            result.put(key, val.toString());
        });
        return result;
    }

    private static HealthStatus adapter(Status status) {
        return HealthStatus.fromValue(status.name());
    }

    private static HealthDependencies adapter(List<String> dependencies) {
        HealthDependencies result = new HealthDependencies();
        result.addAll(dependencies);
        return result;
    }

}