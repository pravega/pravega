/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rest.resources;

import io.pravega.controller.server.rest.generated.api.NotFoundException;
import io.pravega.controller.server.rest.generated.model.HealthDependencies;
import io.pravega.controller.server.rest.generated.model.HealthDetails;
import io.pravega.controller.server.rest.generated.model.HealthResult;
import io.pravega.controller.server.rest.generated.model.HealthStatus;
import io.pravega.controller.server.rest.v1.ApiV1;
import io.pravega.controller.server.security.auth.RESTAuthHelper;
import io.pravega.controller.server.security.auth.handler.AuthHandlerManager;
import io.pravega.shared.health.ContributorNotFoundException;
import io.pravega.shared.health.Details;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthService;
import io.pravega.shared.health.Status;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import io.pravega.common.LoggerHelpers;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class HealthImpl implements ApiV1.HealthApi {

    private final HealthService service;

    private final RESTAuthHelper restAuthHelper;

    public HealthImpl(AuthHandlerManager pravegaAuthManager, HealthService service) {
        this.service = service;
        this.restAuthHelper = new RESTAuthHelper(pravegaAuthManager);
    }

    // Note: If 'Boolean details' is a null value, the request will fail -- circle back to Controller.yaml.
    @Override
    public void getContributorHealth(String id, Boolean details, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getHealth(id, details == null ? Boolean.FALSE : details, securityContext, asyncResponse, "getContributorHealth");
    }

    @Override
    public void getHealth(Boolean details, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getHealth(null, details == null ? Boolean.FALSE : details, securityContext, asyncResponse, "getHealth");
    }

    private void getHealth(String id, Boolean details, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        try {
            Health health = id == null ? service.endpoint().health(details) : service.endpoint().health(id, details);
            Response response = Response.status(Response.Status.OK)
                    .entity(adapter(health))
                    .build();
            asyncResponse.resume(response);
        } catch (ContributorNotFoundException e) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
        return;
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
            boolean alive = id == null ? service.endpoint().liveness() : service.endpoint().liveness(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(alive)
                    .build());
        } catch (ContributorNotFoundException e) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
        } catch (RuntimeException e) {
            asyncResponse.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
        return;
    }

    @Override
    public void getContributorDependencies(String id, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getDependencies(id, securityContext, asyncResponse, "getContributorDependencies");
    }

    @Override
    public void getDependencies(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getDependencies(null, securityContext, asyncResponse, "getDependencies");
    }

    private void getDependencies(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        try {
            List<String> dependencies = id == null ? service.endpoint().dependencies() : service.endpoint().dependencies(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(adapter(dependencies))
                    .build());
        } catch (ContributorNotFoundException e) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
        return;
    }

    @Override
    public void getContributorDetails(String id, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getDetails(id, securityContext, asyncResponse, "getContributorDetails");
    }

    @Override
    public void getDetails(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getDetails(null, securityContext, asyncResponse, "getDetails");
    }

    private void getDetails(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        try {
            Details details = id == null ? service.endpoint().details() : service.endpoint().details(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(adapter(details))
                    .build());
        } catch (ContributorNotFoundException e) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
        return;
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
            boolean ready = id == null ? service.endpoint().readiness() : service.endpoint().readiness(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(ready)
                    .build());
        } catch (ContributorNotFoundException e) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
        } catch (RuntimeException e) {
            asyncResponse.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
        return;
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
            Status status = id == null ? service.endpoint().status() : service.endpoint().status(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(adapter(status))
                    .build());
        } catch (ContributorNotFoundException e) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
        return;
    }

    // The follow methods provide a means to cast the HealthService framework models, to the generated models.
    private static HealthResult adapter(Health health) {
            return new HealthResult()
                    .name(health.getName())
                    .status(adapter(health.getStatus()))
                    .liveness(health.isAlive())
                    .readiness(health.isReady())
                    .details(adapter(health.getDetails()))
                    .children(health.getChildren().stream()
                            .map(entry -> adapter(entry))
                            .collect(Collectors.toList()));
    }

    private static HealthDetails adapter(Details details) {
        HealthDetails result = new HealthDetails();
        result.putAll(details);
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
