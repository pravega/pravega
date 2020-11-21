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
import io.pravega.controller.server.rest.generated.model.HealthResult;
import io.pravega.controller.server.rest.generated.model.Status;
import io.pravega.controller.server.rest.v1.ApiV1;
import io.pravega.controller.server.security.auth.RESTAuthHelper;
import io.pravega.controller.server.security.auth.handler.AuthHandlerManager;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.HealthService;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import io.pravega.shared.health.HealthProvider;

import io.pravega.common.LoggerHelpers;

import java.util.Optional;

@Slf4j
public class HealthImpl implements ApiV1.HealthApi {

    private HealthService service = HealthProvider.getHealthService();

    private final RESTAuthHelper restAuthHelper;

    public HealthImpl(AuthHandlerManager pravegaAuthManager) {
        this.restAuthHelper = new RESTAuthHelper(pravegaAuthManager);
    }

    @Override
    public void getContributorHealth(String id, Boolean details, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getHealth(id, details, securityContext, asyncResponse, "getContributorHealth");
    }

    @Override
    public void getHealth(Boolean details, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getHealth(null, details, securityContext, asyncResponse, "getHealth");
    }

    private void getHealth(String id, Boolean details, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        Optional<HealthContributor> contributor = id != null ? service.registry().get(id) : service.registry().get();
        long traceId = LoggerHelpers.traceEnter(log, method);
        if (contributor.isEmpty()) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
            LoggerHelpers.traceLeave(log, method, traceId);
            return;
        }

        asyncResponse.resume(Response.status(Response.Status.OK)
                .entity(contributor.get().health(details))
                .build());
        LoggerHelpers.traceLeave(log, method, traceId);
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
        Optional<HealthContributor> contributor = id != null ? service.registry().get(id) : service.registry().get();
        long traceId = LoggerHelpers.traceEnter(log, method);
        if (contributor.isEmpty()) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
            LoggerHelpers.traceLeave(log, method, traceId);
            return;
        }

        asyncResponse.resume(Response.status(Response.Status.OK).entity(contributor.get().health().isAlive()).build());
        LoggerHelpers.traceLeave(log, method, traceId);
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
        Optional<HealthContributor> contributor = id == null ? service.registry().get(id) : service.registry().get();
        long traceId = LoggerHelpers.traceEnter(log, method);
        if (contributor.isEmpty()) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
            LoggerHelpers.traceLeave(log, method, traceId);
            return;
        }

        asyncResponse.resume(Response.status(Response.Status.OK)
                .entity(contributor.get().contributors().stream().map(item -> item.getName()))
                .build());
        LoggerHelpers.traceLeave(log, method, traceId);
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
        Optional<HealthContributor> contributor = id != null ? service.registry().get(id) : service.registry().get();
        long traceId = LoggerHelpers.traceEnter(log, method);
        if (contributor.isEmpty()) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
            LoggerHelpers.traceLeave(log, method, traceId);
            return;
        }

        asyncResponse.resume(Response.status(Response.Status.OK)
                .entity(contributor.get().health().getDetails())
                .build());
        LoggerHelpers.traceLeave(log, method, traceId);
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
        Optional<HealthContributor> contributor = id != null ? service.registry().get(id) : service.registry().get();
        long traceId = LoggerHelpers.traceEnter(log, method);
        if (contributor.isEmpty()) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
            LoggerHelpers.traceLeave(log, method, traceId);
            return;
        }

        asyncResponse.resume(Response.status(Response.Status.OK)
                .entity(contributor.get().health().isReady())
                .build());
        LoggerHelpers.traceLeave(log, method, traceId);
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
        Optional<HealthContributor> contributor = id != null ? service.registry().get(id) : service.registry().get();
        long traceId = LoggerHelpers.traceEnter(log, method);
        if (contributor.isEmpty()) {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
            LoggerHelpers.traceLeave(log, method, traceId);
            return;
        }

        asyncResponse.resume(Response.status(Response.Status.OK)
                .entity(contributor.get().health().getStatus())
                .build());
        LoggerHelpers.traceLeave(log, method, traceId);
        return;
    }

    private HealthResult castHealth(Health health) {
        return new HealthResult()
                .status(Status.fromValue(health.getStatus().toString()))
                .liveness(health.isAlive())
                .readiness(health.isReady())
                .details(health.getDetails())
                .children(health.getChildren());


    }

    private

}
