/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;


import io.pravega.shared.health.impl.HealthServiceImpl;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

@Path("/")
public class HealthEndpoint {

    public static final String PING_RESPONSE = "pong";

    private static final HealthService SERVICE = HealthServiceImpl.INSTANCE;

    @GET
    @Path("/ping")
    @Produces(MediaType.TEXT_PLAIN)
    public Response ping() {
        return Response.status(Status.OK).entity(PING_RESPONSE).build();
    }

    @GET
    @Path("/health")
    @Produces(MediaType.APPLICATION_JSON)
    public Response health() {
        return Response.status(Status.OK)
                .entity(SERVICE.health(HealthComponent.ROOT.getName(), false))
                .build();
    }

    @GET
    @Path("/health/readiness")
    @Produces(MediaType.APPLICATION_JSON)
    public Response readiness() {
        return Response.status(Status.OK).build();
    }

    @GET
    @Path("/health/liveness")
    @Produces(MediaType.APPLICATION_JSON)
    public Response liveness() {
        return Response.status(Status.OK).build();
    }

    @GET
    @Path("/health/details")
    @Produces(MediaType.APPLICATION_JSON)
    public Response details() {
        return Response.status(Status.OK).build();
    }

    @GET
    @Path("/health/components")
    @Produces(MediaType.APPLICATION_JSON)
    public Response components() {
        return Response.status(Status.OK).build();
    }
}
