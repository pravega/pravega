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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.stream.Collectors;

@Path("/")
public class HealthEndpoint {

    public static final String PING_RESPONSE = "pong";

    public static final String PING_PATH = "/ping";

    public static final String HEALTH_PATH = "/health";

    public static final String READINESS_PATH = HEALTH_PATH + "/readiness";

    public static final String LIVENESS_PATH = HEALTH_PATH + "/liveness";

    public static final String DETAILS_PATH = HEALTH_PATH + "/details";

    public static final String COMPONENTS_PATH = HEALTH_PATH + "/components";

    private static final HealthService SERVICE = HealthServiceImpl.INSTANCE;

    @GET
    @Path(PING_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public Response ping() {
        return Response.status(Status.OK).entity(PING_RESPONSE).build();
    }

    @GET
    @Path(HEALTH_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response health(@QueryParam("details") boolean details) {
        Health health = SERVICE.health(details);
        if (health == null) {
            return invalid();
        }
        return Response.status(Status.OK)
                .entity(health)
                .build();
    }

    @GET
    @Path(READINESS_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response readiness() {
        Health health = SERVICE.health(false);
        if (health == null) {
            return invalid();
        }
        return Response.status(Status.OK)
                .entity(health.ready())
                .build();
    }

    @GET
    @Path(LIVENESS_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response liveness() {
        Health health = SERVICE.health(false);
        if (health == null) {
            return invalid();
        }
        return Response.status(Status.OK)
                .entity(health.alive())
                .build();
    }

    @GET
    @Path(DETAILS_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response details() {
        Health health = SERVICE.health(true);
        if (health == null) {
            return invalid();
        }
        return Response.status(Status.OK)
                .entity(health)
                .build();
    }

    @GET
    @Path(COMPONENTS_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response components() {
        return Response.status(Status.OK)
                .entity(SERVICE.components()
                        .stream()
                        .map(HealthComponent::getName)
                        .collect(Collectors.toList()))
                .build();
    }

    private static Response invalid() {
        return Response.status(Response.Status.NOT_FOUND)
                .entity(null)
                .build();
    }
}
