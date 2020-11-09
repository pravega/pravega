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
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
public class HealthComponentEndpoint {

    private static final HealthService SERVICE = HealthServiceImpl.INSTANCE;

    private static final String ID_PATH_PARAM = "/{id}";
    @GET
    @Path(HealthEndpoint.HEALTH_PATH + ID_PATH_PARAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response health(@PathParam("id") String id, @QueryParam("details") boolean details) {
        Health health = SERVICE.health(id, details);
        if (health == null) {
            return invalid();
        }
        return Response.status(Response.Status.OK)
                .entity(health)
                .build();
    }

    @GET
    @Path(HealthEndpoint.LIVENESS_PATH + ID_PATH_PARAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response readiness(@PathParam("id") String id) {
        Health health = SERVICE.health(id, false);
        if (health == null) {
            return invalid();
        }
        return Response.status(Response.Status.OK)
                .entity(health.ready())
                .build();
    }

    @GET
    @Path(HealthEndpoint.READINESS_PATH + ID_PATH_PARAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response liveness(@PathParam("id") String id) {
        Health health = SERVICE.health(id, false);
        if (health == null) {
            return invalid();
        }
        return Response.status(Response.Status.OK)
                .entity(health.alive())
                .build();
    }

    @GET
    @Path(HealthEndpoint.DETAILS_PATH + ID_PATH_PARAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response details(@PathParam("id") String id) {
        Health health = SERVICE.health(id, true);
        if (health == null) {
            return invalid();
        }
        return Response.status(Response.Status.OK)
                    .entity(health.getDetails())
                    .build();
    }

    private static Response invalid() {
        return Response.status(Response.Status.NOT_FOUND)
                .entity(null)
                .build();
    }
}
