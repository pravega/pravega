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


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/")
public class HealthEndpoint {

    public static final String PING_RESPONSE = "pong";

    @GET
    @Path("/ping")
    @Produces(MediaType.TEXT_PLAIN)
    public String ping() {
        return PING_RESPONSE;
    }

    @GET
    @Path("/health")
    @Produces(MediaType.APPLICATION_JSON)
    public String health() {
        return "health";
    }

    @GET
    @Path("/health/readiness")
    @Produces(MediaType.APPLICATION_JSON)
    public String readiness() {
        return "readiness";
    }

    @GET
    @Path("/health/liveness")
    @Produces(MediaType.APPLICATION_JSON)
    public String liveness() {
        return "liveness";
    }

    @GET
    @Path("/health/details")
    @Produces(MediaType.APPLICATION_JSON)
    public String details() {
        return "details";
    }

    @GET
    @Path("/health/components")
    @Produces(MediaType.APPLICATION_JSON)
    public String components() {
        return "details";
    }
}
