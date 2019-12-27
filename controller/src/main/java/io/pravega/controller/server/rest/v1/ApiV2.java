/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rest.v1;

import io.pravega.controller.server.rest.generated.model.CreateEventRequest;
import io.pravega.controller.server.rest.generated.model.EventProperty;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

public class ApiV2 {

    /**
     * Stream data version 2.0 APIs.
     */
/*
    @Path("/v1/events")
    @io.swagger.annotations.Api(description = "the events API")
    public interface EventsApi {

        @POST
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @ApiOperation(
                value = "", notes = "Creates a new event", response = EventProperty.class, tags = {})
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the event", response = EventProperty.class),

                @ApiResponse(
                        code = 409, message = "Event already exists", response = EventProperty.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = EventProperty.class)})
        void createEvent(
                @ApiParam(value = "The event configuration", required = true) CreateEventRequest createEventRequest,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

    }
*/
}
