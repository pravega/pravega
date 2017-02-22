/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rest.v1;

import com.emc.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.InlineResponse201;
import com.emc.pravega.controller.server.rest.generated.model.ScopesList;
import com.emc.pravega.controller.server.rest.generated.model.StreamProperty;
import com.emc.pravega.controller.server.rest.generated.model.StreamsList;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * Controller APIs exposed via REST.
 * Different interfaces will hold different groups of APIs.
 *
 * ##############################IMPORTANT NOTE###################################
 * Do not make any API changes here directly, you need to update swagger/Controller.yaml and generate
 * the server stubs as documented in swagger/README before updating this file.
 *
 */
public final class ApiV1 {

    @Path("/ping")
    public interface Ping {
        @GET
        Response ping();
    }

    /**
     * Stream metadata version 1.0 APIs.
     */
    @Path("/v1/scopes")
    @io.swagger.annotations.Api(description = "the scopes API")
    public interface ScopesApi {

        @POST
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(
                value = "", notes = "Creates a new scope", response = InlineResponse201.class, tags = {  })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(
                        code = 201, message = "Successfully created the scope", response = InlineResponse201.class),

                @io.swagger.annotations.ApiResponse(
                        code = 409, message = "Scope already exists", response = InlineResponse201.class),

                @io.swagger.annotations.ApiResponse(
                        code = 500, message = "Server error", response = InlineResponse201.class) })
        void createScope(
                @ApiParam(value = "The scope configuration", required = true) CreateScopeRequest createScopeRequest,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @POST
        @Path("/{scopeName}/streams")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(
                value = "", notes = "Creates a new stream", response = StreamProperty.class, tags = {  })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(
                        code = 201, message = "Successful created the stream", response = StreamProperty.class),

                @io.swagger.annotations.ApiResponse(
                        code = 409, message = "Stream already exists", response = StreamProperty.class),

                @io.swagger.annotations.ApiResponse(
                        code = 500, message = "Server error", response = StreamProperty.class) })
        void createStream(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "The stream configuration", required = true) CreateStreamRequest createStreamRequest,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @DELETE
        @Path("/{scopeName}")
        @io.swagger.annotations.ApiOperation(value = "", notes = "", response = void.class, tags = {  })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(
                        code = 204, message = "Successfully deleted the scope", response = void.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = void.class),

                @io.swagger.annotations.ApiResponse(
                        code = 412, message = "Connot delete scope which has non-empty list of streams",
                        response = void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = void.class) })
        void deleteScope(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @DELETE
        @Path("/{scopeName}/streams/{streamName}")
        @io.swagger.annotations.ApiOperation(value = "", notes = "", response = void.class, tags = {  })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(
                        code = 204, message = "Successfully deleted the stream", response = void.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or stream not found",
                        response = void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = void.class) })
        void deleteStream(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Stream name", required = true) @PathParam("streamName") String streamName,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @GET
        @Path("/{scopeName}/streams/{streamName}")
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(
                value = "", notes = "Fetch the stream properties", response = StreamProperty.class, tags = {  })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(
                        code = 200, message = "Found stream configuration", response = StreamProperty.class),

                @io.swagger.annotations.ApiResponse(
                        code = 404, message = "Scope or stream not found", response = StreamProperty.class),

                @io.swagger.annotations.ApiResponse(
                        code = 500, message = "Server error", response = StreamProperty.class) })
        void getStream(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Stream name", required = true) @PathParam("streamName") String streamName,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @GET
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(
                value = "", notes = "List all scopes in the system", response = ScopesList.class, tags = {  })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(
                        code = 200, message = "List of scope objects", response = ScopesList.class),

                @io.swagger.annotations.ApiResponse(
                        code = 500, message = "Server error", response = ScopesList.class) })
        void listScopes(@Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @GET
        @Path("/{scopeName}/streams")
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(
                value = "", notes = "List streams within the given scope", response = StreamsList.class, tags = {  })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(
                        code = 200, message = "List of stream objects", response = StreamsList.class),

                @io.swagger.annotations.ApiResponse(
                        code = 404, message = "Scope not found", response = StreamsList.class),

                @io.swagger.annotations.ApiResponse(
                        code = 500, message = "Server error", response = StreamsList.class) })
        void listStreams(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @PUT
        @Path("/{scopeName}/streams/{streamName}")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "", response = StreamProperty.class, tags = {  })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(
                        code = 200,
                        message = "Successfully updated the stream configuration",
                        response = StreamProperty.class),

                @io.swagger.annotations.ApiResponse(
                        code = 404, message = "Scope or stream not found", response = StreamProperty.class),

                @io.swagger.annotations.ApiResponse(
                        code = 500, message = "Server error", response = StreamProperty.class) })
        void updateStream(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Stream name", required = true) @PathParam("streamName") String streamName,
                @ApiParam(value = "The new stream configuration", required = true)
                        UpdateStreamRequest updateStreamRequest,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);
    }
}
