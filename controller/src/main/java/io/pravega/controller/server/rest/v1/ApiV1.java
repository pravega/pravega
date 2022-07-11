/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.server.rest.v1;

import io.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.ReaderGroupProperty;
import io.pravega.controller.server.rest.generated.model.ReaderGroupsList;
import io.pravega.controller.server.rest.generated.model.ScalingEventList;
import io.pravega.controller.server.rest.generated.model.ScopeProperty;
import io.pravega.controller.server.rest.generated.model.ScopesList;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.server.rest.generated.model.StreamState;
import io.pravega.controller.server.rest.generated.model.StreamsList;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
 * the server stubs as documented in swagger/README.md before updating this file.
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
        @ApiOperation(
                value = "", notes = "Creates a new scope", response = ScopeProperty.class, tags = {  })
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the scope", response = ScopeProperty.class),

                @ApiResponse(
                        code = 409, message = "Scope already exists", response = ScopeProperty.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = ScopeProperty.class) })
        void createScope(
                @ApiParam(value = "The scope configuration", required = true) CreateScopeRequest createScopeRequest,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @POST
        @Path("/{scopeName}/streams")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @ApiOperation(
                value = "", notes = "Creates a new stream", response = StreamProperty.class, tags = {  })
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successful created the stream", response = StreamProperty.class),

                @ApiResponse(
                        code = 404, message = "Scope not found", response = StreamProperty.class),

                @ApiResponse(
                        code = 409, message = "Stream already exists", response = StreamProperty.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = StreamProperty.class) })
        void createStream(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "The stream configuration", required = true) CreateStreamRequest createStreamRequest,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @DELETE
        @Path("/{scopeName}")
        @ApiOperation(value = "", notes = "Delete a scope", response = void.class, tags = {  })
        @ApiResponses(value = {
                @ApiResponse(
                        code = 204, message = "Successfully deleted the scope", response = void.class),

                @ApiResponse(code = 404, message = "Scope not found", response = void.class),

                @ApiResponse(
                        code = 412, message = "Cannot delete scope which has non-empty list of streams",
                        response = void.class),

                @ApiResponse(code = 500, message = "Server error", response = void.class) })
        void deleteScope(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @DELETE
        @Path("/{scopeName}/streams/{streamName}")
        @ApiOperation(value = "", notes = "Delete a stream", response = void.class, tags = {  })
        @ApiResponses(value = {
                @ApiResponse(
                        code = 204, message = "Successfully deleted the stream", response = void.class),

                @ApiResponse(
                        code = 404, message = "Stream not found", response = void.class),

                @ApiResponse(
                        code = 412, message = "Cannot delete stream since it is not sealed", response = void.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = void.class) })
        void deleteStream(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Stream name", required = true) @PathParam("streamName") String streamName,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @DELETE
        @Path("/{scopeName}/readergroups/{readerGroupName}")
        @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a reader group", response = void.class,
                tags = { "ReaderGroups", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the reader group",
                        response = void.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Reader group with given name not found",
                        response = void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting a reader group",
                        response = void.class) })
        void deleteReaderGroup(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                               @ApiParam(value = "Reader group name", required = true)
                               @PathParam("readerGroupName") String readerGroupName,
                               @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @GET
        @Path("/{scopeName}/readergroups/{readerGroupName}")
        @Produces({ "application/json" })
        @ApiOperation(value = "", notes = "Fetch the properties of an existing reader group",
                response = ReaderGroupProperty.class, tags = { "ReaderGroups", })
        @ApiResponses(value = {
                @ApiResponse(code = 200, message = "Found reader group properties",
                        response = ReaderGroupProperty.class),

                @ApiResponse(code = 404, message = "Scope or reader group with given name not found",
                        response = ReaderGroupProperty.class),

                @ApiResponse(code = 500, message = "Internal server error while fetching reader group details",
                        response = ReaderGroupProperty.class) })
        void getReaderGroup(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Reader group name", required = true)
                @PathParam("readerGroupName") String readerGroupName,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @GET
        @Path("/{scopeName}/streams/{streamName}/scaling-events")
        @Produces({ "application/json" })
        @ApiOperation(value = "", notes = "Get scaling events for a given datetime period.",
                response = ScalingEventList.class, tags = {  })
        @ApiResponses(value = {
                @ApiResponse(code = 200, message = "List of scaling events",
                        response = ScalingEventList.class),

                @ApiResponse(code = 404, message = "Scope/Stream not found",
                        response = ScalingEventList.class),

                @ApiResponse(code = 500, message = "Server error",
                        response = ScalingEventList.class) })
        void getScalingEvents(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                              @ApiParam(value = "Stream name", required = true) @PathParam("streamName") String streamName,
                              @ApiParam(value = "Parameter to display scaling events from that particular datetime. " +
                                      "Input should be milliseconds from Jan 1 1970.", required = true)
                              @QueryParam("from") Long from,
                              @ApiParam(value = "Parameter to display scaling events to that particular datetime. " +
                                      "Input should be milliseconds from Jan 1 1970.", required = true)
                              @QueryParam("to") Long to,
                              @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @GET
        @Path("/{scopeName}")
        @Produces({ "application/json" })
        @ApiOperation(
                value = "", notes = "Retrieve scope", response = ScopeProperty.class, tags = {  })
        @ApiResponses(value = {
                @ApiResponse(
                        code = 200, message = "Successfully retrieved the scope", response = ScopeProperty.class),

                @ApiResponse(
                        code = 404, message = "Scope not found", response = ScopeProperty.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = ScopeProperty.class) })
        void getScope(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                      @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @GET
        @Path("/{scopeName}/streams/{streamName}")
        @Produces({ "application/json" })
        @ApiOperation(
                value = "", notes = "Fetch the stream properties", response = StreamProperty.class, tags = {  })
        @ApiResponses(value = {
                @ApiResponse(
                        code = 200, message = "Found stream configuration", response = StreamProperty.class),

                @ApiResponse(
                        code = 404, message = "Scope or stream not found", response = StreamProperty.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = StreamProperty.class) })
        void getStream(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Stream name", required = true) @PathParam("streamName") String streamName,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @GET
        @Path("/{scopeName}/readergroups")
        @Produces({ "application/json" })
        @ApiOperation(value = "", notes = "List reader groups within the given scope",
                response = ReaderGroupsList.class, tags = { "ReaderGroups", })
        @ApiResponses(value = {
                @ApiResponse(code = 200, message = "List of all reader groups configured for the given scope",
                        response = ReaderGroupsList.class),

                @ApiResponse(code = 404, message = "Scope not found", response = ReaderGroupsList.class),

                @ApiResponse(code = 500,
                        message = "Internal server error while fetching the list of reader groups for the given scope",
                        response = ReaderGroupsList.class) })
        void listReaderGroups(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @GET
        @Produces({ "application/json" })
        @ApiOperation(
                value = "", notes = "List all scopes in the system", response = ScopesList.class, tags = {  })
        @ApiResponses(value = {
                @ApiResponse(
                        code = 200, message = "List of scope objects", response = ScopesList.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = ScopesList.class) })
        void listScopes(@Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @GET
        @Path("/{scopeName}/streams")
        @Produces({ "application/json" })
        @ApiOperation(
                value = "", notes = "List streams within the given scope", response = StreamsList.class, tags = {  })
        @ApiResponses(value = {
                @ApiResponse(
                        code = 200, message = "List of stream objects", response = StreamsList.class),

                @ApiResponse(
                        code = 404, message = "Scope not found", response = StreamsList.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = StreamsList.class) })
        void listStreams(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                         @ApiParam(value = "Filter options", allowableValues = "showInternalStreams, tag") @QueryParam("filter_type") String filterType,
                 @ApiParam(value = "value to be passed. must match the type passed with it.") @QueryParam("filter_value") String filterValue,
                         @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @PUT
        @Path("/{scopeName}/streams/{streamName}")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @ApiOperation(value = "", notes = "", response = StreamProperty.class, tags = {  })
        @ApiResponses(value = {
                @ApiResponse(
                        code = 200,
                        message = "Successfully updated the stream configuration",
                        response = StreamProperty.class),

                @ApiResponse(
                        code = 404, message = "Scope or stream not found", response = StreamProperty.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = StreamProperty.class) })
        void updateStream(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Stream name", required = true) @PathParam("streamName") String streamName,
                @ApiParam(value = "The new stream configuration", required = true)
                                  UpdateStreamRequest updateStreamRequest,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @PUT
        @Path("/{scopeName}/streams/{streamName}/state")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @ApiOperation(value = "", notes = "Updates the current state of the stream",
                response = StreamState.class, tags = {  })
        @ApiResponses(value = {
                @ApiResponse(code = 200, message = "Successfully updated the stream state",
                        response = StreamState.class),

                @ApiResponse(code = 404, message = "Scope or stream not found",
                        response = StreamState.class),

                @ApiResponse(code = 500, message = "Server error",
                        response = StreamState.class) })
        void updateStreamState(
                @ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Stream name", required = true) @PathParam("streamName") String streamName,
                @ApiParam(value = "The state info to be updated", required = true) StreamState updateStreamStateRequest,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);
    }
}
