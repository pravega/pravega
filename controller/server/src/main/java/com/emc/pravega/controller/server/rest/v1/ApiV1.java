/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.controller.server.rest.v1;

import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.StreamProperty;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.PathParam;
import javax.ws.rs.PUT;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * Controller APIs exposed via REST.
 * Different interfaces will hold different groups of APIs.
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

        /**
         * REST API to create a stream.
         *
         * @param scopeName             The scope of the stream.
         * @param createStreamRequest   The object conforming to createStream request json.
         * @param securityContext       The security context for the API.
         * @param asyncResponse         AsyncResponse provides means for asynchronous server side response processing.
         */
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
        void createStream(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName
                , @ApiParam(value = "The stream configuration", required = true) CreateStreamRequest createStreamRequest
                , @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        /**
         * REST API to get configuration of a stream.
         *
         * @param scopeName         The scope of the stream.
         * @param streamName        The name of the stream.
         * @param securityContext   The security context for the API.
         * @param asyncResponse     AsyncResponse provides means for asynchronous server side response processing.
         */
        @GET
        @Path("/{scopeName}/streams/{streamName}")

        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(
                value = "", notes = "Fetch the stream properties", response = StreamProperty.class, tags = {  })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(
                        code = 200, message = "Found stream configuration", response = StreamProperty.class),

                @io.swagger.annotations.ApiResponse(
                        code = 404, message = "Stream not found", response = StreamProperty.class),

                @io.swagger.annotations.ApiResponse(
                        code = 500, message = "Server error", response = StreamProperty.class) })
        void getStream(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName
                , @ApiParam(value = "Stream name", required = true) @PathParam("streamName") String streamName
                , @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        /**
         * REST API to update stream configuration.
         *
         * @param scopeName             The scope of the stream.
         * @param streamName            The name of the stream.
         * @param updateStreamRequest   The object conforming to updateStreamConfig request json.
         * @param securityContext       The security context for the API.
         * @param asyncResponse         AsyncResponse provides means for asynchronous server side response processing.
         */
        @PUT
        @Path("/{scopeName}/streams/{streamName}")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "", response = StreamProperty.class, tags = {  })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(
                        code = 201, message = "Successfully updated the stream configuration",
                        response = StreamProperty.class),

                @io.swagger.annotations.ApiResponse(
                        code = 404, message = "Stream not found", response = StreamProperty.class),

                @io.swagger.annotations.ApiResponse(
                        code = 500, message = "Server error", response = StreamProperty.class) })
        void updateStream(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName
                , @ApiParam(value = "Stream name", required = true) @PathParam("streamName") String streamName
                , @ApiParam(value = "The new stream configuration", required = true)
                        UpdateStreamRequest updateStreamRequest
                , @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);
    }
}
