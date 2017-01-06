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

import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;

import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.DELETE;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.PathParam;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/*
Controller APIs exposed via REST
Different interfaces will hold different groups of APIs
also new version of APIs will be added through a new interface
*/

public final class ApiV1 {

    @Path("/v1")
    public static interface Controller {

        @POST
        @Path("/scopes/{scope}/streams/{stream}")
        @Produces(MediaType.APPLICATION_JSON)
        @Consumes(MediaType.APPLICATION_JSON)
        public Response createStream(StreamConfigurationImpl streamConfig);

        @PUT
        @Path("/scopes/{scope}/streams/{stream}")
        @Produces(MediaType.APPLICATION_JSON)
        @Consumes(MediaType.APPLICATION_JSON)
        public Response updateStreamConfig(StreamConfiguration streamConfig);

        @GET
        @Path("/scopes/{scope}/streams/{stream}")
        @Produces(MediaType.APPLICATION_JSON)
        public Response getStreamConfig(@PathParam("scope") String scope, @PathParam("stream") String stream);

        @GET
        @Path("/scopes/{scope}/streams")
        @Produces(MediaType.APPLICATION_JSON)
        public Response listStreamsInScope(@PathParam("scope") String scope);

        @GET
        @Path("/scopes")
        @Produces(MediaType.APPLICATION_JSON)
        public Response listScopes();

        @DELETE
        @Path("/scopes/{scope}/streams/{stream}")
        @Produces(MediaType.APPLICATION_JSON)
        public Response deleteStream(StreamConfiguration streamConfig);

    }
}
