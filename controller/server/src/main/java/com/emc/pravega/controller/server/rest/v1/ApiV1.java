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

import com.emc.pravega.controller.server.rest.contract.request.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.request.UpdateStreamRequest;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.PathParam;
import javax.ws.rs.PUT;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/*
Controller APIs exposed via REST
Different interfaces will hold different groups of APIs
*/

public final class ApiV1 {

    @Path("/ping")
    public static interface Ping {
        @GET
        public Response ping();
    }

    @Path("/v1")
    public static interface StreamMetaData {

        @POST
        @Path("/scopes/{scope}/streams")
        @Produces(MediaType.APPLICATION_JSON)
        @Consumes(MediaType.APPLICATION_JSON)
        public void createStream(@Suspended final AsyncResponse asyncResponse, CreateStreamRequest createStreamRequest,
                                 @PathParam("scope") String scope);


        @PUT
        @Path("/scopes/{scope}/streams/{stream}")
        @Produces(MediaType.APPLICATION_JSON)
        @Consumes(MediaType.APPLICATION_JSON)
        public void updateStreamConfig(@Suspended final AsyncResponse asyncResponse, UpdateStreamRequest updateStreamRequest, @PathParam("scope") String scope,
                                           @PathParam("stream") String stream);

        @GET
        @Path("/scopes/{scope}/streams/{stream}")
        @Produces(MediaType.APPLICATION_JSON)
        public void getStreamConfig(@Suspended final AsyncResponse asyncResponse, @PathParam("scope") String scope, @PathParam("stream") String stream);

    }
}
