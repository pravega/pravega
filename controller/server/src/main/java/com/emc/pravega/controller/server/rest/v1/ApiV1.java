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

import com.emc.pravega.controller.server.rest.resources.SampleRequest;
import com.emc.pravega.stream.StreamConfiguration;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

/*
Conntroller APIs exposed via REST
Different interfaces will hold different groups of APIs
also new version of APIs will be added through a new interface
 */
public final class ApiV1 {

    @Path("/hello")
    public static interface Hello {

        @GET
        @Path("/world")
        @Produces(MediaType.APPLICATION_JSON)
        public String helloWorld();

        @GET
        @Path("/user/{var}")
        @Produces({MediaType.APPLICATION_JSON})
        public String printName(@PathParam("var") String name);

        @GET
        @Path("/wrapper")
        @Produces(MediaType.APPLICATION_JSON)
        public Response getWrapper();

        @POST
        @Path("/jackson")
        @Produces(MediaType.APPLICATION_JSON)
        @Consumes(MediaType.APPLICATION_JSON)
        public Response getResponse(SampleRequest request) throws IOException;


    }

    @Path("/pravega")
    public static interface Controller {

        @POST
        @Path("/scopes/{scope}/streams/{stream}")
        @Produces(MediaType.APPLICATION_JSON)
        @Consumes(MediaType.APPLICATION_JSON)
        public Response createStream(StreamConfiguration streamConfiguration) throws IOException;

        @PUT
        @Path("/scopes/{scope}/streams/{stream}")
        @Produces(MediaType.APPLICATION_JSON)
        @Consumes(MediaType.APPLICATION_JSON)
        public Response updateStreamConfig(StreamConfiguration streamConfiguration) throws IOException;



    }
}
