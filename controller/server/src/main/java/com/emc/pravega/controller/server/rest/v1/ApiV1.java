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

/**
 * Created by root on 12/22/16.
 */

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;


public final class ApiV1 {

    @Path("/hello")
    public static interface Hello {

        @GET
        @Path("/world")
        @Produces(MediaType.TEXT_PLAIN)
        public String helloWorld();

        @GET
        @Path("/user/{var}")
        @Produces({MediaType.TEXT_PLAIN})
        public String printName(@PathParam("var") String name);
    }

    /*@Path("/docker")
    public static interface Docker {

        @GET
        @Path("/repositories/{repository}/images")
        @Produces({MediaType.APPLICATION_JSON})
        public Response images(@PathParam("repository") final String repository);
    }*/
}
