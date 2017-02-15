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

package com.emc.pravega.controller.server.rest;

import com.emc.pravega.controller.server.rest.resources.PingImpl;
import com.emc.pravega.controller.server.rest.resources.StreamMetadataResourceImpl;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.netty.httpserver.NettyHttpContainerProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import static com.emc.pravega.controller.util.Config.REST_SERVER_IP;
import static com.emc.pravega.controller.util.Config.REST_SERVER_PORT;

import lombok.extern.slf4j.Slf4j;

/**
 * Netty REST server implementation.
 */
@Slf4j
public class RESTServer {

    public static final void start(ControllerService controllerService) {

        final String serverURI = "http://" + REST_SERVER_IP + "/";
        final URI baseUri = UriBuilder.fromUri(serverURI).port(REST_SERVER_PORT).build();

        final Set<Object> resourceObjs = new HashSet<Object>();
        resourceObjs.add(new PingImpl());
        resourceObjs.add(new StreamMetadataResourceImpl(controllerService));

        final ControllerApplication controllerApplication = new ControllerApplication(resourceObjs);
        final ResourceConfig resourceConfig = ResourceConfig.forApplication(controllerApplication);
        resourceConfig.property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);

        try {
            NettyHttpContainerProvider.createServer(baseUri, resourceConfig, true);
        } catch (Exception e) {
            log.error("Error starting Rest Service {}", e);
        }
    }

}
