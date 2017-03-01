/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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

        // Register the custom JSON parser.
        resourceConfig.register(new CustomObjectMapperProvider());

        try {
            NettyHttpContainerProvider.createServer(baseUri, resourceConfig, true);
        } catch (Exception e) {
            log.error("Error starting Rest Service {}", e);
        }
    }

}
