/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rest;

import com.emc.pravega.controller.server.rest.resources.PingImpl;
import com.emc.pravega.controller.server.rest.resources.StreamMetadataResourceImpl;
import com.emc.pravega.controller.server.ControllerService;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.UriBuilder;

import com.google.common.util.concurrent.AbstractService;
import io.netty.channel.Channel;
import lombok.Lombok;
import org.glassfish.jersey.netty.httpserver.NettyHttpContainerProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import lombok.extern.slf4j.Slf4j;

/**
 * Netty REST server implementation.
 */
@Slf4j
public class RESTServer extends AbstractService {

    private final RESTServerConfig restServerConfig;
    private final URI baseUri;
    private final ResourceConfig resourceConfig;
    private Channel channel;

    public RESTServer(ControllerService controllerService, RESTServerConfig restServerConfig) {
        this.restServerConfig = restServerConfig;
        final String serverURI = "http://" + restServerConfig.getHost() + "/";
        this.baseUri = UriBuilder.fromUri(serverURI).port(restServerConfig.getPort()).build();

        final Set<Object> resourceObjs = new HashSet<Object>();
        resourceObjs.add(new PingImpl());
        resourceObjs.add(new StreamMetadataResourceImpl(controllerService));

        final ControllerApplication controllerApplication = new ControllerApplication(resourceObjs);
        this.resourceConfig = ResourceConfig.forApplication(controllerApplication);
        this.resourceConfig.property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);

        // Register the custom JSON parser.
        this.resourceConfig.register(new CustomObjectMapperProvider());

    }

    /**
     * Start REST service.
     */
    @Override
    protected void doStart() {
        try {
            log.info("Starting REST server listening on port: {}", this.restServerConfig.getPort());
            channel = NettyHttpContainerProvider.createServer(baseUri, resourceConfig, true);
            notifyStarted();
        } catch (Exception e) {
            log.error("Error starting Rest Service {}", e);
            // Throw the error so that the service is marked as FAILED.
            throw Lombok.sneakyThrow(e);
        }
    }

    /**
     * Gracefully stop REST service.
     */
    @Override
    protected void doStop() {
        log.info("Stopping REST server listening on port: {}", this.restServerConfig.getPort());
        channel.close();
    }
}
