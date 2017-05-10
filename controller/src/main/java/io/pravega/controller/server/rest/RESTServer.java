/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rest;

import io.pravega.common.LoggerHelpers;
import io.pravega.controller.server.rest.resources.PingImpl;
import io.pravega.controller.server.rest.resources.StreamMetadataResourceImpl;
import io.pravega.controller.server.ControllerService;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.UriBuilder;

import com.google.common.util.concurrent.AbstractIdleService;
import io.netty.channel.Channel;
import org.glassfish.jersey.netty.httpserver.NettyHttpContainerProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import lombok.extern.slf4j.Slf4j;

/**
 * Netty REST server implementation.
 */
@Slf4j
public class RESTServer extends AbstractIdleService {

    private final String objectId;
    private final RESTServerConfig restServerConfig;
    private final URI baseUri;
    private final ResourceConfig resourceConfig;
    private Channel channel;

    public RESTServer(ControllerService controllerService, RESTServerConfig restServerConfig) {
        this.objectId = "RESTServer";
        this.restServerConfig = restServerConfig;
        final String serverURI = "http://" + restServerConfig.getHost() + "/";
        this.baseUri = UriBuilder.fromUri(serverURI).port(restServerConfig.getPort()).build();

        final Set<Object> resourceObjs = new HashSet<>();
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
    protected void startUp() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        try {
            log.info("Starting REST server listening on port: {}", this.restServerConfig.getPort());
            channel = NettyHttpContainerProvider.createServer(baseUri, resourceConfig, false);
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    /**
     * Gracefully stop REST service.
     */
    @Override
    protected void shutDown() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "shutDown");
        try {
            log.info("Stopping REST server listening on port: {}", this.restServerConfig.getPort());
            channel.close();
            log.info("Awaiting termination of REST server");
            channel.closeFuture().sync();
            log.info("REST server terminated");
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
    }
}
