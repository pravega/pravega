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

import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.common.LoggerHelpers;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.controller.server.rest.resources.PingImpl;
import io.pravega.controller.server.rest.resources.StreamMetadataResourceImpl;
import io.pravega.controller.server.rpc.auth.PravegaAuthManager;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.UriBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.glassfish.grizzly.GrizzlyFuture;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

/**
 * Netty REST server implementation.
 */
@Slf4j
public class RESTServer extends AbstractIdleService {

    private final String objectId;
    private final RESTServerConfig restServerConfig;
    private final URI baseUri;
    private final ResourceConfig resourceConfig;
    private HttpServer httpServer;

    public RESTServer(LocalController localController, ControllerService controllerService, PravegaAuthManager pravegaAuthManager, RESTServerConfig restServerConfig, ConnectionFactory connectionFactory) {
        this.objectId = "RESTServer";
        this.restServerConfig = restServerConfig;
        final String serverURI = "http://" + restServerConfig.getHost() + "/";
        this.baseUri = UriBuilder.fromUri(serverURI).port(restServerConfig.getPort()).build();

        final Set<Object> resourceObjs = new HashSet<>();
        resourceObjs.add(new PingImpl());
        resourceObjs.add(new StreamMetadataResourceImpl(localController, controllerService, pravegaAuthManager, connectionFactory));

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
            if (restServerConfig.isTlsEnabled()) {
                SSLContextConfigurator contextConfigurator = new SSLContextConfigurator();
                contextConfigurator.setKeyStoreFile(restServerConfig.getKeyFilePath());
                contextConfigurator.setKeyStorePass(loadPasswordFromFile(restServerConfig.getKeyFilePasswordPath()));
                httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, resourceConfig, true,
                        new SSLEngineConfigurator(contextConfigurator, false, false, false));
            } else {
                httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, resourceConfig, true);
            }
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    private String loadPasswordFromFile(String keyFilePasswordPath) {
        // In case the path is not specified, return empty string. This means the password is not used.
        // In case of error, return empty password. Which will fail the SSL connection if the password is expected.

        if (Strings.isNullOrEmpty(keyFilePasswordPath)) {
            return "";
        }
        File passwdFile = new File(keyFilePasswordPath);
        if (passwdFile.length() == 0) {
            return "";
        }
        try {
            return new String(FileUtils.readFileToByteArray(passwdFile)).trim();
        } catch (IOException e) {
            log.warn("Could not read the password from file.", e);
            return "";
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
            final GrizzlyFuture<HttpServer> shutdown = httpServer.shutdown(30, TimeUnit.SECONDS);
            log.info("Awaiting termination of REST server");
            shutdown.get();
            log.info("REST server terminated");
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
    }
}
