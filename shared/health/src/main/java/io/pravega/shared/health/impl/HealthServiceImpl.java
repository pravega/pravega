/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health.impl;

import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.HealthService;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.grizzly.GrizzlyFuture;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.URI;
import java.net.UnknownHostException;

@Slf4j
public class HealthServiceImpl extends ContributorRegistryImpl implements HealthService {

    /**
     * The singleton {@link HealthService} INSTANCE.
     */
    public static final HealthService INSTANCE = new HealthServiceImpl();

    /**
     * The default PORT to host the {@link HealthService} on.
     */
    private static final int HEALTH_REST_PORT = 10090;

    private ContributorRegistry registry;

    private HttpServer server;

    @Getter
    private URI uri;

    private HealthServiceImpl() {
        // Initiate the contributor registry.
        registry = new ContributorRegistryImpl();
        // Setup the server.
        uri = UriBuilder.fromUri(String.format("http://%s/", getHostAddress())).port(HEALTH_REST_PORT).build();
        server = createHttpServer();
    }

    public synchronized void start() throws IOException {
        if (!server.isStarted()) {
            server.start();
            log.info("Starting Health HTTP Server @ {}", uri);
        } else {
            log.warn("start() was called with an existing INSTANCE or active REST server.");
        }
    }

    public synchronized void stop() {
        if (INSTANCE == null || !server.isStarted()) {
            log.warn("stop() was called with no existing INSTANCE or an inactive REST server.");
            return;
        }
        try {
            log.info("Clearing the ContributorRegistry.");
            registry.clear();
            log.info("Stopping REST server @ {}", uri);
            final GrizzlyFuture<HttpServer> shutdown = server.shutdown();
            log.info("Awaiting termination of server.");
            shutdown.get();
            log.info("Server terminated.");
        } catch (Exception e) {
            log.error("Error shutting down REST server. Unhandled exception : {}", e);
        } finally {
            server = createHttpServer();
        }
    }

    private synchronized HttpServer createHttpServer() {
        return GrizzlyHttpServerFactory.createHttpServer(uri, new ResourceConfig().register(HealthEndpoint.class), false);
    }

    /**
     * The default address to host the {@link HealthService} on.
     */
    @SneakyThrows(UnknownHostException.class)
    private static String getHostAddress() {
        String address = Inet4Address.getLocalHost().getHostAddress();
        return address == null ? Inet4Address.getLoopbackAddress().getHostAddress() : address;
    }

    public synchronized boolean running() {
        return server == null || !server.isStarted() ? false : true;
    }
}
