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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthComponent;
import io.pravega.shared.health.HealthComponentConfig;
import io.pravega.shared.health.HealthComponentEndpoint;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.HealthService;
import io.pravega.shared.health.HealthServiceConfig;
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
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class HealthServiceImpl implements HealthService {

    /**
     * The singleton {@link HealthService} INSTANCE.
     */
    public static final HealthService INSTANCE = new HealthServiceImpl();
    @VisibleForTesting
    public ContributorRegistryImpl registry;
    private HealthServiceConfig serverConfig;
    private HealthComponentConfig componentConfig;
    @Getter
    private URI uri;
    private HttpServer server;
    /**
     * The base under which all other {@link HealthComponent} will be registered.
     */
    private final HealthComponent root;
    /**
     * Due to the use of a SINGLETON to simplify {@link HealthContributor} registration, this flag is used to signal
     * when the {@link HealthService} is ready to be started (client configuration has been applied).
     */
    private boolean initialized = false;
    /**
     * The {@link ScheduledExecutorService} used for recurring health checks.
     */
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "health-check");

    private HealthServiceImpl() {
        // Create the root component.
        root = new HealthComponent(ROOT_COMPONENT_NAME);
        // Set default configs.
        serverConfig = HealthServiceConfig.builder().build();
        componentConfig = HealthComponentConfig.empty();
    }

    public void configure() {
        configure(this.serverConfig, this.componentConfig);
    }

   public synchronized void configure(HealthServiceConfig serverConfig, HealthComponentConfig componentConfig) {
        if (!this.initialized) {
            // Setup the server.
            this.uri = UriBuilder.fromUri(String.format("http://%s/", serverConfig.getAddress()))
                    .port(serverConfig.getPort())
                    .build();
            // Provide configuration.
            this.serverConfig = serverConfig;
            this.componentConfig = componentConfig;
            // Now considered initialized.
            this.initialized = true;
        } else {
            log.warn("Attempted to call configure() on an already initialized HealthService.");
        }
    }

    public void register(HealthContributor contributor) {
        register(contributor, root);
    }

    public void register(HealthContributor contributor, HealthComponent parent) {
        registry.register(contributor, parent);
    }

    public synchronized void start() throws IOException {
        // If has not already been configured, do so now. May use default configuration classes.
        if (!initialized) {
            // Initialize the server, but don't start.
            configure();
            this.server = createHttpServer(uri);
        }
        if (!server.isStarted()) {
            server.start();
            log.info("Starting Health HTTP Server @ {}", uri);
        } else {
            log.warn("start() was called with an existing INSTANCE or active REST server.");
        }
        if (serverConfig.isMonitorEnabled()) {
            // Continuously monitor the health of the top level service.
            executor.scheduleAtFixedRate(() -> INSTANCE.health(true),
                    serverConfig.getInterval(),
                    serverConfig.getInterval(),
                    TimeUnit.SECONDS);
        }
        // Initiate the contributor registry.
        this.registry = new ContributorRegistryImpl(root, componentConfig);
    }

    public synchronized void stop() {
        if (INSTANCE == null || !server.isStarted()) {
            log.warn("stop() was called with no existing INSTANCE or an inactive REST server.");
            return;
        }

        log.info("Shutting down the health monitor executor service.");
        executor.shutdownNow();
        try {
            log.info("Await for monitor termination.");
            executor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Unexpected InterruptedException shutting down health monitor.", e);
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
            server = createHttpServer(uri);
        }

    }

    private synchronized HttpServer createHttpServer(URI uri) {
        return GrizzlyHttpServerFactory.createHttpServer(uri,
                new ResourceConfig().register(HealthEndpoint.class).register(HealthComponentEndpoint.class),
                false);
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

    public Health health(String name, boolean includeDetails) {
        Optional<HealthContributor> result = registry.get(name);
        if (result.isEmpty()) {
            log.error("No HealthComponent with name: {} found in the registry.", name);
            return null;
        }
        return result.get().health(includeDetails);
    }

    public Health health(boolean includeDetails) {
        return health(root.getName(), includeDetails);
    }

    public Collection<HealthComponent> components() {
       return registry.getComponents()
               .entrySet()
               .stream()
               .map(entry -> entry.getValue())
               .collect(Collectors.toList());
    }

    private void runHealthMonitor() {
    }

}
