/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.Assert;

@Slf4j
abstract class AbstractControllerTests {
    static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();

    Service controllerService1 = null;
    AtomicReference<URI> controllerURIDirect = new AtomicReference();
    URI controllerURIDiscover = null;

    static void setup() throws MarathonException, ExecutionException {
        Service zkService = Utils.createZookeeperService();
        if (!zkService.isRunning()) {
            zkService.start(true);
        }
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        Service controllerService = Utils.createPravegaControllerService(zkUris.get(0));
        if (!controllerService.isRunning()) {
            controllerService.start(true);
        }
        Futures.getAndHandleExceptions(controllerService.scaleService(3), ExecutionException::new);

        List<URI> conUris = controllerService.getServiceDetails();
        log.info("conuris {} {}", conUris.get(0), conUris.get(1));
        log.debug("Pravega Controller service  details: {}", conUris);
        // Fetch all the RPC endpoints and construct the client URIs.
        List<String> uris;
        if (Utils.isDockerLocalExecEnabled()) {
            uris = conUris.stream().filter(uri -> uri.getPort() == Utils.DOCKER_CONTROLLER_PORT).map(URI::getAuthority)
                    .collect(Collectors.toList());
            log.info("uris {}", uris);
        } else {
            uris = conUris.stream().filter(uri -> uri.getPort() == Utils.MARATHON_CONTROLLER_PORT).map(URI::getAuthority)
                    .collect(Collectors.toList());
        }
        URI controllerURI = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURI);
    }

    public void getControllerInfo() {
        Service zkService = Utils.createZookeeperService();
        Assert.assertTrue(zkService.isRunning());
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        controllerService1 = Utils.createPravegaControllerService(zkUris.get(0));
        if (!controllerService1.isRunning()) {
            controllerService1.start(true);
        }

        List<URI> conUris = controllerService1.getServiceDetails();
        log.info("conuris {} {}", conUris.get(0), conUris.get(1));
        log.debug("Pravega Controller service  details: {}", conUris);
        // Fetch all the RPC endpoints and construct the client URIs.
        List<String> uris;
        if (Utils.isDockerLocalExecEnabled()) {
            uris = conUris.stream().filter(uri -> uri.getPort() == Utils.DOCKER_CONTROLLER_PORT).map(URI::getAuthority)
                    .collect(Collectors.toList());
            log.info("uris {}", uris);
        } else {
            uris = conUris.stream().filter(uri -> uri.getPort() == Utils.MARATHON_CONTROLLER_PORT).map(URI::getAuthority)
                    .collect(Collectors.toList());
        }
        controllerURIDirect.set(URI.create("tcp://" + String.join(",", uris)));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
        controllerURIDiscover = URI.create("pravega://" + String.join(",", uris));
        log.info("Controller Service discovery URI: {}", controllerURIDiscover);
    }

    public void tearDown() {
        EXECUTOR_SERVICE.shutdownNow();
        if (controllerService1 != null && controllerService1.isRunning()) {
            controllerService1.stop();
            controllerService1.clean();
        }
    }

    CompletableFuture<Boolean> createScopeWithSimpleRetry(String scopeName, URI controllerURI) {
        // Need to retry since there is a delay for the mesos DNS name to resolve correctly.
        return Retry.withExpBackoff(500, 2, 10, 5000)
                .retryingOn(Exception.class)
                .throwingOn(IllegalArgumentException.class)
                .runAsync(() -> createScope(scopeName, controllerURI), EXECUTOR_SERVICE);
    }

    CompletableFuture<Boolean> createScope(String scopeName, URI controllerURI) {
        final ControllerImpl controllerClient = new ControllerImpl(controllerURI,
                ControllerImplConfig.builder().build(), EXECUTOR_SERVICE);
        return controllerClient.createScope(scopeName);
    }
}
