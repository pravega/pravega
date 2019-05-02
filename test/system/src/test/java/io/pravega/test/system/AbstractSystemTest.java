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

import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.pravega.test.system.framework.Utils.EXECUTOR_TYPE;

/**
 * Abstract class containing utilities to initialize Pravega services that are necessary in most system tests.
 */
@Slf4j
abstract class AbstractSystemTest {

    static final Predicate<URI> ISGRPC = uri -> {
        log.info("URI in ISGRPC: {}", uri);
        switch (EXECUTOR_TYPE) {
            case REMOTE_SEQUENTIAL:
                log.info("REMOTE_SEQUENTIAL in ISGRPC: {}", uri.getPort() == Utils.MARATHON_CONTROLLER_PORT);
                return uri.getPort() == Utils.MARATHON_CONTROLLER_PORT;
            case DOCKER:
            case KUBERNETES:
            default:
                log.info("DOCKER, KUBERNETES in ISGRPC: {}", uri.getPort() == Utils.DOCKER_CONTROLLER_PORT);  
                return uri.getPort() == Utils.DOCKER_CONTROLLER_PORT;
        }
    };
    static URI startZookeeperInstance() {
        Service zkService = Utils.createZookeeperService();
        if (!zkService.isRunning()) {
            zkService.start(true);
        }
        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        return zkUris.get(0);
    }

    static void startBookkeeperInstances(final URI zkUri) {
        Service bkService = Utils.createBookkeeperService(zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }
        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("Bookkeeper service details: {}", bkUris);
    }

    static URI ensureControllerRunning(final URI zkUri) {
        Service conService = Utils.createPravegaControllerService(zkUri);
        if (!conService.isRunning()) {
            log.info("Starting the controller service");
            conService.start(true);
        }

        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service details: {}", conUris);
        return conUris.get(0);
    }

    static List<URI> ensureSegmentStoreRunning(final URI zkUri, final URI controllerURI) {
        Service segService = Utils.createPravegaSegmentStoreService(zkUri, controllerURI);
        if (!segService.isRunning()) {
            segService.start(true);
        }

        List<URI> segUris = segService.getServiceDetails();
        log.debug("Pravega segmentstore service details: {}", segUris);
        return segUris;
    }

    static URI startPravegaControllerInstances(final URI zkUri, final int instanceCount) throws ExecutionException {
        Service controllerService = Utils.createPravegaControllerService(zkUri);
        if (!controllerService.isRunning()) {
            controllerService.start(true);
        }
        Futures.getAndHandleExceptions(controllerService.scaleService(instanceCount), ExecutionException::new);
        List<URI> conUris = controllerService.getServiceDetails();
        log.info("Pravega Controller service  details: {}", conUris);

        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conUris.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());

        URI controllerURI = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURI);
        return controllerURI;
    }

    static void startPravegaSegmentStoreInstances(final URI zkUri, final URI controllerURI, final int instanceCount) throws ExecutionException {
        Service segService = Utils.createPravegaSegmentStoreService(zkUri, controllerURI);
        if (!segService.isRunning()) {
            segService.start(true);
        }
        Futures.getAndHandleExceptions(segService.scaleService(instanceCount), ExecutionException::new);
        List<URI> segUris = segService.getServiceDetails();
        log.info("Pravega Segmentstore service details: {}", segUris);
    }
}
