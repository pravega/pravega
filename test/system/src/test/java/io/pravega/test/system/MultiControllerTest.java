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
import io.pravega.common.Exceptions;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.TestFrameworkException;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.pravega.test.system.framework.TestFrameworkException.Type.RequestFailed;

@Slf4j
@RunWith(SystemTestRunner.class)
public class MultiControllerTest {
    private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();

    private Service controllerService1 = null;
    private AtomicReference<URI> controllerURIDirect = new AtomicReference();
    private URI controllerURIDiscover = null;

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException if error in setup
     */
    @Environment
    public static void initialize() throws MarathonException {
        Service zkService = Utils.createServiceInstance("zookeeper", null, null, null);
        if (!zkService.isRunning()) {
            zkService.start(true);
        }
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        Service controllerService = Utils.createServiceInstance("controller", zkUris.get(0), null, null);
        if (!controllerService.isRunning()) {
            controllerService.start(true);
        }
        try {
            Exceptions.handleInterrupted(() -> controllerService.scaleService(3).get());
        } catch (ExecutionException e) {
            throw new TestFrameworkException(RequestFailed, "Scaling operation failed", e);
        }
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

    @Before
    public void setup() {
        Service zkService = Utils.createServiceInstance("zookeeper", null, null, null);
        Assert.assertTrue(zkService.isRunning());
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        controllerService1 = Utils.createServiceInstance("controller", zkUris.get(0), null, null);
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

    @After
    public void tearDown() {
        EXECUTOR_SERVICE.shutdownNow();
        if (controllerService1 != null && controllerService1.isRunning()) {
            controllerService1.stop();
            controllerService1.clean();
        }
    }

    /**
     * Invoke the multi controller test.
     *
     * @throws ExecutionException   On API execution failures.
     * @throws InterruptedException If test is interrupted.
     */
    @Test(timeout = 300000)
    public void multiControllerTest() throws ExecutionException, InterruptedException {

        log.info("Start execution of multiControllerTest");

        log.info("Test tcp:// with all 3 controller instances running");
        Assert.assertTrue(createScopeWithSimpleRetry(
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect.get()).get());
        if (!Utils.isDockerLocalExecEnabled()) {
            log.info("Test pravega:// with all 3 controller instances running");
            Assert.assertTrue(createScopeWithSimpleRetry(
                    "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDiscover).get());
        }

        try {
            Exceptions.handleInterrupted(() -> controllerService1.scaleService(2).get());
        } catch (ExecutionException e) {
            throw new TestFrameworkException(RequestFailed, "Scaling operation failed", e);
        }

        log.info("Test tcp:// with 2 controller instances running");
        Assert.assertTrue(createScopeWithSimpleRetry(
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect.get()).get());
        if (!Utils.isDockerLocalExecEnabled()) {
            log.info("Test pravega:// with 2 controller instances running");
            Assert.assertTrue(createScopeWithSimpleRetry(
                    "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDiscover).get());
        }

        try {
            Exceptions.handleInterrupted(() -> controllerService1.scaleService(1).get());
        } catch (ExecutionException e) {
            throw new TestFrameworkException(RequestFailed, "Scaling operation failed", e);
        }

        log.info("Test tcp:// with only 1 controller instance running");
        Assert.assertTrue(createScopeWithSimpleRetry(
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect.get()).get());
        if (!Utils.isDockerLocalExecEnabled()) {
            log.info("Test pravega:// with only 1 controller instance running");
            Assert.assertTrue(createScopeWithSimpleRetry(
                    "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDiscover).get());
        }

        // All APIs should throw exception and fail.
        try {
            Exceptions.handleInterrupted(() -> controllerService1.scaleService(0).get());
        } catch (ExecutionException e) {
            throw new TestFrameworkException(RequestFailed, "Scaling operation failed", e);
        }
        if (!controllerService1.getServiceDetails().isEmpty()) {
            controllerURIDirect.set(controllerService1.getServiceDetails().get(0));
        } else {
            controllerURIDirect.set(URI.create("tcp://0.0.0.0:9090"));
        }

        log.info("Test tcp:// with no controller instances running");
        AssertExtensions.assertThrows("Should throw RetriesExhaustedException",
                createScope("scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect.get()),
                throwable -> throwable instanceof RetriesExhaustedException);

        if (!Utils.isDockerLocalExecEnabled()) {
            log.info("Test pravega:// with no controller instances running");
            AssertExtensions.assertThrows("Should throw RetriesExhaustedException",
                    createScope("scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDiscover),
                    throwable -> throwable instanceof RetriesExhaustedException);
        }
        log.info("multiControllerTest execution completed");
    }

    private CompletableFuture<Boolean> createScopeWithSimpleRetry(String scopeName, URI controllerURI) {
        // Need to retry since there is a delay for the mesos DNS name to resolve correctly.
        return Retry.withExpBackoff(500, 2, 10, 5000)
                .retryingOn(Exception.class)
                .throwingOn(IllegalArgumentException.class)
                .runAsync(() -> createScope(scopeName, controllerURI), EXECUTOR_SERVICE);
    }

    private CompletableFuture<Boolean> createScope(String scopeName, URI controllerURI) {
        final ControllerImpl controllerClient = new ControllerImpl(controllerURI,
                ControllerImplConfig.builder().build(), EXECUTOR_SERVICE);
        return controllerClient.createScope(scopeName);
    }
}
