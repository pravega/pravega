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
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import java.util.concurrent.ExecutionException;

@Slf4j
@RunWith(SystemTestRunner.class)
public class DockerBasedMultiControllerTest extends AbstractControllerTests {

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException if error in setup
     */
    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        setup();
    }

    @Before
    public void getControllerInfo() {
        Assume.assumeTrue("Docker swarm based execution is enabled",  Utils.isDockerLocalExecEnabled());
        super.getControllerInfo();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * Invoke the multi controller test.
     *
     * @throws ExecutionException   On API execution failures.
     * @throws InterruptedException If test is interrupted.
     */
    @Test(timeout = 300000)
    public void dockerBasedMultiControllerTest() throws ExecutionException, InterruptedException {

        log.info("Start execution of multiControllerTest");

        log.info("Test tcp:// with all 3 controller instances running");
        Assert.assertTrue(createScopeWithSimpleRetry(
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect.get()).get());

        Futures.getAndHandleExceptions(controllerService1.scaleService(2), ExecutionException::new);

        log.info("Test tcp:// with 2 controller instances running");
        Assert.assertTrue(createScopeWithSimpleRetry(
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect.get()).get());

        Futures.getAndHandleExceptions(controllerService1.scaleService(1), ExecutionException::new);

        log.info("Test tcp:// with only 1 controller instance running");
        Assert.assertTrue(createScopeWithSimpleRetry(
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect.get()).get());

        // All APIs should throw exception and fail.
        Futures.getAndHandleExceptions(controllerService1.scaleService(0), ExecutionException::new);

        if (!controllerService1.getServiceDetails().isEmpty()) {
            controllerURIDirect.set(controllerService1.getServiceDetails().get(0));
        } else {
            controllerURIDirect.set(URI.create("tcp://0.0.0.0:9090"));
        }

        log.info("Test tcp:// with no controller instances running");
        AssertExtensions.assertThrows("Should throw RetriesExhaustedException",
                createScope("scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect.get()),
                throwable -> throwable instanceof RetriesExhaustedException);
        log.info("multiControllerTest execution completed");
    }
}
