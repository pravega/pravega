/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.test.system;

import io.pravega.common.util.Retry;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.services.PravegaControllerService;
import io.pravega.test.system.framework.services.Service;
import io.pravega.test.system.framework.services.ZookeeperService;
import io.pravega.stream.impl.ControllerImpl;
import io.pravega.test.common.AssertExtensions;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

@Slf4j
@RunWith(SystemTestRunner.class)
public class MultiControllerTest {
    private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();

    private Service controllerServiceInstance1 = null;
    private Service controllerServiceInstance2 = null;
    private Service controllerServiceInstance3 = null;

    private URI controllerURIDirect = null;
    private URI controllerURIDiscover = null;

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException if error in setup
     */
    @Environment
    public static void initialize() throws MarathonException {
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            zkService.start(true);
        }
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        // Start multiple instances of the controller.
        Service controller1 = new PravegaControllerService("multicontroller1", zkUris.get(0));
        if (!controller1.isRunning()) {
            controller1.start(true);
        }
        List<URI> conUris1 = controller1.getServiceDetails();
        log.info("Pravega Controller service instance 1 details: {}", conUris1);
        Service controller2 = new PravegaControllerService("multicontroller2", zkUris.get(0));
        if (!controller2.isRunning()) {
            controller2.start(true);
        }
        List<URI> conUris2 = controller2.getServiceDetails();
        log.info("Pravega Controller service instance 2 details: {}", conUris2);
        Service controller3 = new PravegaControllerService("multicontroller3", zkUris.get(0));
        if (!controller3.isRunning()) {
            controller3.start(true);
        }
        List<URI> conUris3 = controller3.getServiceDetails();
        log.info("Pravega Controller service instance 3 details: {}", conUris3);
    }

    @Before
    public void setup() {
        Service zkService = new ZookeeperService("zookeeper");
        Assert.assertTrue(zkService.isRunning());
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        // Fetch the controller instances.
        List<URI> conUris = new ArrayList<>();
        controllerServiceInstance1 = new PravegaControllerService("multicontroller1", zkUris.get(0));
        Assert.assertTrue(controllerServiceInstance1.isRunning());
        conUris.addAll(controllerServiceInstance1.getServiceDetails());
        controllerServiceInstance2 = new PravegaControllerService("multicontroller2", zkUris.get(0));
        Assert.assertTrue(controllerServiceInstance2.isRunning());
        conUris.addAll(controllerServiceInstance2.getServiceDetails());
        controllerServiceInstance3 = new PravegaControllerService("multicontroller3", zkUris.get(0));
        Assert.assertTrue(controllerServiceInstance3.isRunning());
        conUris.addAll(controllerServiceInstance3.getServiceDetails());
        log.info("Pravega Controller service instance details: {}", conUris);

        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conUris.stream().filter(uri -> uri.getPort() == 9092).map(URI::getAuthority)
                .collect(Collectors.toList());
        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
        controllerURIDiscover = URI.create("pravega://" + String.join(",", uris));
        log.info("Controller Service discovery URI: {}", controllerURIDiscover);
    }

    @After
    public void tearDown() {
        EXECUTOR_SERVICE.shutdownNow();
        if (controllerServiceInstance1 != null && controllerServiceInstance1.isRunning()) {
            controllerServiceInstance1.stop();
            controllerServiceInstance1.clean();
        }
        if (controllerServiceInstance2 != null && controllerServiceInstance2.isRunning()) {
            controllerServiceInstance2.stop();
            controllerServiceInstance2.clean();
        }
        if (controllerServiceInstance3 != null && controllerServiceInstance3.isRunning()) {
            controllerServiceInstance3.stop();
            controllerServiceInstance3.clean();
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
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect).get());
        log.info("Test pravega:// with all 3 controller instances running");
        Assert.assertTrue(createScopeWithSimpleRetry(
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDiscover).get());

        controllerServiceInstance1.stop();
        log.info("Test tcp:// with 2 controller instances running");
        Assert.assertTrue(createScopeWithSimpleRetry(
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect).get());
        log.info("Test pravega:// with 2 controller instances running");
        Assert.assertTrue(createScopeWithSimpleRetry(
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDiscover).get());

        controllerServiceInstance2.stop();
        log.info("Test tcp:// with only 1 controller instance running");
        Assert.assertTrue(createScopeWithSimpleRetry(
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect).get());
        log.info("Test pravega:// with only 1 controller instance running");
        Assert.assertTrue(createScopeWithSimpleRetry(
                "scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDiscover).get());

        // All APIs should throw exception and fail.
        controllerServiceInstance3.stop();
        log.info("Test tcp:// with no controller instances running");
        AssertExtensions.assertThrows(ExecutionException.class,
                () -> createScope("scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDirect).get());
        log.info("Test pravega:// with no controller instances running");
        AssertExtensions.assertThrows(ExecutionException.class,
                () -> createScope("scope" + RandomStringUtils.randomAlphanumeric(10), controllerURIDiscover).get());

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
        final ControllerImpl controllerClient = new ControllerImpl(controllerURI);
        return controllerClient.createScope(scopeName);
    }
}
