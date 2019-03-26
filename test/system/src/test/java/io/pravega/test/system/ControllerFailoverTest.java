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

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

/**
 * Controller fail over system test.
 */
@Slf4j
@RunWith(SystemTestRunner.class)
public class ControllerFailoverTest extends AbstractSystemTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(3 * 60);

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
    private Service controllerService1 = null;
    private URI controllerURIDirect = null;

    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = startPravegaControllerInstances(zkUri, 1);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Before
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
        final List<String> uris = conUris.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());

        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
    }

    @Test
    public void failoverTest() throws InterruptedException, ExecutionException {
        String scope = "testFailoverScope" + RandomStringUtils.randomAlphabetic(5);
        String stream = "testFailoverStream" + RandomStringUtils.randomAlphabetic(5);
        int initialSegments = 1;
        List<Long> segmentsToSeal = Collections.singletonList(0L);
        Map<Double, Double> newRangesToCreate = new HashMap<>();
        newRangesToCreate.put(0.0, 1.0);

        // Connect with first controller instance.
        final Controller controller1 = new ControllerImpl(
                ControllerImplConfig.builder()
                                    .clientConfig( ClientConfig.builder().controllerURI(controllerURIDirect).build())
                                    .build(), executorService);

        // Create scope, stream, and a transaction with high timeout value.
        controller1.createScope(scope).join();
        log.info("Scope {} created successfully", scope);

        createStream(controller1, scope, stream, ScalingPolicy.fixed(initialSegments));
        log.info("Stream {}/{} created successfully", scope, stream);

        long txnCreationTimestamp = System.nanoTime();
        StreamImpl stream1 = new StreamImpl(scope, stream);

        // Initiate scale operation. It will block until ongoing transaction is complete.
        controller1.startScale(stream1, segmentsToSeal, newRangesToCreate).join();

        // Now stop the controller instance executing scale operation.
        Futures.getAndHandleExceptions(controllerService1.scaleService(0), ExecutionException::new);
        log.info("Successfully stopped one instance of controller service");

        // restart controller service
        Futures.getAndHandleExceptions(controllerService1.scaleService(1), ExecutionException::new);
        log.info("Successfully stopped one instance of controller service");

        List<URI> conUris = controllerService1.getServiceDetails();
        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conUris.stream().filter(ISGRPC).map(URI::getAuthority)
                .collect(Collectors.toList());

        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);

        // Connect to another controller instance.
        @Cleanup
        final Controller controller2 = new ControllerImpl(
                ControllerImplConfig.builder()
                                    .clientConfig(ClientConfig.builder().controllerURI(controllerURIDirect).build())
                                    .build(), executorService);

        // Note: if scale does not complete within desired time, test will timeout.
        boolean scaleStatus = controller2.checkScaleStatus(stream1, 0).join();
        while (!scaleStatus) {
            scaleStatus = controller2.checkScaleStatus(stream1, 0).join();
            Thread.sleep(30000);
        }

        segmentsToSeal = Collections.singletonList(StreamSegmentNameUtils.computeSegmentId(1, 1));

        newRangesToCreate = new HashMap<>();
        newRangesToCreate.put(0.0, 0.5);
        newRangesToCreate.put(0.5, 1.0);

        controller2.scaleStream(stream1, segmentsToSeal, newRangesToCreate, executorService).getFuture().join();

        log.info("Checking whether scale operation succeeded by fetching current segments");
        StreamSegments streamSegments = controller2.getCurrentSegments(scope, stream).join();
        log.info("Current segment count=", streamSegments.getSegments().size());
        Assert.assertEquals(2, streamSegments.getSegments().size());
    }

    private void createStream(Controller controller, String scope, String stream, ScalingPolicy scalingPolicy) {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(scalingPolicy)
                .build();
        controller.createStream(scope, stream, config).join();
    }
}
