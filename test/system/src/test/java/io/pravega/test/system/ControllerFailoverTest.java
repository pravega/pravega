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

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.impl.TxnSegments;
import io.pravega.common.Exceptions;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.TestFrameworkException;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.docker.HDFSDockerService;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.pravega.test.system.framework.TestFrameworkException.Type.RequestFailed;

/**
 * Controller fail over system test.
 */
@Slf4j
@RunWith(SystemTestRunner.class)
public class ControllerFailoverTest {
    private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newScheduledThreadPool(5);
    private Service controllerService1 = null;
    private URI controllerURIDirect = null;

    @Environment
    public static void initialize() throws InterruptedException, MarathonException, URISyntaxException {

        //1. check if zk is running, if not start it
        Service zkService = Utils.createServiceInstance("zookeeper", null, null, null);
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);
        //2, check if bk is running, otherwise start, get the zk ip
        Service bkService = Utils.createServiceInstance("bookkeeper", zkUri, null, null);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }

        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("bookkeeper service details: {}", bkUris);

        //start HDFS
        URI hdfsUri = null;
        if (Utils.isDockerLocalExecEnabled()) {
            Service hdfsService = new HDFSDockerService("hdfs");
            if (!hdfsService.isRunning()) {
                hdfsService.start(true);
            }
            hdfsUri = hdfsService.getServiceDetails().get(0);
            log.debug("HDFS service details: {}", hdfsService.getServiceDetails());
        }

        //3. start controller
        Service controllerService = Utils.createServiceInstance("controller", zkUri, null, null);
        if (!controllerService.isRunning()) {
            controllerService.start(true);
        }
        try {
            Exceptions.handleInterrupted(() -> controllerService.scaleService(2).get());
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

        //4.start host
        Service segService = Utils.createServiceInstance("segmentstore", zkUri, hdfsUri, conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }

        List<URI> segUris = segService.getServiceDetails();
        log.debug("pravega host service details: {}", segUris);
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
        //controllerService1.scaleService(3, true);
        List<URI> conUris = controllerService1.getServiceDetails();
        log.info("conuris {} {}", conUris.get(0), conUris.get(1));
        log.debug("Pravega Controller service  details: {}", conUris);
        // Fetch all the RPC endpoints and construct the client URIs.
        List<String> uris;
        if (Utils.isDockerLocalExecEnabled()) {
            uris = conUris.stream().filter(uri -> uri.getPort() ==  Utils.DOCKER_CONTROLLER_PORT).map(URI::getAuthority)
                    .collect(Collectors.toList());
            log.info("uris {}", uris);
        } else {
            uris = conUris.stream().filter(uri -> uri.getPort() == Utils.MARATHON_CONTROLLER_PORT).map(URI::getAuthority)
                    .collect(Collectors.toList());
        }
        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
    }

    @Test(timeout = 180000)
    public void failoverTest() throws URISyntaxException, InterruptedException {
        String scope = "testFailoverScope" + RandomStringUtils.randomAlphabetic(5);
        String stream = "testFailoverStream" + RandomStringUtils.randomAlphabetic(5);
        int initialSegments = 2;
        List<Integer> segmentsToSeal = Collections.singletonList(0);
        Map<Double, Double> newRangesToCreate = new HashMap<>();
        newRangesToCreate.put(0.0, 0.25);
        newRangesToCreate.put(0.25, 0.5);
        long lease = 29000;
        long maxExecutionTime = 60000;
        long scaleGracePeriod = 30000;

        // Connect with first controller instance.
        final Controller controller1 = new ControllerImpl(controllerURIDirect,
                ControllerImplConfig.builder().build(), EXECUTOR_SERVICE);

        // Create scope, stream, and a transaction with high timeout value.
        controller1.createScope(scope).join();
        log.info("Scope {} created successfully", scope);

        createStream(controller1, scope, stream, ScalingPolicy.fixed(initialSegments));
        log.info("Stream {}/{} created successfully", scope, stream);

        long txnCreationTimestamp = System.nanoTime();
        StreamImpl stream1 = new StreamImpl(scope, stream);
        TxnSegments txnSegments = controller1.createTransaction(
                stream1, lease, maxExecutionTime, scaleGracePeriod).join();
        log.info("Transaction {} created successfully, beginTime={}", txnSegments.getTxnId(), txnCreationTimestamp);

        // Initiate scale operation. It will block until ongoing transaction is complete.
        controller1.startScale(stream1, segmentsToSeal, newRangesToCreate).join();

        // Ensure that scale is not yet done.
        boolean scaleStatus = controller1.checkScaleStatus(stream1, 0).join();
        log.info("Status of scale operation isDone={}", scaleStatus);
        Assert.assertTrue(!scaleStatus);

        // Now stop the controller instance executing scale operation.
        try {
            Exceptions.handleInterrupted(() -> controllerService1.scaleService(1).get());
        } catch (ExecutionException e) {
            throw new TestFrameworkException(RequestFailed, "Scaling operation failed", e);
        }
        log.info("Successfully stopped test controller service");

        // Connect to another controller instance.
        //controllerUri = getControllerURI();
        final Controller controller2 = new ControllerImpl(controllerURIDirect,
                ControllerImplConfig.builder().build(), EXECUTOR_SERVICE);

        // Fetch status of transaction.
        log.info("Fetching status of transaction {}, time elapsed since its creation={}",
                txnSegments.getTxnId(), System.nanoTime() - txnCreationTimestamp);
        Transaction.Status status = controller2.checkTransactionStatus(stream1,
                txnSegments.getTxnId()).join();
        log.info("Transaction {} status={}", txnSegments.getTxnId(), status);

        if (status == Transaction.Status.OPEN) {
            // Abort the ongoing transaction.
            log.info("Trying to abort transaction {}, by sending request to controller at {}", txnSegments.getTxnId(),
                    controllerURIDirect);
            controller2.abortTransaction(stream1, txnSegments.getTxnId()).join();
        }

        // Scale operation should now complete on the second controller instance.
        // Note: if scale does not complete within desired time, test will timeout. 
        while (!scaleStatus) {
            scaleStatus = controller2.checkScaleStatus(stream1, 0).join();
            Thread.sleep(30000);
        }

        // Ensure that the stream has 3 segments now.
        log.info("Checking whether scale operation succeeded by fetching current segments");
        StreamSegments streamSegments = controller2.getCurrentSegments(scope, stream).join();
        log.info("Current segment count=", streamSegments.getSegments().size());
        Assert.assertEquals(initialSegments - segmentsToSeal.size() + newRangesToCreate.size(),
                streamSegments.getSegments().size());
    }

    private void createStream(Controller controller, String scope, String stream, ScalingPolicy scalingPolicy) {
        StreamConfiguration config = StreamConfiguration.builder()
                .scope(scope)
                .streamName(stream)
                .scalingPolicy(scalingPolicy)
                .build();
        controller.createStream(config).join();
    }
}
