/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.integrationtests;

import com.emc.pravega.StreamManager;
import com.emc.pravega.demo.ControllerWrapper;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.testcommon.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * Tests for validating controller fail over behaviour.
 */
@Slf4j
public class ControllerFailoverTest {
    private static final String SCOPE = "testScope";
    private static final String STREAM = "testStream";

    private final int servicePort = TestUtils.randomPort();
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;

    @Before
    public void setup() {
        // 1. Start ZK
        try {
            zkTestServer = new TestingServer();
        } catch (Exception e) {
            Assert.fail("Failed starting ZK test server");
        }

        // 2. Start Pravega SSS
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        try {
            serviceBuilder.initialize().get();
        } catch (Exception e) {
            Assert.fail("Failed starting Pravega host");
        }
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();
    }

    @After
    public void cleanup() throws Exception {
        if (server != null) {
            server.close();
        }
        if (zkTestServer != null) {
            zkTestServer.close();
        }
    }

    @Test(timeout = 20000)
    public void testSessionExpiryTolerance() {
        final int controllerPort = TestUtils.randomPort();
        final String serviceHost = "localhost";
        final int containerCount = 4;
        final ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false, true,
                false, controllerPort, serviceHost, servicePort, containerCount);

        try {
            controllerWrapper.awaitRunning();
        } catch (IllegalStateException e) {
            log.error("Received interrupt while awaiting start of controllerWrapper", e);
            Assert.fail("Failed starting controllerWrapper");
            return;
        }

        // Simulate ZK session timeout
        try {
            controllerWrapper.forceClientSessionExpiry();
        } catch (Exception e) {
            log.error("Error while simulating client session expiry", e);
            Assert.fail();
        }

        // Now, that session has expired, lets do some operations on
        try {
            controllerWrapper.awaitPaused();
        } catch (IllegalStateException e) {
            log.error("Error waiting for starter termination", e);
            Assert.fail();
        }

        try {
            controllerWrapper.awaitRunning();
        } catch (IllegalStateException e) {
            log.error("Error waiting for starter ready", e);
            Assert.fail();
        }

        URI controllerURI = URI.create("tcp://localhost:" + controllerPort);
        StreamManager streamManager = StreamManager.create(controllerURI);

        // Create scope
        streamManager.createScope(SCOPE);

        // Create stream
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(STREAM)
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        streamManager.createStream(SCOPE, STREAM, streamConfiguration);

        streamManager.sealStream(SCOPE, STREAM);

        streamManager.deleteStream(SCOPE, STREAM);

        streamManager.deleteScope(SCOPE);

        try {
            controllerWrapper.close();
        } catch (Exception e) {
            log.error("Error closing controllerWrapper", e);
            Assert.fail();
        }

        controllerWrapper.awaitTerminated();
    }
}
