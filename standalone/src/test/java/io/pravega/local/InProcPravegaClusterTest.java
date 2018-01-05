/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.PravegaDefaultCredentials;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import java.net.URI;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for standalone
 */
@Slf4j
public class InProcPravegaClusterTest {
    boolean authEnabled = false;
    private LocalPravegaEmulator localPravega;

    @Before
    public void setUp() throws Exception {
        ServiceBuilderConfig config = ServiceBuilderConfig
                .builder()
                .include(System.getProperties())
                .build();
        SingleNodeConfig conf = config.getConfig(SingleNodeConfig::builder);

        localPravega = LocalPravegaEmulator.builder()
                                           .controllerPort(TestUtils.getAvailableListenPort())
                                           .segmentStorePort(TestUtils.getAvailableListenPort())
                                           .zkPort(TestUtils.getAvailableListenPort())
                                           .restServerPort(TestUtils.getAvailableListenPort())
                                           .enableAuth(authEnabled)
                                           .build();
        localPravega.start();
    }

    /**
     * Create the test stream.
     *
     * @throws Exception on any errors.
     */
    @Test
    public void createTestStream()
            throws Exception {
        Assert.assertNotNull("Pravega not initialized", localPravega);
        String scope = "Scope";
        String streamName = "Stream";
        int numSegments = 10;

        System.setProperty("io.pravega.auth.enabled", String.valueOf(authEnabled));
        System.setProperty("io.pravega.auth.certFile",  "../config/cert.pem");

        @Cleanup
        StreamManager streamManager = StreamManager.create(URI.create(
                localPravega.getInProcPravegaCluster().getControllerURI()),
                new PravegaDefaultCredentials("1111_aaaa", "admin"));

        streamManager.createScope(scope);
        Assert.assertTrue("Stream creation is not successful ",
                streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                   .scope(scope)
                                   .streamName(streamName)
                                   .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                   .build()));

        log.info("Created stream: " + streamName);
    }

    @After
    public void tearDown() throws Exception {
        if (localPravega != null) {
            localPravega.close();
        }
    }
}
