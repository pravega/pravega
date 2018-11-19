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

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
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
    boolean restEnabled = true;
    boolean authEnabled = false;
    boolean tlsEnabled = false;
    LocalPravegaEmulator localPravega;

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
                                           .enableRestServer(restEnabled)
                                           .enableAuth(authEnabled)
                                           .enableTls(tlsEnabled)
                                           .certFile("../config/cert.pem")
                                           .keyFile("../config/key.pem")
                                           .jksKeyFile("../config/bookie.keystore.jks")
                                           .jksTrustFile("../config/bookie.truststore.jks")
                                           .keyPasswordFile("../config/bookie.keystore.jks.passwd")
                                           .passwdFile("../config/passwd")
                                           .userName("admin")
                                           .passwd("1111_aaaa")
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

        ClientConfig clientConfig = ClientConfig.builder()
                                                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))
                                                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                                                .trustStore("../config/cert.pem")
                                                .validateHostName(false)
                                                .build();
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);

        streamManager.createScope(scope);
        Assert.assertTrue("Stream creation is not successful ",
                streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                   .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                   .build()));
        log.info("Created stream: " + streamName);

        ClientFactory clientFactory = ClientFactory.withScope(scope, clientConfig);
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        log.info("Created writer for stream: " + streamName);

        writer.writeEvent("hello").get();
        log.info("Wrote data to the stream");
    }

    @After
    public void tearDown() throws Exception {
        if (localPravega != null) {
            localPravega.close();
        }
    }
}
