/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;

import lombok.Cleanup;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.concurrent.ExecutionException;

/**
 * Tests for TLS enabled standalone cluster.
 */
public class TlsEnabledInProcPravegaClusterTest extends InProcPravegaClusterTest {

    @Before
    @Override
    public void setUp() throws Exception {
        this.authEnabled = false;
        this.tlsEnabled = true;
        super.setUp();
    }

    @Test
    public void writeEventToClusterWithProperConfiguration() throws ExecutionException, InterruptedException {
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
        assertNotNull(streamManager);

        boolean isScopeCreated = streamManager.createScope(scope);
        assertTrue("Failed to create scope", isScopeCreated);

        boolean isStreamCreated = streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        Assert.assertTrue("Failed to create the stream ", isStreamCreated);

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("hello").get();
        // If the control reaches here, the write event operation was successful.
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }


}
