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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.test.common.AssertExtensions;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for secure standalone cluster.
 */
@Slf4j
public class SecurePravegaClusterTest extends InProcPravegaClusterTest {
    @Rule
    public Timeout globalTimeout = new Timeout(300, TimeUnit.SECONDS);

    @Before
    @Override
    public void setUp() throws Exception {
        this.authEnabled = true;
        this.tlsEnabled = true;
        super.setUp();
    }

    /**
     * Create the test stream.
     *
     * @throws Exception on any errors.
     */
    @Test
    public void failingCreateTestStream()
            throws Exception {
        Assert.assertNotNull("Pravega not initialized", localPravega);
        String scope = "Scope";
        String streamName = "Stream";
        int numSegments = 10;

        ClientConfig clientConfig = ClientConfig.builder()
                                                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))
                                                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                                                .validateHostName(false)
                                                .build();
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);

        AssertExtensions.assertThrows(RuntimeException.class,
                () -> streamManager.createScope(scope));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }
}
