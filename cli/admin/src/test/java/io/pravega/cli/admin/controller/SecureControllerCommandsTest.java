/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.controller;

import io.pravega.cli.admin.AbstractTlsAdminCommandTest;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import lombok.Cleanup;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SecureControllerCommandsTest extends AbstractTlsAdminCommandTest {

    @Before
    @Override
    public void setUp() throws Exception {
        this.authEnabled = true;
        this.tlsEnabled = true;
        super.setUp();
    }


    @Test
    public void testListScopesCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller list-scopes", STATE.get());
        assertTrue(commandResult.contains("_system"));
    }

    @Test
    public void testListStreamsCommand() throws Exception {
        String scope = "testScope";
        String testStream = "testStream";
        ClientConfig clientConfig = prepareValidClientConfig();

        // Generate the scope and stream required for testing.
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        boolean isScopeCreated = streamManager.createScope(scope);

        // Check if scope created successfully.
        assertTrue("Failed to create scope", isScopeCreated);

        boolean isStreamCreated = streamManager.createStream(scope, testStream, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());

        // Check if stream created successfully.
        assertTrue("Failed to create the stream ", isStreamCreated);

        String commandResult = TestUtils.executeCommand("controller list-streams " + scope, STATE.get());
        assertTrue(commandResult.contains(testStream));
    }

    @Test
    public void testListReaderGroupsCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller list-readergroups _system", STATE.get());
        assertTrue(commandResult.contains("commitStreamReaders"));
    }

    @Test
    public void testDescribeScopeCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller describe-scope _system", STATE.get());
        assertTrue(commandResult.contains("_system"));
    }
}
