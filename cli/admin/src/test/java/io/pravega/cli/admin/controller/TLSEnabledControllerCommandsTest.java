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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TLSEnabledControllerCommandsTest extends AbstractTlsAdminCommandTest {

    @Before
    @Override
    public void setUp() throws Exception {
        tlsEnabled = true;
        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testAllCommands() throws Exception {
        String scope = "TLSEnabledScope";
        String testStream = "TLSEnabledStream";
        String readerGroup = UUID.randomUUID().toString().replace("-", "");
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, testStream))
                .disableAutomaticCheckpoints()
                .build();
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

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

        String commandResult = TestUtils.executeCommand("controller list-scopes", state.get());
        assertTrue("ListScopesCommand failed.", commandResult.contains(scope));
        assertNotNull(ControllerListScopesCommand.descriptor());

        commandResult = TestUtils.executeCommand("controller list-streams " + scope, state.get());
        assertTrue("ListStreamsCommand failed.", commandResult.contains(testStream));
        assertNotNull(ControllerListStreamsInScopeCommand.descriptor());

        commandResult = TestUtils.executeCommand("controller list-readergroups " + scope, state.get());
        assertTrue("ListReaderGroupsCommand failed.", commandResult.contains(readerGroup));
        assertNotNull(ControllerListReaderGroupsInScopeCommand.descriptor());

        commandResult = TestUtils.executeCommand("controller describe-scope " + scope, state.get());
        assertTrue("DescribeScopeCommand failed.", commandResult.contains(scope));
        assertNotNull(ControllerDescribeStreamCommand.descriptor());
    }
}
