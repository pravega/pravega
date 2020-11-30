/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user;

import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.cli.user.stream.StreamCommand;
import io.pravega.shared.NameUtils;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SecureUserCommandsTest extends AbstractTlsUserCommandTest {
    @Before
    @Override
    public void setUp() {
        tlsEnabled = true;
        authEnabled = true;
        super.setUp();
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();
    }

    @Test
    @SneakyThrows
    public void testCommands() {
        final String scope = "SecureTestScope";
        final String streamName = "SecureTestStream";
        String stream = NameUtils.getScopedStreamName(scope, streamName);
        final String tableName = "SecureKvtTestTable";
        final String table = NameUtils.getScopedStreamName(scope, tableName);

        String commandResult = TestUtils.executeCommand("scope create " + scope, config.get());
        Assert.assertTrue("ScopeCreateCommand failed.", commandResult.contains("created successfully"));
        Assert.assertNotNull(ScopeCommand.Create.descriptor());

        // List Streams in scope when it is empty.
        commandResult = TestUtils.executeCommand("stream list " + scope, config.get());
        Assert.assertFalse("StreamListCommand failed.", commandResult.contains(streamName));

        commandResult = TestUtils.executeCommand("stream create " + stream, config.get());
        Assert.assertTrue("StreamCreateCommand failed.", commandResult.contains("created successfully"));
        Assert.assertNotNull(StreamCommand.Create.descriptor());

        // List Streams in scope when we have one.
        commandResult = TestUtils.executeCommand("stream list " + scope, config.get());
        Assert.assertTrue("StreamListCommand failed.", commandResult.contains(streamName));
        Assert.assertNotNull(StreamCommand.List.descriptor());

        commandResult = TestUtils.executeCommand("stream append " + stream + " 100", config.get());
        Assert.assertTrue("StreamAppendCommand failed.", commandResult.contains("Done"));
        Assert.assertNotNull(StreamCommand.Append.descriptor());

        commandResult = TestUtils.executeCommand("stream read " + stream + " true 5", config.get());
        Assert.assertTrue("StreamReadCommand failed.", commandResult.contains("Done"));

        commandResult = TestUtils.executeCommand("stream append " + stream + " key 100", config.get());
        Assert.assertTrue("StreamAppendCommand failed.", commandResult.contains("Done"));
        commandResult = TestUtils.executeCommand("stream read " + stream + " 5", config.get());
        Assert.assertTrue("StreamReadCommand failed.", commandResult.contains("Done"));
        Assert.assertNotNull(StreamCommand.Read.descriptor());

        commandResult = TestUtils.executeCommand("stream delete " + stream, config.get());
        Assert.assertTrue("StreamDeleteCommand failed.", commandResult.contains("deleted successfully"));
        Assert.assertNotNull(StreamCommand.Delete.descriptor());

        // TODO: Test KVT commands in the secure scenario (auth+TLS).
        // Cannot at this point due to the following issue:
        // Issue 5374: Updating a KeyValueTable throws an InvalidClaimException against standalone with auth and TLS enabled
        // https://github.com/pravega/pravega/issues/5374

        commandResult = TestUtils.executeCommand("scope delete " + scope, config.get());
        Assert.assertTrue("ScopeDeleteCommand failed.", commandResult.contains("deleted successfully"));
        Assert.assertNotNull(ScopeCommand.Delete.descriptor());
    }
}
