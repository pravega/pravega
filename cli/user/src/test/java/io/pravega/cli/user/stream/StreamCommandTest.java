/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user.stream;

import io.pravega.cli.user.AbstractUserCommandTest;
import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.TestUtils;
import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.shared.NameUtils;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class StreamCommandTest extends AbstractUserCommandTest {

    @Test(timeout = 20000)
    @SneakyThrows
    public void testStreamCommands() {
        String scope = "testScope";
        String streamName = "testStream";
        String stream = NameUtils.getScopedStreamName(scope, streamName);
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), config.get());
        new ScopeCommand.Create(commandArgs).execute();

        // List Streams in scope when it is empty.
        String commandResult = TestUtils.executeCommand("stream list " + scope, config.get());
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

        // Need to use a timeout for readers, otherwise the test never completes.
        commandResult = TestUtils.executeCommand("stream read " + stream + " true 5", config.get());
        Assert.assertTrue("StreamReadCommand failed.", commandResult.contains("Done"));

        commandResult = TestUtils.executeCommand("stream append " + stream + " key 100", config.get());
        Assert.assertTrue("StreamAppendCommands failed.", commandResult.contains("Done"));
        commandResult = TestUtils.executeCommand("stream read " + stream + " 5", config.get());
        Assert.assertTrue("StreamReadCommand failed.", commandResult.contains("Done"));
        Assert.assertNotNull(StreamCommand.Read.descriptor());

        commandResult = TestUtils.executeCommand("stream delete " + stream, config.get());
        Assert.assertTrue("StreamDeleteCommand failed.", commandResult.contains("deleted successfully"));
        Assert.assertNotNull(StreamCommand.Delete.descriptor());

        commandArgs = new CommandArgs(Collections.singletonList(scope), config.get());
        new ScopeCommand.Delete(commandArgs).execute();
    }
}
