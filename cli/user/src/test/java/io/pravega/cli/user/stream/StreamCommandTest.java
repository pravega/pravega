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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

public class StreamCommandTest extends AbstractUserCommandTest {

    @Test(timeout = 5000)
    public void testCreateStream() throws Exception {
        String scope = "createStreamScope";
        String stream = NameUtils.getScopedStreamName(scope, "newStream");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();

        String commandResult = TestUtils.executeCommand("stream create " + stream, CONFIG.get());
        Assert.assertTrue(commandResult.contains("created successfully"));
        Assert.assertNotNull(StreamCommand.Create.descriptor());
    }

    @Test(timeout = 5000)
    public void testDeleteStream() throws Exception {
        String scope = "deleteStreamScope";
        String stream = NameUtils.getScopedStreamName(scope, "deleteStream");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();

        commandArgs = new CommandArgs(Collections.singletonList(stream), CONFIG.get());
        new StreamCommand.Create(commandArgs).execute();

        String commandResult = TestUtils.executeCommand("stream delete " + stream, CONFIG.get());
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        Assert.assertNotNull(StreamCommand.Delete.descriptor());
    }

    @Test(timeout = 5000)
    public void testListStream() throws Exception {
        String scope = "listStreamScope";
        String stream = NameUtils.getScopedStreamName(scope, "theStream");
        CommandArgs commandArgsScope = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgsScope).execute();

        // List Streams in scope when it is empty.
        String commandResult = TestUtils.executeCommand("stream list " + scope, CONFIG.get());
        Assert.assertFalse(commandResult.contains("theStream"));

        CommandArgs commandArgsStream = new CommandArgs(Collections.singletonList(stream), CONFIG.get());
        new StreamCommand.Create(commandArgsStream).execute();

        // List Streams in scope when we have one.
        commandResult = TestUtils.executeCommand("stream list " + scope, CONFIG.get());
        Assert.assertTrue(commandResult.contains("theStream"));
        Assert.assertNotNull(StreamCommand.List.descriptor());
    }

    @Test(timeout = 20000)
    public void testAppendAndReadStream() throws Exception {
        String scope = "appendAndReadStreamScope";
        String stream = NameUtils.getScopedStreamName(scope, "appendAndReadStream");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();

        CommandArgs commandArgsStream = new CommandArgs(Collections.singletonList(stream), CONFIG.get());
        new StreamCommand.Create(commandArgsStream).execute();

        String commandResult = TestUtils.executeCommand("stream append " + stream + " 100", CONFIG.get());
        Assert.assertTrue(commandResult.contains("Done"));
        Assert.assertNotNull(StreamCommand.Append.descriptor());

        // Need to use a timeout for readers, otherwise the test never completes.
        commandResult = TestUtils.executeCommand("stream read " + stream + " true 5", CONFIG.get());
        Assert.assertTrue(commandResult.contains("Done"));

        commandResult = TestUtils.executeCommand("stream append " + stream + " key 100", CONFIG.get());
        Assert.assertTrue(commandResult.contains("Done"));
        commandResult = TestUtils.executeCommand("stream read " + stream + " 5", CONFIG.get());
        Assert.assertTrue(commandResult.contains("Done"));
        Assert.assertNotNull(StreamCommand.Read.descriptor());
    }

    public static class AuthEnabledStreamCommandsTest extends StreamCommandTest {
        @BeforeClass
        public static void start() {
            setUpCluster(true, false);
        }
    }

    public static class TLSEnabledStreamCommandsTest extends StreamCommandTest {
        @BeforeClass
        public static void start() {
            setUpCluster(false, true);
        }
    }

    public static class SecureStreamCommandsTest extends StreamCommandTest {
        @BeforeClass
        public static void start() {
            setUpCluster(true, true);
        }
    }
}
