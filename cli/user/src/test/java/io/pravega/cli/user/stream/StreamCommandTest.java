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
import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.shared.NameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class StreamCommandTest extends AbstractUserCommandTest {

    @Test(timeout = 5000)
    public void testCreateStream() {
        String scope = "createStreamScope";
        String stream = "newStream";
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();
        commandArgs = new CommandArgs(Collections.singletonList(NameUtils.getScopedStreamName(scope, stream)), CONFIG.get());
        new StreamCommand.Create(commandArgs).execute();
        Assert.assertNotNull(StreamCommand.Create.descriptor());
    }

    @Test(timeout = 5000)
    public void testDeleteStream() {
        String scope = "deleteStreamScope";
        String stream = "deleteStream";
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();
        commandArgs = new CommandArgs(Collections.singletonList(NameUtils.getScopedStreamName(scope, stream)), CONFIG.get());
        new StreamCommand.Create(commandArgs).execute();
        new StreamCommand.Delete(commandArgs).execute();
        Assert.assertNotNull(StreamCommand.Delete.descriptor());
    }

    @Test(timeout = 5000)
    public void testListStream() {
        String scope = "listStreamScope";
        String stream = "listStream";
        CommandArgs commandArgsScope = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgsScope).execute();

        // List Streams in scope when it is empty.
        new StreamCommand.List(commandArgsScope).execute();

        CommandArgs commandArgsStream = new CommandArgs(Collections.singletonList(NameUtils.getScopedStreamName(scope, stream)), CONFIG.get());
        new StreamCommand.Create(commandArgsStream).execute();

        // List Streams in scope when we have one.
        new StreamCommand.List(commandArgsScope).execute();
        Assert.assertNotNull(StreamCommand.List.descriptor());
    }

    @Test(timeout = 20000)
    public void testAppendAndReadStream() throws Exception {
        String scope = "appendAndReadStreamScope";
        String stream = "appendAndReadStream";
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();

        CommandArgs commandArgsStream = new CommandArgs(Collections.singletonList(NameUtils.getScopedStreamName(scope, stream)), CONFIG.get());
        new StreamCommand.Create(commandArgsStream).execute();

        CommandArgs commandArgsWrite = new CommandArgs(Arrays.asList(NameUtils.getScopedStreamName(scope, stream), "100"), CONFIG.get());
        new StreamCommand.Append(commandArgsWrite).execute();
        Assert.assertNotNull(StreamCommand.Append.descriptor());

        // Need to use a timeout for readers, otherwise the test never completes.
        CommandArgs commandArgsRead = new CommandArgs(Arrays.asList(NameUtils.getScopedStreamName(scope, stream), "true", "5"), CONFIG.get());
        new StreamCommand.Read(commandArgsRead).execute();
        Assert.assertNotNull(StreamCommand.Read.descriptor());
    }
}
