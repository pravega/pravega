/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user.kvs;

import io.pravega.cli.user.AbstractUserCommandTest;
import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.TestUtils;
import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.shared.NameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

public class AuthEnabledKVTCommandsTest extends AbstractUserCommandTest {

    @BeforeClass
    public static void start() {
        setUpCluster(true, false);
    }

    @Test(timeout = 10000)
    public void testCreateKVT() throws Exception {
        final String scope = "createKVTable";
        final String table = NameUtils.getScopedStreamName(scope, "kvt1");
        String commandResult = TestUtils.executeCommand("scope create " + scope, CONFIG.get());
        Assert.assertTrue(commandResult.contains("created successfully"));

        commandResult = TestUtils.executeCommand("kvt create " + table, CONFIG.get());
        Assert.assertTrue(commandResult.contains("created successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Create.descriptor());
    }

    @Test(timeout = 20000)
    public void testDeleteKVT() throws Exception {
        final String scope = "deleteKVTable";
        final String table = NameUtils.getScopedStreamName(scope, "kvt1");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();

        commandArgs = new CommandArgs(Collections.singletonList(table), CONFIG.get());
        new KeyValueTableCommand.Create(commandArgs).execute();

        String commandResult = TestUtils.executeCommand("kvt delete " + table, CONFIG.get());
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Delete.descriptor());
    }

    @Test(timeout = 10000)
    public void testListKVT() throws Exception {
        final String scope = "listKVTable";
        final String table = scope + "/kvt1";
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();

        CommandArgs commandArgsCreate = new CommandArgs(Collections.singletonList(table), CONFIG.get());
        new KeyValueTableCommand.Create(commandArgsCreate).execute();

        String commandResult = TestUtils.executeCommand("kvt list " + scope, CONFIG.get());
        Assert.assertTrue(commandResult.contains("kvt1"));
        Assert.assertNotNull(KeyValueTableCommand.ListKVTables.descriptor());
    }

    public static class SecureKVTCommandsTest extends AuthEnabledKVTCommandsTest {
        @BeforeClass
        public static void start() {
            setUpCluster(true, true);
        }
    }
}
