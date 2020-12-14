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

import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.TestUtils;
import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.shared.NameUtils;
import io.pravega.test.integration.demo.ClusterWrapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static io.pravega.cli.user.TestUtils.createPravegaCluster;
import static io.pravega.cli.user.TestUtils.getCLIControllerUri;
import static io.pravega.cli.user.TestUtils.createCLIConfig;

public class AuthEnabledKVTCommandsTest {
    protected static final ClusterWrapper CLUSTER = createPravegaCluster(true, false);
    static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), true, false);

    @BeforeClass
    public static void start() {
        CLUSTER.start();
    }

    @AfterClass
    public static void shutDown() {
        if (CLUSTER != null) {
            CLUSTER.close();
        }
    }

    @Test(timeout = 10000)
    public void testCreateKVT() throws Exception {
        final String scope = "createKVTable";
        final String table = NameUtils.getScopedStreamName(scope, "kvt1");
        String commandResult = TestUtils.executeCommand("scope create " + scope, CONFIG);
        Assert.assertTrue(commandResult.contains("created successfully"));

        commandResult = TestUtils.executeCommand("kvt create " + table, CONFIG);
        Assert.assertTrue(commandResult.contains("created successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Create.descriptor());
    }

    @Test(timeout = 20000)
    public void testDeleteKVT() throws Exception {
        final String scope = "deleteKVTable";
        final String table = NameUtils.getScopedStreamName(scope, "kvt1");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG);
        new ScopeCommand.Create(commandArgs).execute();

        commandArgs = new CommandArgs(Collections.singletonList(table), CONFIG);
        new KeyValueTableCommand.Create(commandArgs).execute();

        String commandResult = TestUtils.executeCommand("kvt delete " + table, CONFIG);
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Delete.descriptor());
    }

    @Test(timeout = 10000)
    public void testListKVT() throws Exception {
        final String scope = "listKVTable";
        final String table = scope + "/kvt1";
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG);
        new ScopeCommand.Create(commandArgs).execute();

        CommandArgs commandArgsCreate = new CommandArgs(Collections.singletonList(table), CONFIG);
        new KeyValueTableCommand.Create(commandArgsCreate).execute();

        String commandResult = TestUtils.executeCommand("kvt list " + scope, CONFIG);
        Assert.assertTrue(commandResult.contains("kvt1"));
        Assert.assertNotNull(KeyValueTableCommand.ListKVTables.descriptor());
    }

    public static class SecureKVTCommandsTest extends AuthEnabledKVTCommandsTest {
        protected static final ClusterWrapper CLUSTER = createPravegaCluster(true, true);
        static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), true, true);

        @BeforeClass
        public static void start() {
            CLUSTER.start();
        }

        @AfterClass
        public static void shutDown() {
            if (CLUSTER != null) {
                CLUSTER.close();
            }
        }
    }
}
