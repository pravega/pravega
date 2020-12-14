/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user.scope;

import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.TestUtils;
import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.test.integration.demo.ClusterWrapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static io.pravega.cli.user.TestUtils.createPravegaCluster;
import static io.pravega.cli.user.TestUtils.getCLIControllerUri;
import static io.pravega.cli.user.TestUtils.createCLIConfig;

public class ScopeCommandsTest {

    private static final ClusterWrapper CLUSTER = createPravegaCluster(false, false);
    private static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), false, false);

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

    @Test(timeout = 5000)
    public void testCreateScope() throws Exception {
        final String scope = "testCreate";
        String commandResult = TestUtils.executeCommand("scope create " + scope, CONFIG);
        Assert.assertTrue(commandResult.contains("created successfully"));
        Assert.assertNotNull(ScopeCommand.Create.descriptor());
    }

    @Test(timeout = 5000)
    public void testDeleteScope() throws Exception {
        String scopeToDelete = "toDelete";
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scopeToDelete), CONFIG);
        Assert.assertNotNull(commandArgs.toString());
        new ScopeCommand.Create(commandArgs).execute();
        String commandResult = TestUtils.executeCommand("scope delete " + scopeToDelete, CONFIG);
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        Assert.assertNotNull(ScopeCommand.Delete.descriptor());
    }

    public static class AuthEnabledScopeCommandsTest extends ScopeCommandsTest {
        private static final ClusterWrapper CLUSTER = createPravegaCluster(true, false);
        private static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), true, false);

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

    public static class TLSEnabledScopeCommandsTest extends ScopeCommandsTest {
        protected static final ClusterWrapper CLUSTER = createPravegaCluster(false, true);
        protected static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), false, true);

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

    public static class SecureScopeCommandsTest extends ScopeCommandsTest {
        protected static final ClusterWrapper CLUSTER = createPravegaCluster(true, true);
        protected static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), true, true);

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
