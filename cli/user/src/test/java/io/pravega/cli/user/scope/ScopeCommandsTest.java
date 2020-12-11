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

import io.pravega.cli.user.AbstractUserCommandTest;
import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

public class ScopeCommandsTest extends AbstractUserCommandTest {

    @Test(timeout = 5000)
    public void testCreateScope() throws Exception {
        final String scope = "testCreate";
        String commandResult = TestUtils.executeCommand("scope create " + scope, CONFIG.get());
        Assert.assertTrue(commandResult.contains("created successfully"));
        Assert.assertNotNull(ScopeCommand.Create.descriptor());
    }

    @Test(timeout = 5000)
    public void testDeleteScope() throws Exception {
        String scopeToDelete = "toDelete";
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scopeToDelete), CONFIG.get());
        Assert.assertNotNull(commandArgs.toString());
        new ScopeCommand.Create(commandArgs).execute();
        String commandResult = TestUtils.executeCommand("scope delete " + scopeToDelete, CONFIG.get());
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        Assert.assertNotNull(ScopeCommand.Delete.descriptor());
    }

    public static class AuthEnabledScopeCommandsTest extends ScopeCommandsTest {
        @BeforeClass
        public static void start() {
            setUpCluster(true, false);
        }
    }

    public static class TLSEnabledScopeCommandsTest extends ScopeCommandsTest {
        @BeforeClass
        public static void start() {
            setUpCluster(false, true);
        }
    }

    public static class SecureScopeCommandsTest extends ScopeCommandsTest {
        @BeforeClass
        public static void start() {
            setUpCluster(true, true);
        }
    }
}
