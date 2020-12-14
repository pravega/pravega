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

import io.pravega.cli.admin.AbstractAdminCommandTest;
import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.TestUtils;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Properties;

public class SecureControllerCommandsTest extends AbstractAdminCommandTest {
    @BeforeClass
    public static void setUp() throws Exception {
        setUpCluster(true, true);
    }

    @Test
    @SneakyThrows
    public void testListScopesCommand() {
        String commandResult = TestUtils.executeCommand("controller list-scopes", STATE.get());
        Assert.assertTrue(commandResult.contains("_system"));
    }

    @Test
    @SneakyThrows
    public void testListStreamsCommand() {
        String commandResult = TestUtils.executeCommand("controller list-streams testScope", STATE.get());
        Assert.assertTrue(commandResult.contains("testStream"));
    }

    @Test
    @SneakyThrows
    public void testListReaderGroupsCommand() {
        String commandResult = TestUtils.executeCommand("controller list-readergroups _system", STATE.get());
        System.out.println(commandResult);
        Assert.assertTrue(commandResult.contains("commitStreamReaders"));
    }

    @Test
    @SneakyThrows
    public void testDescribeScopeCommand() {
        String commandResult = TestUtils.executeCommand("controller describe-scope _system", STATE.get());
        System.out.println(commandResult);
        Assert.assertTrue(commandResult.contains("_system"));
    }

    @Test
    @SneakyThrows
    public void testAuthConfig() {
        String scope = "testScope";
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.security.auth.enable", "true");
        pravegaProperties.setProperty("cli.security.auth.credentials.username", "admin");
        pravegaProperties.setProperty("cli.security.auth.credentials.password", "1111_aaaa");
        STATE.get().getConfigBuilder().include(pravegaProperties);
        String commandResult = TestUtils.executeCommand("controller list-scopes", STATE.get());
        // Check that both the new scope and the system one exist.
        Assert.assertTrue(commandResult.contains("_system"));
        Assert.assertTrue(commandResult.contains(scope));
        Assert.assertNotNull(ControllerListScopesCommand.descriptor());
        // Restore config
        pravegaProperties.setProperty("cli.security.auth.enable", "false");
        STATE.get().getConfigBuilder().include(pravegaProperties);

        // Exercise response codes for REST requests.
        @Cleanup
        val c1 = new AdminCommandState();
        CommandArgs commandArgs = new CommandArgs(Collections.emptyList(), c1);
        ControllerListScopesCommand command = new ControllerListScopesCommand(commandArgs);
        command.printResponseInfo(Response.status(Response.Status.UNAUTHORIZED).build());
        command.printResponseInfo(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
    }

    public static class AuthEnabledControllerCommandsTest extends SecureControllerCommandsTest {
        @BeforeClass
        public static void setUp() throws Exception {
            setUpCluster(true, false);
        }

        @Test
        @SneakyThrows
        public void testDescribeReaderGroupCommand() {
            // Check that the system reader group can be listed.
            String commandResult = TestUtils.executeCommand("controller describe-readergroup _system commitStreamReaders", STATE.get());
            Assert.assertTrue(commandResult.contains("commitStreamReaders"));
            Assert.assertNotNull(ControllerDescribeReaderGroupCommand.descriptor());
        }
    }

    public static class TLSEnabledControllerCommandsTest extends SecureControllerCommandsTest {
        @BeforeClass
        public static void setUp() throws Exception {
            setUpCluster(false, true);
        }
    }
}

