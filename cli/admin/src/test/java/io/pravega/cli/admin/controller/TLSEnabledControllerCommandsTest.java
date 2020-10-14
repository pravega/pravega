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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Properties;

public class TLSEnabledControllerCommandsTest extends AbstractAdminCommandTest {

    @Before
    @Override
    public void setUp() throws Exception {
        SETUP_UTILS.setTlsEnabled(true);
        super.setUp();
    }


    @Test
    public void testListScopesCommand() throws Exception {
        SETUP_UTILS.createTestStream("testListScopesCommand", 2);
        String commandResult = TestUtils.executeCommand("controller list-scopes", STATE.get());
        // Check that both the new scope and the system one exist.
        Assert.assertTrue(commandResult.contains("_system"));
        Assert.assertTrue(commandResult.contains(SETUP_UTILS.getScope()));
        Assert.assertNotNull(ControllerListScopesCommand.descriptor());
    }

    @Test
    public void testDescribeScopeCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller describe-scope _system", STATE.get());
        Assert.assertTrue(commandResult.contains("_system"));
        Assert.assertNotNull(ControllerDescribeStreamCommand.descriptor());
    }

    @Test
    public void testListStreamsCommand() throws Exception {
        String testStream = "testListStreamsCommand";
        SETUP_UTILS.createTestStream(testStream, 1);
        String commandResult = TestUtils.executeCommand("controller list-streams " + SETUP_UTILS.getScope(), STATE.get());
        // Check that the newly created stream is retrieved as part of the list of streams.
        Assert.assertTrue(commandResult.contains(testStream));
        Assert.assertNotNull(ControllerListStreamsInScopeCommand.descriptor());
    }

    @Test
    public void testListReaderGroupsCommand() throws Exception {
        // Check that the system reader group can be listed.
        String commandResult = TestUtils.executeCommand("controller list-readergroups _system", STATE.get());
        Assert.assertTrue(commandResult.contains("commitStreamReaders"));
        Assert.assertNotNull(ControllerListReaderGroupsInScopeCommand.descriptor());
    }

    @Test
    public void testDescribeReaderGroupCommand() throws Exception {
        // Check that the system reader group can be listed.
        String commandResult = TestUtils.executeCommand("controller describe-readergroup _system commitStreamReaders", STATE.get());
        Assert.assertTrue(commandResult.contains("commitStreamReaders"));
        Assert.assertNotNull(ControllerDescribeReaderGroupCommand.descriptor());
    }

    @Test
    public void testAuthConfig() throws Exception {
        SETUP_UTILS.createTestStream("testListScopesCommand", 2);
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.security.auth.enable", "true");
        pravegaProperties.setProperty("cli.security.auth.credentials.username", "admin");
        pravegaProperties.setProperty("cli.security.auth.credentials.password", "1111_aaaa");
        STATE.get().getConfigBuilder().include(pravegaProperties);
        String commandResult = TestUtils.executeCommand("controller list-scopes", STATE.get());
        // Check that both the new scope and the system one exist.
        Assert.assertTrue(commandResult.contains("_system"));
        Assert.assertTrue(commandResult.contains(SETUP_UTILS.getScope()));
        Assert.assertNotNull(ControllerListScopesCommand.descriptor());
        // Restore config
        pravegaProperties.setProperty("cli.security.auth.enable", "false");
        STATE.get().getConfigBuilder().include(pravegaProperties);

        // Exercise response codes for REST requests.
        CommandArgs commandArgs = new CommandArgs(Collections.emptyList(), new AdminCommandState());
        ControllerListScopesCommand command = new ControllerListScopesCommand(commandArgs);
        command.printResponseInfo(Response.status(Response.Status.UNAUTHORIZED).build());
        command.printResponseInfo(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
    }
}
