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
import io.pravega.cli.admin.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Validate basic controller commands.
 */
public class ControllerCommandsTest extends AbstractAdminCommandTest {

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

    /*@Test
    public void testDescribeStreamCommand() throws Exception {
        String testStream = "testDescribeStreamCommand";
        SETUP_UTILS.createTestStream(testStream, 1);
        String commandResult = TestUtils.executeCommand("controller describe-stream " +
                SETUP_UTILS.getScope() + " " + testStream, STATE.get());
        System.err.println(commandResult);
        Assert.assertNotNull(ControllerDescribeStreamCommand.descriptor());
    }*/

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

}
