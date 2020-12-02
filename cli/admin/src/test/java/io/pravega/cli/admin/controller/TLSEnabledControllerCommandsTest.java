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

import io.pravega.cli.admin.AbstractTlsAdminCommandTest;
import io.pravega.cli.admin.utils.TestUtils;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

public class TLSEnabledControllerCommandsTest extends AbstractTlsAdminCommandTest {

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
}
