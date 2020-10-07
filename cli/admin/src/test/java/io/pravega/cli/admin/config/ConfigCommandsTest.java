/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.config;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class ConfigCommandsTest {

    @Test
    public void testSetAndListConfigCommands() throws Exception {
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.rest.uri", "test");
        AdminCommandState adminCommandState = new AdminCommandState();
        adminCommandState.getConfigBuilder().include(pravegaProperties);
        String commandResult = TestUtils.executeCommand("config list", adminCommandState);
        Assert.assertTrue(commandResult.contains("cli.controller.rest.uri"));
        TestUtils.executeCommand("config set hello=world", adminCommandState);
        commandResult = TestUtils.executeCommand("config list", adminCommandState);
        Assert.assertTrue(commandResult.contains("hello=world"));
    }

}
