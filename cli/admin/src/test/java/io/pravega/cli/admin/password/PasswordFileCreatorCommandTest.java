/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.password;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

public class PasswordFileCreatorCommandTest {

    @Test
    public void testPasswordFileCreatorCommand() throws Exception {
        final String fileName = "passwordFileTest";
        TestUtils.executeCommand("password create-password-file " + fileName + " user:password:acl", new AdminCommandState());
        Assert.assertTrue(Files.exists(Paths.get(fileName)));
        // Remove generated file by command.
        Files.delete(Paths.get(fileName));
    }
}
