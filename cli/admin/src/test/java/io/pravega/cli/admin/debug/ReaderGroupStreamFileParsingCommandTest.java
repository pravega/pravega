/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.debug;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
import lombok.Cleanup;
import lombok.val;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ReaderGroupStreamFileParsingCommandTest {

    @Test
    public void testPasswordFileCreatorCommand() throws Exception {
        final String inputFileName = "input";
        final String outputFileName = "output";
        byte[] bytes = {0, 0, 0, 0, 0, 0, 0, 0};
        FileUtils.writeByteArrayToFile(new File(inputFileName), bytes);

        @Cleanup
        val c1 = new AdminCommandState();
        TestUtils.executeCommand("debug parse-rg-stream-file " + inputFileName + " " + outputFileName, c1);
        Assert.assertTrue(Files.exists(Paths.get(outputFileName)));
        // Remove generated file by command.
        Files.delete(Paths.get(inputFileName));
        Files.delete(Paths.get(outputFileName));
    }
}