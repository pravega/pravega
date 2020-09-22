/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user;


import io.pravega.cli.user.config.InteractiveConfig;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Class to contain convenient utilities for writing test cases.
 */
public final class TestUtils {

    /**
     * Invoke any command and get the result by using a mock PrintStream object (instead of System.out). The returned
     * String is the output written by the Command that can be check in any test.
     *
     * @param inputCommand Command to execute.
     * @param config       Configuration to execute the command.
     * @return             Output of the command.
     * @throws Exception   If a problem occurs.
     */
    public static String executeCommand(String inputCommand, InteractiveConfig config) throws Exception {
        Parser.Command pc = Parser.parse(inputCommand, config);
        CommandArgs args = new CommandArgs(pc.getArgs().getArgs(), config);
        Command cmd = Command.Factory.get(pc.getComponent(), pc.getName(), args);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, "UTF-8")) {
            assert cmd != null;
            cmd.setOut(ps);
            cmd.execute();
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }
}