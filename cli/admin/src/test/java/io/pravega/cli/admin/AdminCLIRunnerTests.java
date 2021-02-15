/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin;

import java.io.IOException;
import java.util.Arrays;
import lombok.Cleanup;
import lombok.val;
import org.junit.Test;

public class AdminCLIRunnerTests {

    @Test
    public void testWrongExecCommandInputs() throws IOException {
        val commands = Arrays.asList("", "fakecommand", "scope fakeoption", "help", "controller describe-scope 1 2 3");
        for (val cmd : commands) {
            @Cleanup
            val state = new AdminCommandState();
            AdminCLIRunner.processCommand(cmd, state);
        }
    }

    @Test
    public void testDoMain() throws IOException {
        AdminCLIRunner.doMain(new String[]{"scope", "wrongCommand"});
    }

    @Test
    public void testCommandDetails() {
        AdminCLIRunner.printCommandDetails(Parser.parse("controller describe-scope"));
        AdminCLIRunner.printCommandDetails(Parser.parse("wrong command"));
    }

}
