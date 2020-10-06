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

import org.junit.Test;

import java.io.IOException;

public class AdminCLIRunnerTests {

    @Test
    public void testWrongExecCommandInputs() throws IOException {
        AdminCLIRunner.processCommand("", new AdminCommandState());
        AdminCLIRunner.processCommand("fakecommand", new AdminCommandState());
        AdminCLIRunner.processCommand("scope fakeoption", new AdminCommandState());
        AdminCLIRunner.processCommand("help", new AdminCommandState());
        AdminCLIRunner.processCommand("controller describe-scope 1 2 3", new AdminCommandState());
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
