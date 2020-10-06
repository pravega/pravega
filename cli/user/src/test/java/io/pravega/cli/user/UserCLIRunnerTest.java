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
import org.junit.Test;

public class UserCLIRunnerTest {

    @Test
    public void testWrongExecCommandInputs() {
        UserCLIRunner.processCommand("", InteractiveConfig.builder().build());
        UserCLIRunner.processCommand("fakecommand", InteractiveConfig.builder().build());
        UserCLIRunner.processCommand("scope fakeoption", InteractiveConfig.builder().build());
        UserCLIRunner.processCommand("help", InteractiveConfig.builder().build());
        UserCLIRunner.processCommand("scope create 1 2 3", InteractiveConfig.builder().build());
    }

    @Test
    public void testDoMain() {
        UserCLIRunner.doMain(new String[]{"scope", "wrongCommand"});
    }

    @Test
    public void testCommandDetails() {
        UserCLIRunner.printCommandDetails(Parser.parse("kvt create test", InteractiveConfig.getDefault()));
        UserCLIRunner.printCommandDetails(Parser.parse("wrong command", InteractiveConfig.getDefault()));
    }
}
