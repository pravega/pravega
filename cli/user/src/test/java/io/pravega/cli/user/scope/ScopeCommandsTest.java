/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user.scope;

import io.pravega.cli.user.AbstractUserCommandTest;
import io.pravega.cli.user.CommandArgs;
import org.junit.Test;

public class ScopeCommandsTest extends AbstractUserCommandTest {

    @Test
    public void testCreateScope() {
        CommandArgs commandArgs = new CommandArgs(null, CONFIG.get());
        new ScopeCommand.Create(commandArgs);
    }
}
