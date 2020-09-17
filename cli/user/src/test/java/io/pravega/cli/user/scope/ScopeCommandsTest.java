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
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class ScopeCommandsTest extends AbstractUserCommandTest {

    @Test(timeout = 5000)
    public void testCreateScope() {
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList("testScope"), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();
        Assert.assertNotNull(ScopeCommand.Create.descriptor());
    }

    @Test(timeout = 5000)
    public void testDeleteScope() {
        String scopeToDelete = "toDelete";
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scopeToDelete), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();
        new ScopeCommand.Delete(commandArgs).execute();
        Assert.assertNotNull(ScopeCommand.Delete.descriptor());
    }
}
