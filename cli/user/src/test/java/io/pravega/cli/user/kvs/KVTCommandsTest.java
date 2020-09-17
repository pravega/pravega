/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user.kvs;

import io.pravega.cli.user.AbstractUserCommandTest;
import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.shared.NameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class KVTCommandsTest extends AbstractUserCommandTest {

    @Test(timeout = 10000)
    public void testCreateKVT() {
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList("createKVTable"), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();

        commandArgs = new CommandArgs(Arrays.asList("createKVTable/kvt1", "createKVTable/kvt2", "createKVTable/kvt3"), CONFIG.get());
        new KeyValueTableCommand.Create(commandArgs).execute();
        Assert.assertNotNull(KeyValueTableCommand.Create.descriptor());
    }

    @Test(timeout = 20000)
    public void testDeleteKVT() {
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList("deleteKVTable"), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();

        commandArgs = new CommandArgs(Collections.singletonList("deleteKVTable/kvt1"), CONFIG.get());
        new KeyValueTableCommand.Create(commandArgs).execute();

        new KeyValueTableCommand.Delete(commandArgs).execute();
        Assert.assertNotNull(KeyValueTableCommand.Delete.descriptor());
    }

    @Test(timeout = 10000)
    public void testListKVT() {
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList("listKVTable"), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();

        CommandArgs commandArgsCreate = new CommandArgs(Arrays.asList("listKVTable/kvt1", "listKVTable/kvt2", "listKVTable/kvt3"), CONFIG.get());
        new KeyValueTableCommand.Create(commandArgsCreate).execute();

        new KeyValueTableCommand.ListKVTables(commandArgs).execute();
        Assert.assertNotNull(KeyValueTableCommand.ListKVTables.descriptor());
    }

    @Test(timeout = 60000)
    public void testPutAndGetKVT() throws Exception {
        String scope = "putAndGetKVTable";
        String scopedStreamName = NameUtils.getScopedStreamName(scope, "kvt1");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();
        commandArgs = new CommandArgs(Collections.singletonList(scopedStreamName), CONFIG.get());
        new KeyValueTableCommand.Create(commandArgs).execute();

        // Exercise puts first.
        commandArgs = new CommandArgs(Arrays.asList(scopedStreamName, "key-family-1", "key1", "value1"), CONFIG.get());
        new KeyValueTableCommand.Put(commandArgs).execute();
        Assert.assertNotNull(KeyValueTableCommand.Put.descriptor());

        commandArgs = new CommandArgs(Arrays.asList(scopedStreamName, "key-family-1", "key1", "1:1", "value1"), CONFIG.get());
        new KeyValueTableCommand.PutIf(commandArgs).execute();
        Assert.assertNotNull(KeyValueTableCommand.PutIf.descriptor());

        commandArgs = new CommandArgs(Arrays.asList(scopedStreamName, "key-family-1", "key2", "value1"), CONFIG.get());
        new KeyValueTableCommand.PutIfAbsent(commandArgs).execute();
        Assert.assertNotNull(KeyValueTableCommand.PutIfAbsent.descriptor());

        commandArgs = new CommandArgs(Arrays.asList(scopedStreamName, "key-family-1", "{[[key1, value1]]}"), CONFIG.get());
        new KeyValueTableCommand.PutAll(commandArgs).execute();
        Assert.assertNotNull(KeyValueTableCommand.PutAll.descriptor());

        commandArgs = new CommandArgs(Arrays.asList(scopedStreamName, "key-family-1", "1", "2"), CONFIG.get());
        new KeyValueTableCommand.PutRange(commandArgs).execute();
        Assert.assertNotNull(KeyValueTableCommand.PutRange.descriptor());

        // Exercise list commands.
        commandArgs = new CommandArgs(Arrays.asList(scopedStreamName, "key-family-1"), CONFIG.get());
        new KeyValueTableCommand.ListKeys(commandArgs).execute();
        Assert.assertNotNull(KeyValueTableCommand.ListKeys.descriptor());

        commandArgs = new CommandArgs(Arrays.asList(scopedStreamName, "key-family-1"), CONFIG.get());
        new KeyValueTableCommand.ListEntries(commandArgs).execute();
        Assert.assertNotNull(KeyValueTableCommand.ListEntries.descriptor());

        // Exercise Remove commands.
        commandArgs = new CommandArgs(Arrays.asList(scopedStreamName, "key-family-1", "{[[key1]]}"), CONFIG.get());
        new KeyValueTableCommand.Remove(commandArgs).execute();
        Assert.assertNotNull(KeyValueTableCommand.Remove.descriptor());
    }

}
