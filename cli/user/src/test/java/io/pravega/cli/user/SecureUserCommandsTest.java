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

import io.pravega.cli.user.kvs.KeyValueTableCommand;
import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.cli.user.stream.StreamCommand;
import io.pravega.shared.NameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SecureUserCommandsTest extends AbstractTlsUserCommandTest {
    @Before
    @Override
    public void setUp() throws Exception {
        tlsEnabled = true;
        authEnabled = true;
        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testCommands() throws Exception {
        final String scope = "SecureTestScope";
        final String streamName = "SecureTestStream";
        String stream = NameUtils.getScopedStreamName(scope, streamName);
        final String tableName = "SecureKvtTestTable";
        final String table = NameUtils.getScopedStreamName(scope, tableName);

        String commandResult = TestUtils.executeCommand("scope create " + scope, config.get());
        Assert.assertTrue("ScopeCreateCommand failed.", commandResult.contains("created successfully"));
        Assert.assertNotNull(ScopeCommand.Create.descriptor());

        // List Streams in scope when it is empty.
        commandResult = TestUtils.executeCommand("stream list " + scope, config.get());
        Assert.assertFalse("StreamListCommand failed.", commandResult.contains(streamName));

        commandResult = TestUtils.executeCommand("stream create " + stream, config.get());
        Assert.assertTrue("StreamCreateCommand failed.", commandResult.contains("created successfully"));
        Assert.assertNotNull(StreamCommand.Create.descriptor());

        // List Streams in scope when we have one.
        commandResult = TestUtils.executeCommand("stream list " + scope, config.get());
        Assert.assertTrue("StreamListCommand failed.", commandResult.contains(streamName));
        Assert.assertNotNull(StreamCommand.List.descriptor());

        commandResult = TestUtils.executeCommand("stream append " + stream + " 100", config.get());
        Assert.assertTrue("StreamAppendCommand failed.", commandResult.contains("Done"));
        Assert.assertNotNull(StreamCommand.Append.descriptor());

        commandResult = TestUtils.executeCommand("stream read " + stream + " true 5", config.get());
        Assert.assertTrue("StreamReadCommand failed.", commandResult.contains("Done"));

        commandResult = TestUtils.executeCommand("stream append " + stream + " key 100", config.get());
        Assert.assertTrue("StreamAppendCommand failed.", commandResult.contains("Done"));
        commandResult = TestUtils.executeCommand("stream read " + stream + " 5", config.get());
        Assert.assertTrue("StreamReadCommand failed.", commandResult.contains("Done"));
        Assert.assertNotNull(StreamCommand.Read.descriptor());

        commandResult = TestUtils.executeCommand("stream delete " + stream, config.get());
        Assert.assertTrue("StreamDeleteCommand failed.", commandResult.contains("deleted successfully"));
        Assert.assertNotNull(StreamCommand.Delete.descriptor());

        commandResult = TestUtils.executeCommand("kvt create " + table, config.get());
        Assert.assertTrue("KVTCreateCommand failed.", commandResult.contains("created successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Create.descriptor());

        commandResult = TestUtils.executeCommand("kvt list " + scope, config.get());
        Assert.assertTrue("KVTListCommand failed.", commandResult.contains(tableName));
        Assert.assertNotNull(KeyValueTableCommand.ListKVTables.descriptor());

        commandResult = TestUtils.executeCommand("kvt put " + table + " key-family-1 key1 value1", config.get());
        Assert.assertTrue("KVTPutCommand failed.", commandResult.contains("updated successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Put.descriptor());

        commandResult = TestUtils.executeCommand("kvt put-if " + table + " key-family-1 key1 0:0 value2", config.get());
        Assert.assertTrue("KVTPutIfCommand failed.", commandResult.contains("BadKeyVersionException"));
        Assert.assertNotNull(KeyValueTableCommand.PutIf.descriptor());

        commandResult = TestUtils.executeCommand("kvt put-if-absent " + table + " key-family-1 key2 value1", config.get());
        Assert.assertTrue("KVTPutIfAbsentCommand failed.", commandResult.contains("inserted successfully"));
        Assert.assertNotNull(KeyValueTableCommand.PutIfAbsent.descriptor());

        commandResult = TestUtils.executeCommand("kvt put-all " + table + " key-family-1 {[[key3, value2]]}", config.get());
        Assert.assertTrue("KVTPutAllCommand failed.", commandResult.contains("Updated"));
        Assert.assertNotNull(KeyValueTableCommand.PutAll.descriptor());

        commandResult = TestUtils.executeCommand("kvt put-range " + table + " key-family-1 1 2", config.get());
        Assert.assertTrue("KVTPutRangeCommand failed.", commandResult.contains("Bulk-updated"));
        Assert.assertNotNull(KeyValueTableCommand.PutRange.descriptor());

        // Exercise list commands.
        commandResult = TestUtils.executeCommand("kvt list-keys " + table + " key-family-1", config.get());
        Assert.assertTrue("KVTListKeysCommand failed.", commandResult.contains("key1"));
        Assert.assertNotNull(KeyValueTableCommand.ListKeys.descriptor());

        commandResult = TestUtils.executeCommand("kvt list-entries " + table + " key-family-1", config.get());
        Assert.assertTrue("KVTListEntriesCommand failed.", commandResult.contains("value1"));
        Assert.assertNotNull(KeyValueTableCommand.ListEntries.descriptor());

        // Exercise Get command.
        commandResult = TestUtils.executeCommand("kvt get " + table + " key-family-1 \"{[key1, key2]}\"", config.get());
        Assert.assertTrue("KVTGetCommand failed.", commandResult.contains("Get"));
        Assert.assertNotNull(KeyValueTableCommand.Get.descriptor());

        // Exercise Remove commands.
        commandResult = TestUtils.executeCommand("kvt remove " + table + " key-family-1 {[[key1]]}", config.get());
        Assert.assertTrue("KVTRemoveCommand failed.", commandResult.contains("Removed"));
        Assert.assertNotNull(KeyValueTableCommand.Remove.descriptor());

        commandResult = TestUtils.executeCommand("kvt delete " + table, config.get());
        Assert.assertTrue("KVTDeleteCommand failed.", commandResult.contains("deleted successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Delete.descriptor());

        commandResult = TestUtils.executeCommand("scope delete " + scope, config.get());
        Assert.assertTrue("ScopeDeleteCommand failed.", commandResult.contains("deleted successfully"));
        Assert.assertNotNull(ScopeCommand.Delete.descriptor());
    }
}
