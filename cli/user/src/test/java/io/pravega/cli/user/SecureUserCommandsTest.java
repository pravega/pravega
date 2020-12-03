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
import io.pravega.cli.user.kvs.KeyValueTableCommand;
import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.cli.user.stream.StreamCommand;
import io.pravega.shared.NameUtils;
import io.pravega.test.integration.demo.ClusterWrapper;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.cli.user.TestUtils.createPravegaCluster;
import static io.pravega.cli.user.TestUtils.setInteractiveConfig;

public class SecureUserCommandsTest {

    protected static final AtomicReference<ClusterWrapper> CLUSTER = new AtomicReference<>();
    protected static final AtomicReference<InteractiveConfig> CONFIG = new AtomicReference<>();
    private static final String SCOPE = "SecureTestScope";
    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    public static void setUpCluster(boolean authEnabled, boolean tlsEnabled) {
        CLUSTER.set(createPravegaCluster(authEnabled, tlsEnabled));
        CLUSTER.get().initialize();
        setInteractiveConfig(CLUSTER.get().controllerUri().replace("tcp://", "").replace("tls://", ""),
                authEnabled, tlsEnabled, CONFIG);
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(SCOPE), CONFIG.get());
        new ScopeCommand.Create(commandArgs).execute();
    }

    @BeforeClass
    @SneakyThrows
    public static void start() {
        setUpCluster(true, true);
    }

    @AfterClass
    @SneakyThrows
    public static void stop() {
        val cluster = CLUSTER.getAndSet(null);
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test(timeout = 5000)
    @SneakyThrows
    public void testCreateScope() {
        final String scope = "testCreate";
        String commandResult = TestUtils.executeCommand("scope create " + scope, CONFIG.get());
        Assert.assertTrue(commandResult.contains("created successfully"));
        Assert.assertNotNull(ScopeCommand.Create.descriptor());
    }

    @Test(timeout = 5000)
    @SneakyThrows
    public void testDeleteScope() {
        String scopeToDelete = "toDelete";
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scopeToDelete), CONFIG.get());
        Assert.assertNotNull(commandArgs.toString());
        new ScopeCommand.Create(commandArgs).execute();
        String commandResult = TestUtils.executeCommand("scope delete " + scopeToDelete, CONFIG.get());
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        Assert.assertNotNull(ScopeCommand.Delete.descriptor());
    }

    @Test(timeout = 5000)
    @SneakyThrows
    public void testCreateStream() {
        String stream = NameUtils.getScopedStreamName(SCOPE, "newStream");

        String commandResult = TestUtils.executeCommand("stream create " + stream, CONFIG.get());
        Assert.assertTrue(commandResult.contains("created successfully"));
        Assert.assertNotNull(StreamCommand.Create.descriptor());
    }

    @Test(timeout = 5000)
    @SneakyThrows
    public void testDeleteStream() {
        String stream = NameUtils.getScopedStreamName(SCOPE, "deleteStream");

        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(stream), CONFIG.get());
        new StreamCommand.Create(commandArgs).execute();

        String commandResult = TestUtils.executeCommand("stream delete " + stream, CONFIG.get());
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        Assert.assertNotNull(StreamCommand.Delete.descriptor());
    }

    @Test(timeout = 5000)
    @SneakyThrows
    public void testListStream() {
        String scope = "listStreamScope";
        String stream = NameUtils.getScopedStreamName(scope, "theStream");
        CommandArgs commandArgsScope = new CommandArgs(Collections.singletonList(scope), CONFIG.get());
        new ScopeCommand.Create(commandArgsScope).execute();

        // List Streams in scope when it is empty.
        String commandResult = TestUtils.executeCommand("stream list " + scope, CONFIG.get());
        Assert.assertFalse(commandResult.contains("theStream"));

        CommandArgs commandArgsStream = new CommandArgs(Collections.singletonList(stream), CONFIG.get());
        new StreamCommand.Create(commandArgsStream).execute();

        // List Streams in scope when we have one.
        commandResult = TestUtils.executeCommand("stream list " + scope, CONFIG.get());
        Assert.assertTrue(commandResult.contains("theStream"));
        Assert.assertNotNull(StreamCommand.List.descriptor());
    }

    @Test(timeout = 20000)
    @SneakyThrows
    public void testAppendAndReadStream() {
        String stream = NameUtils.getScopedStreamName(SCOPE, "appendAndReadStream");

        CommandArgs commandArgsStream = new CommandArgs(Collections.singletonList(stream), CONFIG.get());
        new StreamCommand.Create(commandArgsStream).execute();

        String commandResult = TestUtils.executeCommand("stream append " + stream + " 100", CONFIG.get());
        Assert.assertTrue(commandResult.contains("Done"));
        Assert.assertNotNull(StreamCommand.Append.descriptor());

        // Need to use a timeout for readers, otherwise the test never completes.
        commandResult = TestUtils.executeCommand("stream read " + stream + " true 5", CONFIG.get());
        Assert.assertTrue(commandResult.contains("Done"));

        commandResult = TestUtils.executeCommand("stream append " + stream + " key 100", CONFIG.get());
        Assert.assertTrue(commandResult.contains("Done"));
        commandResult = TestUtils.executeCommand("stream read " + stream + " 5", CONFIG.get());
        Assert.assertTrue(commandResult.contains("Done"));
        Assert.assertNotNull(StreamCommand.Read.descriptor());
    }

    @Test(timeout = 10000)
    @SneakyThrows
    public void testCreateKeyValueTable() {
        final String table = NameUtils.getScopedStreamName(SCOPE, "kvt1");

        String commandResult = TestUtils.executeCommand("kvt create " + table, CONFIG.get());
        Assert.assertTrue(commandResult.contains("created successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Create.descriptor());
    }

    @Test(timeout = 20000)
    @SneakyThrows
    public void testDeleteKeyValueTable() {
        final String table = NameUtils.getScopedStreamName(SCOPE, "kvt2");

        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(table), CONFIG.get());
        new KeyValueTableCommand.Create(commandArgs).execute();

        String commandResult = TestUtils.executeCommand("kvt delete " + table, CONFIG.get());
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Delete.descriptor());
    }

    @Test(timeout = 10000)
    @SneakyThrows
    public void testListKeyValueTables() {
        final String table = NameUtils.getScopedStreamName(SCOPE, "kvt3");

        CommandArgs commandArgsCreate = new CommandArgs(Collections.singletonList(table), CONFIG.get());
        new KeyValueTableCommand.Create(commandArgsCreate).execute();

        String commandResult = TestUtils.executeCommand("kvt list " + SCOPE, CONFIG.get());
        Assert.assertTrue(commandResult.contains("kvt3"));
        Assert.assertNotNull(KeyValueTableCommand.ListKVTables.descriptor());
    }

    // TODO: Test KVT commands in the secure scenario (auth+TLS).
    // Cannot at this point due to the following issue:
    // Issue 5374: Updating a KeyValueTable throws an InvalidClaimException against standalone with auth and TLS enabled
    // https://github.com/pravega/pravega/issues/5374

    public static class AuthEnabledUserCommandsTest extends SecureUserCommandsTest {
        @BeforeClass
        public static void start() {
            setUpCluster(true, false);
        }
    }

    public static class TLSEnabledUserCommandsTest extends SecureUserCommandsTest {
        @BeforeClass
        public static void start() {
            setUpCluster(false, true);
        }

        @Test(timeout = 60000)
        @SneakyThrows
        public void testPutAndGetKeyValueTable() {
            final String table = NameUtils.getScopedStreamName(SCOPE, "kvt4");
            CommandArgs commandArgs = new CommandArgs(Collections.singletonList(table), CONFIG.get());
            new KeyValueTableCommand.Create(commandArgs).execute();

            // Exercise puts first.
            String commandResult = TestUtils.executeCommand("kvt put " + table + " key-family-1 key1 value1", CONFIG.get());
            Assert.assertTrue(commandResult.contains("updated successfully"));
            Assert.assertNotNull(KeyValueTableCommand.Put.descriptor());

            commandResult = TestUtils.executeCommand("kvt put-if " + table + " key-family-1 key1 0:0 value2", CONFIG.get());
            Assert.assertTrue(commandResult.contains("BadKeyVersionException"));
            Assert.assertNotNull(KeyValueTableCommand.PutIf.descriptor());

            commandResult = TestUtils.executeCommand("kvt put-if-absent " + table + " key-family-1 key2 value1", CONFIG.get());
            Assert.assertTrue(commandResult.contains("inserted successfully"));
            Assert.assertNotNull(KeyValueTableCommand.PutIfAbsent.descriptor());

            commandResult = TestUtils.executeCommand("kvt put-all " + table + " key-family-1 {[[key3, value2]]}", CONFIG.get());
            Assert.assertTrue(commandResult.contains("Updated"));
            Assert.assertNotNull(KeyValueTableCommand.PutAll.descriptor());

            commandResult = TestUtils.executeCommand("kvt put-range " + table + " key-family-1 1 2", CONFIG.get());
            Assert.assertTrue(commandResult.contains("Bulk-updated"));
            Assert.assertNotNull(KeyValueTableCommand.PutRange.descriptor());

            // Exercise list commands.
            commandResult = TestUtils.executeCommand("kvt list-keys " + table + " key-family-1", CONFIG.get());
            Assert.assertTrue(commandResult.contains("key1"));
            Assert.assertNotNull(KeyValueTableCommand.ListKeys.descriptor());

            commandResult = TestUtils.executeCommand("kvt list-entries " + table + " key-family-1", CONFIG.get());
            Assert.assertTrue(commandResult.contains("value1"));
            Assert.assertNotNull(KeyValueTableCommand.ListEntries.descriptor());

            // Exercise Get command.
            commandResult = TestUtils.executeCommand("kvt get " + table + " key-family-1 \"{[key1, key2]}\"", CONFIG.get());
            Assert.assertTrue(commandResult.contains("Get"));
            Assert.assertNotNull(KeyValueTableCommand.Get.descriptor());

            // Exercise Remove commands.
            commandResult = TestUtils.executeCommand("kvt remove " + table + " key-family-1 {[[key1]]}", CONFIG.get());
            Assert.assertTrue(commandResult.contains("Removed"));
            Assert.assertNotNull(KeyValueTableCommand.Remove.descriptor());
        }
    }
}
