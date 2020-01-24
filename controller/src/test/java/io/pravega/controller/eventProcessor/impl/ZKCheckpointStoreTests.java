/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.eventProcessor.impl;

import io.pravega.client.stream.Position;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Predicate;

/**
 * Tests for Zookeeper based checkpoint store.
 */
public class ZKCheckpointStoreTests extends CheckpointStoreTests {

    private TestingServer zkServer;
    private CuratorFramework cli;

    @Override
    public void setupCheckpointStore() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 10000, new RetryOneTime(10000));
        cli.start();
        checkpointStore = CheckpointStoreFactory.createZKStore(cli);
    }

    @Override
    public void cleanupCheckpointStore() throws IOException {
        cli.close();
        zkServer.close();
    }

    @Test
    public void failingTests() {
        final String process1 = UUID.randomUUID().toString();
        final String readerGroup1 = UUID.randomUUID().toString();
        final String readerGroup2 = UUID.randomUUID().toString();
        final String reader1 = UUID.randomUUID().toString();
        cli.close();

        Predicate<Throwable> predicate = e -> e instanceof CheckpointStoreException && e.getCause() instanceof IllegalStateException;
        AssertExtensions.assertThrows("failed getProcesses", () -> checkpointStore.getProcesses(), predicate);

        AssertExtensions.assertThrows("failed addReaderGroup",
                () -> checkpointStore.addReaderGroup(process1, readerGroup1), predicate);

        AssertExtensions.assertThrows("failed getReaderGroups",
                () -> checkpointStore.getReaderGroups(process1), predicate);

        AssertExtensions.assertThrows("failed addReader",
                () -> checkpointStore.addReader(process1, readerGroup1, reader1), predicate);

        Position position = new PositionImpl(Collections.emptyMap());
        AssertExtensions.assertThrows("failed setPosition",
                () -> checkpointStore.setPosition(process1, readerGroup1, reader1, position), predicate);

        AssertExtensions.assertThrows("failed getPositions",
                () -> checkpointStore.getPositions(process1, readerGroup1), predicate);

        AssertExtensions.assertThrows("failed sealReaderGroup",
                () -> checkpointStore.sealReaderGroup(process1, readerGroup2), predicate);

        AssertExtensions.assertThrows("failed removeReader",
                () -> checkpointStore.removeReader(process1, readerGroup1, reader1), predicate);

        AssertExtensions.assertThrows("failed removeReaderGroup",
                () -> checkpointStore.removeReaderGroup(process1, readerGroup1), predicate);
    }
}

