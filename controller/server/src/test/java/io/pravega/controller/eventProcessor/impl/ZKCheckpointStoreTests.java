/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.eventProcessor.impl;

import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.stream.Position;
import io.pravega.stream.impl.PositionImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

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
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10, 10, new RetryOneTime(10));
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

        AssertExtensions.assertThrows("failed getProcesses", () -> checkpointStore.getProcesses(),
                e -> e instanceof CheckpointStoreException && e.getCause() instanceof IllegalStateException);

        AssertExtensions.assertThrows("failed addReaderGroup", () -> checkpointStore.addReaderGroup(process1, readerGroup1),
                e -> e instanceof CheckpointStoreException && e.getCause() instanceof IllegalStateException);

        AssertExtensions.assertThrows("failed getReaderGroups", () -> checkpointStore.getReaderGroups(process1),
                e -> e instanceof CheckpointStoreException && e.getCause() instanceof IllegalStateException);

        AssertExtensions.assertThrows("failed addReader", () -> checkpointStore.addReader(process1, readerGroup1, reader1),
                e -> e instanceof CheckpointStoreException && e.getCause() instanceof IllegalStateException);

        Position position = new PositionImpl(Collections.emptyMap());
        AssertExtensions.assertThrows("failed setPosition", () -> checkpointStore.setPosition(process1, readerGroup1, reader1, position),
                e -> e instanceof CheckpointStoreException && e.getCause() instanceof IllegalStateException);

        AssertExtensions.assertThrows("failed getPositions", () -> checkpointStore.getPositions(process1, readerGroup1),
                e -> e instanceof CheckpointStoreException && e.getCause() instanceof IllegalStateException);

        AssertExtensions.assertThrows("failed sealReaderGroup", () -> checkpointStore.sealReaderGroup(process1, readerGroup2),
                e -> e instanceof CheckpointStoreException && e.getCause() instanceof IllegalStateException);

        AssertExtensions.assertThrows("failed removeReader", () -> checkpointStore.removeReader(process1, readerGroup1, reader1),
                e -> e instanceof CheckpointStoreException && e.getCause() instanceof IllegalStateException);

        AssertExtensions.assertThrows("failed removeReaderGroup", () -> checkpointStore.removeReaderGroup(process1, readerGroup1),
                e -> e instanceof CheckpointStoreException && e.getCause() instanceof IllegalStateException);
    }

    @Test
    public void connectivityFailureTests() throws IOException {
        final String process1 = UUID.randomUUID().toString();
        final String readerGroup1 = UUID.randomUUID().toString();
        final String reader1 = UUID.randomUUID().toString();
        zkServer.close();

        AssertExtensions.assertThrows("failed getProcesses", () -> checkpointStore.getProcesses(),
                e -> e instanceof CheckpointStoreException && ((CheckpointStoreException) e).getType().equals(CheckpointStoreException.Type.Connectivity));

        AssertExtensions.assertThrows("failed addReaderGroup", () -> checkpointStore.addReaderGroup(process1, readerGroup1),
                e -> e instanceof CheckpointStoreException && ((CheckpointStoreException) e).getType().equals(CheckpointStoreException.Type.Connectivity));

        AssertExtensions.assertThrows("failed addReader", () -> checkpointStore.addReader(process1, readerGroup1, reader1),
                e -> e instanceof CheckpointStoreException && ((CheckpointStoreException) e).getType().equals(CheckpointStoreException.Type.Connectivity));

        AssertExtensions.assertThrows("failed sealReaderGroup", () -> checkpointStore.sealReaderGroup(process1, readerGroup1),
                e -> e instanceof CheckpointStoreException && ((CheckpointStoreException) e).getType().equals(CheckpointStoreException.Type.Connectivity));

        AssertExtensions.assertThrows("failed removeReader", () -> checkpointStore.removeReader(process1, readerGroup1, reader1),
                e -> e instanceof CheckpointStoreException && ((CheckpointStoreException) e).getType().equals(CheckpointStoreException.Type.Connectivity));

        AssertExtensions.assertThrows("failed getPositions", () -> checkpointStore.getPositions(process1, readerGroup1),
                e -> e instanceof CheckpointStoreException && ((CheckpointStoreException) e).getType().equals(CheckpointStoreException.Type.Connectivity));

        Position position = new PositionImpl(Collections.emptyMap());
        AssertExtensions.assertThrows("failed setPosition", () -> checkpointStore.setPosition(process1, readerGroup1, reader1, position),
                e -> e instanceof CheckpointStoreException && ((CheckpointStoreException) e).getType().equals(CheckpointStoreException.Type.Connectivity));

        AssertExtensions.assertThrows("failed removeReader", () -> checkpointStore.removeReader(process1, readerGroup1, reader1),
                e -> e instanceof CheckpointStoreException && ((CheckpointStoreException) e).getType().equals(CheckpointStoreException.Type.Connectivity));

        AssertExtensions.assertThrows("failed removeReaderGroup", () -> checkpointStore.removeReaderGroup(process1, readerGroup1),
                e -> e instanceof CheckpointStoreException && ((CheckpointStoreException) e).getType().equals(CheckpointStoreException.Type.Connectivity));
    }
}

