/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.eventProcessor.impl;

import io.pravega.test.common.TestingServerStarter;
import io.pravega.server.controller.service.store.checkpoint.CheckpointStoreException;
import io.pravega.server.controller.service.store.checkpoint.CheckpointStoreFactory;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.impl.PositionImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
    public void testIdempotency() {
        final String process1 = "process1";
        final String readerGroup1 = "rg1";
        final String reader = "reader";

        try {
            checkpointStore.addReaderGroup(process1, readerGroup1);
            checkpointStore.addReader(process1, readerGroup1, reader);
            checkpointStore.removeReader(process1, readerGroup1, reader);
            checkpointStore.removeReader(process1, readerGroup1, reader);
            checkpointStore.sealReaderGroup(process1, readerGroup1);
            checkpointStore.sealReaderGroup(process1, readerGroup1);
            checkpointStore.removeReaderGroup(process1, readerGroup1);

            try {
                checkpointStore.sealReaderGroup(process1, readerGroup1);
            } catch (CheckpointStoreException e) {
                assertEquals(e.getType(), CheckpointStoreException.Type.NoNode);
            }

            checkpointStore.removeReaderGroup(process1, readerGroup1);
        } catch (CheckpointStoreException e) {
            fail();
        }
    }

    @Test
    public void failingTests() {
        final String process1 = "process1";
        final String readerGroup1 = "rg1";
        final String readerGroup2 = "rg2";
        final String reader1 = "reader1";
        cli.close();

        try {
            checkpointStore.getProcesses();
            Assert.fail();
        } catch (CheckpointStoreException e) {
            Assert.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }

        try {
            checkpointStore.addReaderGroup(process1, readerGroup1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            Assert.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }

        try {
            checkpointStore.getReaderGroups(process1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            Assert.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }

        try {
            checkpointStore.addReader(process1, readerGroup1, reader1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            Assert.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }

        Position position = new PositionImpl(Collections.emptyMap());
        try {
            checkpointStore.setPosition(process1, readerGroup1, reader1, position);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            Assert.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }

        try {
            checkpointStore.getPositions(process1, readerGroup1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            Assert.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }

        try {
            checkpointStore.sealReaderGroup(process1, readerGroup2);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            Assert.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }

        try {
            checkpointStore.removeReader(process1, readerGroup1, reader1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            Assert.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }

        try {
            checkpointStore.removeReaderGroup(process1, readerGroup1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            Assert.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }
    }

    @Test
    public void connectivityFailureTests() throws IOException {
        final String process1 = "process1";
        final String readerGroup1 = "rg1";
        final String reader1 = "reader1";
        zkServer.close();

        try {
            checkpointStore.getProcesses();
            Assert.fail();
        } catch (CheckpointStoreException e) {
            assertEquals(e.getType(), CheckpointStoreException.Type.Connectivity);
        }

        try {
            checkpointStore.addReaderGroup(process1, readerGroup1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            assertEquals(e.getType(), CheckpointStoreException.Type.Connectivity);
        }

        try {
            checkpointStore.addReader(process1, readerGroup1, reader1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            assertEquals(e.getType(), CheckpointStoreException.Type.Connectivity);
        }

        try {
            checkpointStore.sealReaderGroup(process1, readerGroup1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            assertEquals(e.getType(), CheckpointStoreException.Type.Connectivity);
        }

        try {
            checkpointStore.removeReader(process1, readerGroup1, reader1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            assertEquals(e.getType(), CheckpointStoreException.Type.Connectivity);
        }

        try {
            checkpointStore.getPositions(process1, readerGroup1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            assertEquals(e.getType(), CheckpointStoreException.Type.Connectivity);
        }

        Position position = new PositionImpl(Collections.emptyMap());
        try {
            checkpointStore.setPosition(process1, readerGroup1, reader1, position);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            assertEquals(e.getType(), CheckpointStoreException.Type.Connectivity);
        }

        try {
            checkpointStore.removeReader(process1, readerGroup1, reader1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            assertEquals(e.getType(), CheckpointStoreException.Type.Connectivity);
        }

        try {
            checkpointStore.removeReaderGroup(process1, readerGroup1);
            Assert.fail();
        } catch (CheckpointStoreException e) {
            assertEquals(e.getType(), CheckpointStoreException.Type.Connectivity);
        }
    }
}

