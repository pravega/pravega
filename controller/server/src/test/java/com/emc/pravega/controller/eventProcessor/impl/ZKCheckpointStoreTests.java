/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.testcommon.ZKCuratorUtils;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.impl.PositionImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 * Tests for Zookeeper based checkpoint store.
 */
public class ZKCheckpointStoreTests extends CheckpointStoreTests {

    private TestingServer zkServer;
    private CuratorFramework cli;

    @Override
    public void setupCheckpointStore() throws Exception {
        zkServer = ZKCuratorUtils.createTestServer();
        zkServer.start();
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
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
}
