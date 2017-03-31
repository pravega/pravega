/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;

import java.io.IOException;

/**
 * Tests for Zookeeper based checkpoint store.
 */
public class ZKCheckpointStoreTests extends CheckpointStoreTests {

    private TestingServer zkServer;
    private CuratorFramework cli;

    @Override
    public void setupCheckpointStore() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
        checkpointStore = CheckpointStoreFactory.createZKStore(cli);
    }

    @Override
    public void cleanupCheckpointStore() throws IOException {
        zkServer.close();
        cli.close();
    }
}
