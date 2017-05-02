/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.task;

import io.pravega.test.common.TestingServerStarter;
import io.pravega.server.controller.service.store.task.TaskStoreFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Zk task metadata store tests.
 */
public class ZKTaskMetadataStoreTests extends TaskMetadataStoreTests {

    private TestingServer zkServer;
    private ScheduledExecutorService executor;
    private CuratorFramework cli;

    @Override
    public void setupTaskStore() throws Exception {
        executor = Executors.newScheduledThreadPool(10);
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
        taskMetadataStore = TaskStoreFactory.createZKStore(cli, executor);
    }

    @Override
    public void cleanupTaskStore() throws IOException {
        if (cli != null) {
            cli.close();
        }

        if (zkServer != null) {
            zkServer.close();
        }

        if (executor != null) {
            executor.shutdown();
        }
    }
}
