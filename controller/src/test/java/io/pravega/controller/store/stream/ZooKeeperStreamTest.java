/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;

import java.time.Duration;

public class ZooKeeperStreamTest extends StreamTestBase {

    private TestingServer zkServer;
    private CuratorFramework cli;
    private ZKStoreHelper storeHelper;
    private ZKStreamMetadataStore store;

    @Override
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
        storeHelper = new ZKStoreHelper(cli, executor);
        store = new ZKStreamMetadataStore(cli, executor, Duration.ofSeconds(1));
    }

    @Override
    public void tearDown() throws Exception {
        cli.close();
        zkServer.close();
        store.close();
        executor.shutdown();
    }

    @Override
    void createScope(String scope) {
        store.createScope(scope).join();
    }

    @Override
    PersistentStreamBase getStream(String scope, String stream, int chunkSize, int shardSize) {
        return new ZKStream(scope, stream, storeHelper, () -> 0, chunkSize, shardSize, executor);
    }
}
