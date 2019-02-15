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

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;

import java.time.Duration;

import static org.mockito.Mockito.mock;

public class PravegaTablesStreamTest extends StreamTestBase {

    private TestingServer zkServer;
    private CuratorFramework cli;
    private PravegaTablesStreamMetadataStore store;
    private PravegaTablesStoreHelper storeHelper;
    
    @Override
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables();
        storeHelper = new PravegaTablesStoreHelper(segmentHelper);
        store = new PravegaTablesStreamMetadataStore(segmentHelper, cli, executor, Duration.ofSeconds(1));
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
        return new PravegaTablesStream(scope, stream, storeHelper, () -> 0, chunkSize, shardSize, executor);
    }
}
