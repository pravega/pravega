/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.bucket;

import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import java.util.concurrent.ScheduledExecutorService;

public class PravegaTablesStoreRetentionTest extends BucketServiceTest {
    private TestingServer zkServer;
    private CuratorFramework zkClient;

    @Override
    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 1000,
                (r, e, s) -> false);

        zkClient.start();
        super.setup();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        streamMetadataStore.close();
        zkClient.close();
        zkServer.close();
    }

    @Override
    StreamMetadataStore createStreamStore(ScheduledExecutorService executor) {
        return StreamStoreFactory.createPravegaTablesStore(SegmentHelperMock.getSegmentHelperMockForTables(executor), 
                GrpcAuthHelper.getDisabledAuthHelper(), zkClient, executor);
    }

    @Override
    BucketStore createBucketStore(int bucketCount) {
        return StreamStoreFactory.createZKBucketStore(bucketCount, zkClient, executor);
    }
}
