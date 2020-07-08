/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

public class StreamRequestProcessorWithZkStore extends StreamRequestProcessorTest {
    private StreamMetadataStore store;
    private CuratorFramework client;
    private TestingServer zkServer;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();
        client = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
            new ExponentialBackoffRetry(200, 10, 5000));
        client.start();

    store = StreamStoreFactory.createZKStore(client, executorService());
    }

    @After
    public void tearDown() throws Exception {
        store.close();
        client.close();
        zkServer.close();
    }

    @Override
    StreamMetadataStore getStore() {
        return store;
    }
}
