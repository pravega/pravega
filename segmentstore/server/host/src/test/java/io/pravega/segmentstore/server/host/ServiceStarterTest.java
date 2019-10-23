/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ServiceStarterTest {

    private String zkUrl;
    private TestingServer zkTestServer;

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServerStarter().start();
        zkUrl = zkTestServer.getConnectString();
    }

    @After
    public void stopZookeeper() throws IOException {
        zkTestServer.close();
    }

    /**
     * Check that the client created by ServiceStarter can correctly connect to a Zookeeper server using the custom
     * Zookeeper client factory.
     *
     * @throws Exception
     */
    @Test
    public void testCuratorClientCreation() throws Exception {
        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.ZK_URL, zkUrl));
        ServiceStarter serviceStarter = new ServiceStarter(configBuilder.build());
        CuratorFramework zkClient = serviceStarter.createZKClient();
        zkClient.blockUntilConnected();
        Assert.assertTrue(zkClient.getZookeeperClient().isConnected());
    }
}
