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

import io.netty.util.ResourceLeakDetector;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.test.common.TestingServerStarter;
import lombok.Cleanup;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ServiceStarterTest {
    private ServiceBuilderConfig.Builder defaultConfigBuilder;
    private String zkUrl;
    private TestingServer zkTestServer;
    private ResourceLeakDetector.Level originalLevel;

    @Before
    public void setUp() throws Exception {
        originalLevel = ResourceLeakDetector.getLevel();
        zkTestServer = new TestingServerStarter().start();
        zkUrl = zkTestServer.getConnectString();
        defaultConfigBuilder = ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.ZK_URL, zkUrl));
    }

    @After
    public void tearDown() throws IOException {
        ResourceLeakDetector.setLevel(originalLevel);
        zkTestServer.close();
    }

    private ServiceBuilderConfig.Builder getDefaultConfigBuilder() {
        return this.defaultConfigBuilder.makeCopy();
    }

    /**
     * Check that the client created by ServiceStarter can correctly connect to a Zookeeper server using the custom
     * Zookeeper client factory.
     */
    @Test
    public void testCuratorClientCreation() throws Exception {
        ServiceStarter serviceStarter = new ServiceStarter(getDefaultConfigBuilder().build());
        CuratorFramework zkClient = serviceStarter.createZKClient();
        zkClient.blockUntilConnected();
        Assert.assertTrue(zkClient.getZookeeperClient().isConnected());
    }

    /**
     * Tests {@link ServiceStarter#start()} and {@link ServiceStarter#shutdown()}.
     */
    @Test
    public void testStartShutdown() throws Exception {
        val config = getDefaultConfigBuilder()
                .include(ServiceConfig.builder().with(ServiceConfig.NETTY_LEAK_DETECTION_ENABLED, true))
                .build();

        @Cleanup("shutdown")
        ServiceStarter serviceStarter = new ServiceStarter(config);
        serviceStarter.start();
        val levelStarted = ResourceLeakDetector.getLevel();
        Assert.assertEquals("Unexpected ResourceLeakDetector.Level when running.", ResourceLeakDetector.Level.PARANOID, levelStarted);

        serviceStarter.shutdown();
        val levelShutdown = ResourceLeakDetector.getLevel();
        Assert.assertEquals("Unexpected ResourceLeakDetector.Level when shutdown.", originalLevel, levelShutdown);
    }
}
