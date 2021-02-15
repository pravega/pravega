/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.client;

import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class StoreClientFactoryTest extends ThreadPooledTestSuite {
    TestingServer zkServer;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();
    }

    @After
    public void tearDown() throws IOException {
        zkServer.stop();
    }
    
    @Test(timeout = 60000)
    public void testZkSessionExpiryRetry() throws Exception {
        CompletableFuture<Void> sessionExpiry = new CompletableFuture<>();
        AtomicInteger expirationRetryCounter = new AtomicInteger();

        Supplier<Boolean> canRetrySupplier = () -> {
            if (sessionExpiry.isDone()) {
                expirationRetryCounter.incrementAndGet();
            }

            return !sessionExpiry.isDone();
        };
        Consumer<Void> expirationHandler = x -> sessionExpiry.complete(null);

        CuratorFramework client = StoreClientFactory.createZKClient(ZKClientConfigImpl.builder().connectionString(zkServer.getConnectString())
                .namespace("test").maxRetries(10).initialSleepInterval(10).secureConnectionToZooKeeper(false).sessionTimeoutMs(15000).build(),
                canRetrySupplier, expirationHandler);
        
        client.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();
        
        sessionExpiry.join();

        // verify that we fail with session expiry and we fail without retrying.
        AssertExtensions.assertEventuallyThrows(KeeperException.SessionExpiredException.class, () -> client.getData().forPath("/test"),
                2000, 30000L);
        assertTrue(expirationRetryCounter.get() > 0);
    }

    /**
     * This test verifies that ZKClientFactory correctly handles the situation in which the Controller attempts to
     * create a new Zookeeper client with different parameters compared to the existing ones. We should not enable to
     * create a new Zookeeper client and, in fact, we close the existing one to force the restart of the Controller.
     * During the Controller restart, a new Zookeeper client will be created with the new parameters.
     */
    @Test
    public void testZkSessionExpiryWithChangedParameters() throws Exception {
        final String testZNode = "/test";

        CompletableFuture<Void> sessionExpiry = new CompletableFuture<>();
        AtomicInteger expirationRetryCounter = new AtomicInteger();

        Supplier<Boolean> canRetrySupplier = () -> {
            if (sessionExpiry.isDone()) {
                expirationRetryCounter.incrementAndGet();
            }

            return !sessionExpiry.isDone();
        };
        Consumer<Void> expirationHandler = x -> sessionExpiry.complete(null);

        StoreClientFactory.ZKClientFactory storeClientFactory = new StoreClientFactory.ZKClientFactory();
        CuratorFramework client = StoreClientFactory.createZKClient(ZKClientConfigImpl.builder()
                                                                                      .connectionString(zkServer.getConnectString())
                                                                                      .namespace("test")
                                                                                      .maxRetries(10)
                                                                                      .initialSleepInterval(10)
                                                                                      .secureConnectionToZooKeeper(false)
                                                                                      .sessionTimeoutMs(15000)
                                                                                      .build(),
                canRetrySupplier, expirationHandler, storeClientFactory);

        // Check that the client works correctly. In this case, it throws a NoNodeException when accessing a non-existent path.
        AssertExtensions.assertThrows(KeeperException.NoNodeException.class, () -> client.getData().forPath(testZNode));

        // Simulate an update of the connection parameters.
        storeClientFactory.setConnectString("changedConnectString");

        // Induce a session expiration, so we invoke newZooKeeper() and notice about the updated parameter.
        client.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();
        sessionExpiry.join();

        // Check that, once closed by the ZKClientFactory, the client throws an expected exception (SessionExpiredException).
        AssertExtensions.assertThrows(KeeperException.SessionExpiredException.class, () -> client.getData().forPath(testZNode));
    }
}
