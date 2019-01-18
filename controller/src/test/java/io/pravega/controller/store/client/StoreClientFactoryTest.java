/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class StoreClientFactoryTest {
    TestingServer zkServer;
    
    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();
    }
    
    @After
    public void tearDown() throws IOException {
        zkServer.stop();
    }
    
    @Test
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
        AssertExtensions.assertThrows(KeeperException.SessionExpiredException.class, () -> client.getData().forPath("/test"));

        // after session expiration we should only ever get one attempt at retry
        // Note: curator is calling all retry loops thrice (so if we give retrycount as `N`, curator calls the retryPolicy
        // 3 * (N + 1) times. Hence we are getting expiration counter as `3` instead of `1`.
        assertEquals(3, expirationRetryCounter.get());
    }
}
