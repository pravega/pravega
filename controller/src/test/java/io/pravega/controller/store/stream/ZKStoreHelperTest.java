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

import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for ZKStoreHelper.
 */
public class ZKStoreHelperTest {
    //Ensure each test completes within 30 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    private TestingServer zkServer;
    private CuratorFramework cli;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private ZKStoreHelper zkStoreHelper;

    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryNTimes(0, 0));
        cli.start();
        zkStoreHelper = new ZKStoreHelper(cli, executor);
    }

    @After
    public void tearDown() throws IOException {
        executor.shutdownNow();
        cli.close();
        zkServer.close();
    }

    @Test
    public void testAddNode() throws ExecutionException, InterruptedException, IOException {
        Assert.assertNull(zkStoreHelper.addNode("/test/test1").get());
        AssertExtensions.assertThrows("Should throw NodeExistsException", zkStoreHelper.addNode("/test/test1"),
                e -> e instanceof StoreException.DataExistsException);
        zkServer.stop();
        AssertExtensions.assertThrows("Should throw UnknownException", zkStoreHelper.addNode("/test/test2"),
                e -> e instanceof StoreException.StoreConnectionException);
    }

    @Test
    public void testDeleteNode() throws ExecutionException, InterruptedException, IOException {
        Assert.assertNull(zkStoreHelper.addNode("/test/test1").get());

        Assert.assertNull(zkStoreHelper.addNode("/test/test1/test2").get());
        AssertExtensions.assertThrows("Should throw NodeNotEmptyException", zkStoreHelper.deleteNode("/test/test1"),
                e -> e instanceof StoreException.DataNotEmptyException);

        Assert.assertNull(zkStoreHelper.deleteNode("/test/test1/test2").get());

        Assert.assertNull(zkStoreHelper.deleteNode("/test/test1").get());
        AssertExtensions.assertThrows("Should throw NodeNotFoundException", zkStoreHelper.deleteNode("/test/test1"),
                e -> e instanceof StoreException.DataNotFoundException);
        zkServer.stop();
        AssertExtensions.assertThrows("Should throw UnknownException", zkStoreHelper.deleteNode("/test/test1"),
                e -> e instanceof StoreException.StoreConnectionException);
    }
}
