/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import io.pravega.test.common.TestingServerStarter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;

import java.io.IOException;
import java.util.UUID;

/**
 * ControllerServiceStarter backed by ZK store tests.
 */
@Slf4j
public class PravegaTablesControllerServiceStarterTest extends ControllerServiceStarterTest {
    private TestingServer zkServer;

    public PravegaTablesControllerServiceStarterTest() {
        super(true, false);
    }

    @Override
    public void setup() {
        try {
            zkServer = new TestingServerStarter().start();
        } catch (Exception e) {
            log.error("Error starting test zk server");
            Assert.fail("Error starting test zk server");
        }

        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder().connectionString(zkServer.getConnectString())
                .initialSleepInterval(500)
                .maxRetries(10)
                .namespace("pravega/" + UUID.randomUUID())
                .sessionTimeoutMs(10 * 1000)
                .build();
        storeClientConfig = StoreClientConfigImpl.withPravegaTablesClient(zkClientConfig);
        storeClient = StoreClientFactory.createStoreClient(storeClientConfig);
        Assert.assertNotNull(storeClient);
    }

    @Override
    public void tearDown() {
        try {
            storeClient.close();
        } catch (Exception e) {
            log.error("Error closing ZK client");
            Assert.fail("Error closing ZK client");
        }

        try {
            zkServer.close();
        } catch (IOException e) {
            log.error("Error stopping test zk server");
            Assert.fail("Error stopping test zk server");
        }
    }
}
