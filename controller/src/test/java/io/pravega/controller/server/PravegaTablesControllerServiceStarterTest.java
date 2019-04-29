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
import java.util.UUID;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;

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
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();

        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder().connectionString(zkServer.getConnectString())
                .initialSleepInterval(500)
                .maxRetries(10)
                .namespace("pravega/" + UUID.randomUUID())
                .sessionTimeoutMs(10 * 1000)
                .build();
        storeClientConfig = StoreClientConfigImpl.withPravegaTablesClient(zkClientConfig);
        storeClient = StoreClientFactory.createStoreClient(storeClientConfig);
        Assert.assertNotNull(storeClient);
        executor = Executors.newScheduledThreadPool(5);
    }

    @Override
    public void tearDown() throws Exception {
        storeClient.close();
        zkServer.close();
        executor.shutdown();
    }
}
