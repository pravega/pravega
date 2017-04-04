/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.controller.store.client.ZKClientConfig;
import com.emc.pravega.controller.store.client.impl.StoreClientConfigImpl;
import com.emc.pravega.controller.store.client.impl.ZKClientConfigImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.util.UUID;

/**
 * ZK store based ControllerServiceMain tests.
 */
@Slf4j
public class ZKControllerServiceMainTest extends ControllerServiceMainTest {
    private TestingServer zkServer;

    public ZKControllerServiceMainTest() {
        super(false);
    }

    @Override
    public void setup() {
        try {
            zkServer = new TestingServer();
        } catch (Exception e) {
            log.error("Error starting test zk server");
        }

        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder().connectionString(zkServer.getConnectString())
                .initialSleepInterval(500)
                .maxRetries(10)
                .namespace("pravega/" + UUID.randomUUID())
                .build();
        storeClientConfig = StoreClientConfigImpl.withZKClient(zkClientConfig);
    }

    @Override
    public void tearDown() {
        try {
            zkServer.close();
        } catch (IOException e) {
            log.error("Error stopping test zk server");
        }
    }
}
