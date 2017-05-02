/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.server;

import io.pravega.test.common.TestingServerStarter;
import io.pravega.server.controller.service.store.client.StoreClientFactory;
import io.pravega.server.controller.service.store.client.ZKClientConfig;
import io.pravega.server.controller.service.store.client.impl.StoreClientConfigImpl;
import io.pravega.server.controller.service.store.client.impl.ZKClientConfigImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;

import java.io.IOException;
import java.util.UUID;

/**
 * ControllerServiceStarter backed by ZK store tests.
 */
@Slf4j
public class ZKControllerServiceStarterTest extends ControllerServiceStarterTest {
    private TestingServer zkServer;

    public ZKControllerServiceStarterTest() {
        super(true);
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
                .build();
        storeClientConfig = StoreClientConfigImpl.withZKClient(zkClientConfig);
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
