package com.emc.pravega.controller.server;

import com.emc.pravega.controller.store.client.StoreClientFactory;
import com.emc.pravega.controller.store.client.ZKClientConfig;
import com.emc.pravega.controller.store.client.impl.StoreClientConfigImpl;
import com.emc.pravega.controller.store.client.impl.ZKClientConfigImpl;
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
            zkServer = new TestingServer();
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
