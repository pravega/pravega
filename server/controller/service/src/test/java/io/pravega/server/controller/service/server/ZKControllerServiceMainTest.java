/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.server;

import io.pravega.test.common.TestingServerStarter;
import io.pravega.server.controller.service.store.client.ZKClientConfig;
import io.pravega.server.controller.service.store.client.impl.StoreClientConfigImpl;
import io.pravega.server.controller.service.store.client.impl.ZKClientConfigImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

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
    }

    @Override
    public void tearDown() {
        try {
            zkServer.close();
        } catch (IOException e) {
            log.error("Error stopping test zk server");
            Assert.fail("Error stopping test zk server");
        }
    }

    @Test(timeout = 10000)
    public void testZKSessionExpiry() {
        ControllerServiceMain controllerServiceMain = new ControllerServiceMain(createControllerServiceConfig(),
                MockControllerServiceStarter::new);

        controllerServiceMain.startAsync();

        try {
            controllerServiceMain.awaitRunning();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for controllerServiceMain to get ready", e);
            Assert.fail("Failed waiting for controllerServiceMain to get ready");
        }

        try {
            controllerServiceMain.awaitServiceStarting().awaitRunning();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for controllerServiceStarter to get ready", e);
            Assert.fail("Failed waiting for controllerServiceStarter to get ready");
            return;
        }

        // Simulate ZK session timeout
        try {
            controllerServiceMain.forceClientSessionExpiry();
        } catch (Exception e) {
            log.error("Failed while simulating client session expiry", e);
            Assert.fail("Failed while simulating client session expiry");
        }

        // Now, that session has expired, lets wait for starter to start again.
        try {
            controllerServiceMain.awaitServicePausing().awaitTerminated();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for controllerServiceStarter termination", e);
            Assert.fail("Failed waiting for controllerServiceStarter termination");
        }

        try {
            controllerServiceMain.awaitServiceStarting().awaitRunning();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for starter to get ready again", e);
            Assert.fail("Failed waiting for controllerServiceStarter to get ready again");
        }

        controllerServiceMain.stopAsync();

        try {
            controllerServiceMain.awaitServicePausing().awaitTerminated();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for controllerServiceStarter termination", e);
            Assert.fail("Failed waiting for controllerServiceStarter termination");
        }

        try {
            controllerServiceMain.awaitTerminated();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for termination of controllerServiceMain", e);
            Assert.fail("Failed waiting for termination of controllerServiceMain");
        }
    }
}
