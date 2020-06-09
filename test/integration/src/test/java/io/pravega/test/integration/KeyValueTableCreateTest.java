/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import lombok.Cleanup;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link KeyValueTableManager} integration tests using real Controller and Segment Store.
 */
public class KeyValueTableCreateTest extends ThreadPooledTestSuite {
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
    private final int servicePort = TestUtils.getAvailableListenPort();
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setup() throws Exception {
        this.zkTestServer = new TestingServerStarter().start();

        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        this.server = new PravegaConnectionListener(false, servicePort, store, tableStore, executorService());
        this.server.startListening();

        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false, this.controllerPort, this.serviceHost, this.servicePort, 1);
        this.controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        this.controllerWrapper.close();
        this.server.close();
        this.serviceBuilder.close();
        this.zkTestServer.close();
    }

    /**
     * Smoke Test. Verify that the KeyValueTable can be created and deleted using real controller
     * TODO This should be properly implemented once the Controller supports these operations.
     */
    @Test
    public void testCreateDeleteKeyValueTableWithRealController() {
        final String scope = "scope";
        final String kvtName = "kvt";
        final KeyValueTableConfiguration config = KeyValueTableConfiguration.builder().build();
        @Cleanup
        val manager = KeyValueTableManager.create(controllerURI);
        AssertExtensions.assertThrows(
                "",
                () -> manager.createKeyValueTable(scope, kvtName, config),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "",
                () -> manager.updateKeyValueTable(scope, kvtName, config),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "",
                () -> manager.listKeyValueTables(scope),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "",
                () -> manager.deleteKeyValueTable(scope, kvtName),
                ex -> ex instanceof UnsupportedOperationException);

        // Explicitly close to verify idempotence.
        manager.close();
    }
}
