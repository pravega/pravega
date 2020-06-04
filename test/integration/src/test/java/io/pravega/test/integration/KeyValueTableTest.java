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

import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.client.tables.impl.KeyValueTableTestBase;
import io.pravega.common.util.ByteArraySegment;

import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;

import io.pravega.test.common.TestUtils;
import java.time.Duration;
import java.util.Collections;

import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * Integration test for {@link KeyValueTable}s using real Segment Store and connection.
 * The only simulated component is the {@link Controller} which is provided via the {@link MockController}.
 * import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
 * //import io.pravega.test.common.AssertExtensions;
 */
@Slf4j
public class KeyValueTableTest extends KeyValueTableTestBase {
    private static final String ENDPOINT = "localhost";
    private static final String SCOPE = "Scope";
    private static final KeyValueTableConfiguration DEFAULT_CONFIG = KeyValueTableConfiguration.builder().partitionCount(5).build();
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private ServiceBuilder serviceBuilder;
    private TableStore tableStore;
    private PravegaConnectionListener serverListener;
    private ConnectionFactory connectionFactory;
    private TestingServer zkTestServer = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller;
    private KeyValueTableFactory keyValueTableFactory;

    @Before
    public void setup() throws Exception {
        super.setup();
        log.info("setup ...");
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize();
        this.tableStore = this.serviceBuilder.createTableStoreService();
        int port = TestUtils.getAvailableListenPort();
        this.serverListener = new PravegaConnectionListener(false, port, mock(StreamSegmentStore.class), this.tableStore);
        this.serverListener.startListening();

        this.connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());

        // 1. Start ZK
        log.info("STARTING ZOOKEEPER ...");
        this.zkTestServer = new TestingServerStarter().start();
        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        log.info("STARTING CONTROLLER ...");
        this.controllerWrapper.awaitRunning();
        //this.controller = new MockController(ENDPOINT, port, this.connectionFactory, true);
        this.controller = controllerWrapper.getController();
        log.info("CREATING SCOPE ...");
        this.controller.createScope(SCOPE);
        this.keyValueTableFactory = new KeyValueTableFactoryImpl(SCOPE, this.controller, this.connectionFactory);
    }

    @After
    public void tearDown() {
        this.controller.close();
        this.connectionFactory.close();
        this.serverListener.close();
        this.serviceBuilder.close();
    }

    /**
     * Smoke Test. Verify that the KeyValueTable can be created and deleted.
     */
    @Test
    public void testCreateDeleteKeyValueTable() {
        log.info("testCreateDeleteKeyValueTable ...");
        val kvt = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);
        val segments = this.controller.getCurrentSegmentsForKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName()).join();
        Assert.assertEquals(DEFAULT_CONFIG.getPartitionCount(), segments.getSegments().size());
        for (val s : segments.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            this.tableStore.get(s.getScopedName(), Collections.singletonList(new ByteArraySegment(new byte[1])), TIMEOUT).join();
        }

        // Verify re-creation does not work.
        Assert.assertFalse(this.controller.createKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName(), DEFAULT_CONFIG).join());

        // Delete and verify segments have been deleted too.
        /*
        val deleted = this.controller.deleteKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName()).join();
        Assert.assertTrue(deleted);
        Assert.assertFalse(this.controller.deleteKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName()).join());
        for (val s : segments.getSegments()) {
            AssertExtensions.assertSuppliedFutureThrows(
                    "Segment " + s + " has not been deleted.",
                    () -> this.tableStore.get(s.getScopedName(), Collections.singletonList(new ByteArraySegment(new byte[1])), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
        */
    }

    @Test
    @Override
    @Ignore
    public void testIterators() {
        // TODO: iterators not yet supported server-side.
        super.testIterators();
    }

    @Override
    protected KeyValueTable<Integer, String> createKeyValueTable() {
        val kvt = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);
        return this.keyValueTableFactory.forKeyValueTable(kvt.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER,
                KeyValueTableClientConfiguration.builder().build());
    }

    private KeyValueTableInfo newKeyValueTableName() {
        return new KeyValueTableInfo(SCOPE, String.format("KVT_%d", System.nanoTime()));
    }

}
