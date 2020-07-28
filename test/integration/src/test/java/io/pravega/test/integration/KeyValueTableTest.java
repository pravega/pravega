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
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.Serializer;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.Version;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.client.tables.impl.KeyValueTableTestBase;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;

import java.time.Duration;
import java.util.Random;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Integration test for {@link KeyValueTable}s using real Segment Store, Controller and connection.
 *
 */
@Slf4j
public class KeyValueTableTest extends KeyValueTableTestBase {
    private static final String ENDPOINT = "localhost";
    private static final String SCOPE = "Scope";
    private static final KeyValueTableConfiguration DEFAULT_CONFIG = KeyValueTableConfiguration.builder()
            .partitionCount(5).build();
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private ServiceBuilder serviceBuilder;
    private TableStore tableStore;
    private PravegaConnectionListener serverListener = null;
    private ConnectionPool connectionPool;
    private TestingServer zkTestServer = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller;
    private ControllerServiceImpl controllerService;
    private RequestTracker requestTracker = new RequestTracker(true);
    private KeyValueTableFactory keyValueTableFactory;
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = ENDPOINT;
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private final int startKeyRange = 10000;
    private final int endKeyRange = 99999;
    private final int valueSize = 10312;
    private Random random = new Random(100);

    @Before
    public void setup() throws Exception {
        super.setup();

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SegmentStore service.
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        this.tableStore = serviceBuilder.createTableStoreService();

        this.serverListener = new PravegaConnectionListener(false, servicePort, serviceBuilder.createStreamSegmentService(),
                this.tableStore, executorService());
        this.serverListener.startListening();

        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();

        //4. Create Scope
        this.controller.createScope(SCOPE).get();
        ClientConfig clientConfig = ClientConfig.builder().build();
        SocketConnectionFactoryImpl connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        this.connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        this.keyValueTableFactory = new KeyValueTableFactoryImpl(SCOPE, this.controller, this.connectionPool);
    }


    @After
    public void tearDown() throws Exception {
        this.controller.close();
        this.connectionPool.close();
        this.controllerWrapper.close();
        this.serverListener.close();
        this.serviceBuilder.close();
        this.zkTestServer.close();
        this.keyValueTableFactory.close();
    }

    /**
     * Smoke Test. Verify that the KeyValueTable can be created and listed.
     */
    @Test
    public void testCreateListKeyValueTable() {
        val kvt1 = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);

        val segments = this.controller.getCurrentSegmentsForKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName()).join();
        Assert.assertEquals(DEFAULT_CONFIG.getPartitionCount(), segments.getSegments().size());

        for (val s : segments.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[1])), TIMEOUT).join();
        }

        // Verify re-creation does not work.
        Assert.assertFalse(this.controller.createKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName(), DEFAULT_CONFIG).join());

        // Create 2 more KVTables
        val kvt2 = newKeyValueTableName();
        created = this.controller.createKeyValueTable(kvt2.getScope(), kvt2.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);

        val kvt3 = newKeyValueTableName();
        created = this.controller.createKeyValueTable(kvt3.getScope(), kvt3.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);

        // Check list tables...
        AsyncIterator<KeyValueTableInfo> kvTablesIterator =  this.controller.listKeyValueTables(SCOPE);
        Iterator<KeyValueTableInfo> iter = kvTablesIterator.asIterator();
        Map<String, Integer> countMap = new HashMap<String, Integer>(3);
        while (iter.hasNext()) {
            KeyValueTableInfo kvtInfo = iter.next();
            if (kvtInfo.getScope().equals(SCOPE)) {
                if (countMap.containsKey(kvtInfo.getKeyValueTableName())) {
                    Integer newCount = Integer.valueOf(countMap.get(kvtInfo.getKeyValueTableName()).intValue() + 1);
                    countMap.put(iter.next().getKeyValueTableName(), newCount);
                } else {
                    countMap.put(kvtInfo.getKeyValueTableName(), 1);
                }
            }
        }
        Assert.assertEquals(3, countMap.size());
        Assert.assertEquals(1, countMap.get(kvt1.getKeyValueTableName()).intValue());
        Assert.assertEquals(1, countMap.get(kvt2.getKeyValueTableName()).intValue());
        Assert.assertEquals(1, countMap.get(kvt3.getKeyValueTableName()).intValue());
    }

    /**
     * Smoke Test. Verify that the KeyValueTable can be created and deleted.
     */
    @Test
    public void testCreateDeleteKeyValueTable() {
        val kvt1 = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);
        val segments = this.controller.getCurrentSegmentsForKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName()).join();
        Assert.assertEquals(DEFAULT_CONFIG.getPartitionCount(), segments.getSegments().size());

        for (val s : segments.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            log.info("Segment Number {}", s.getSegmentId());
            this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[1])), TIMEOUT).join();
        }

        // Delete and verify segments have been deleted too.
        val deleted = this.controller.deleteKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName()).join();
        Assert.assertTrue(deleted);

        Assert.assertFalse(this.controller.deleteKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName()).join());
        for (val s : segments.getSegments()) {
            AssertExtensions.assertSuppliedFutureThrows(
                    "Segment " + s + " has not been deleted.",
                    () -> this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[1])), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }

    }

    /**
     * Delete KVT and check TableEntries are not accessible
     */
    @Test
    public void testDeleteKVTRetrieveKVP() {
        KeyValueTableInfo newKVT = newKeyValueTableName();
        @Cleanup
        KeyValueTable<Integer, String> newKeyValueTable = getKeyValueTable(newKVT);
        String keyFamily = "KeyFamily1";

        LinkedHashMap<Integer, String> entryMap1 = new LinkedHashMap<>();
        Integer[] keyArray = new Integer[10];
        String[] valueArray1 = new String[10];
        // Creating 10 new entries to insert in the Table.
        for (int i = 0; i < keyArray.length; i++) {
            keyArray[i] = getKeyID();
            valueArray1[i] = getValue();
            entryMap1.put(keyArray[i], valueArray1[i]);
        }

        List<Version> versionList1 = newKeyValueTable.putAll(keyFamily, entryMap1.entrySet()).join();
        log.info("Version list1: {}", versionList1);
        assertNotNull("Version list1 should not be empty", versionList1);
        assertEquals("Version list1 size should be same as entry list1 size",
                versionList1.size(), entryMap1.size());

        List<Integer> keyList = new ArrayList<>();
        for (int i = 0; i < keyArray.length; i++) {
            keyList.add(keyArray[i]);
        }
        log.info("Key list: {}", keyList);
        List<TableEntry<Integer, String>> getEntryList1 = newKeyValueTable.getAll(keyFamily, keyList).join();
        log.info("Get Entry List1: {}", getEntryList1);
        assertNotNull("Get Entry List1 should not be empty", getEntryList1);
        assertEquals("Get Entry List1 size should be same as get entry list1 size",
                getEntryList1.size(), entryMap1.size());
        List<Integer> entryKeyList = new ArrayList<>();
        for (Integer key: entryMap1.keySet()) {
            entryKeyList.add(key);
        }
        log.info("entryKeyList: {}", entryKeyList);
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("Corresponding key for getEntryList should be as inserted",
                    keyList.get(i), getEntryList1.get(i).getKey().getKey());
            assertEquals("Corresponding key for entryList should be as inserted",
                    entryKeyList.get(i), getEntryList1.get(i).getKey().getKey());
        }

        // Comparing each & every entry values of both entryList and getEntryList.
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("All string values of both entryList1 and getEntryList1 should be equal",
                    entryMap1.get(entryKeyList.get(i)), getEntryList1.get(i).getValue());
        }

        val segments = this.controller.getCurrentSegmentsForKeyValueTable(SCOPE,
                newKVT.getKeyValueTableName()).join();

        boolean deleted = this.controller.deleteKeyValueTable(SCOPE, newKVT.getKeyValueTableName()).join();
        assertTrue("The requested KVT must get deleted", deleted);
        log.info("KVT '{}' got deleted", newKVT.getKeyValueTableName());

        assertFalse("Request to remove already deleted KVT must not succeed",
                this.controller.deleteKeyValueTable(SCOPE, newKVT.getKeyValueTableName()).join());

        for (val s : segments.getSegments()) {
            AssertExtensions.assertSuppliedFutureThrows(
                    "Segment " + s + " has not been deleted.",
                    () -> this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[1])), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Test Create-Delete-Recreation of KVTable with same name
     */
    @Test
    public void testRecreateKVTable() {
        KeyValueTableInfo newKVT = newKeyValueTableName();
        @Cleanup
        KeyValueTable<Integer, String> newKeyValueTable1 = getKeyValueTable(newKVT);

        String keyFamily = "KeyFamily1";
        List<TableEntry<Integer, String>> entryList = new ArrayList<>();
        Integer[] keyArray = new Integer[10];
        String[] valueArray = new String[10];
        // Inserting 10 new entries to the Table only if they are not already present.
        for (int i = 0; i < keyArray.length; i++) {
            keyArray[i] = getKeyID();
            valueArray[i] = getValue();
            entryList.add(TableEntry.notExists(keyArray[i], valueArray[i]));
        }
        log.info("Entry list: {}", entryList);
        List<Version> versionList1 = newKeyValueTable1.replaceAll(keyFamily, entryList).join();
        assertNotNull("Version list 1 should not be empty", versionList1);
        assertEquals("Version list 1 size should be same as entry list size",
                versionList1.size(), entryList.size());
        log.info("Version list: {}", versionList1);

        List<Integer> keyList1 = new ArrayList<>();
        for (int i = 0; i < keyArray.length; i++) {
            keyList1.add(keyArray[i]);
        }
        log.info("Key list: {}", keyList1);
        List<TableEntry<Integer, String>> getEntryList1 = newKeyValueTable1.getAll(keyFamily, keyList1).join();
        assertNotNull("Get Entry List 1 should not be empty", getEntryList1);
        assertEquals("Get Entry List 1 size should be same as get entry list size",
                getEntryList1.size(), entryList.size());
        log.info("Get Entry List: {}", getEntryList1);
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("Corresponding key for getEntryList1 should be as inserted",
                    keyList1.get(i), getEntryList1.get(i).getKey().getKey());
            assertEquals("Corresponding key for entryList should be as inserted",
                    entryList.get(i).getKey().getKey(), getEntryList1.get(i).getKey().getKey());
        }

        // Comparing each & every entry values of both entryList and getEntryList.
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("All string values of both entryList and getEntryList1 should be equal",
                    entryList.get(i).getValue(), getEntryList1.get(i).getValue());
        }

        val segments = this.controller.getCurrentSegmentsForKeyValueTable(SCOPE,
                newKVT.getKeyValueTableName()).join();

        assertTrue("The requested KVT must get deleted", this.controller.deleteKeyValueTable(SCOPE,
                newKVT.getKeyValueTableName()).join());
        log.info("KVT '{}' got deleted", newKVT.getKeyValueTableName());

        for (val s : segments.getSegments()) {
            AssertExtensions.assertSuppliedFutureThrows(
                    "Segment " + s + " has not been deleted.",
                    () -> this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[1])), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }

        // create KVT-2
        @Cleanup
        KeyValueTable<Integer, String> newKeyValueTable2 = getKeyValueTable(newKVT);

        log.info("Entry list: {}", entryList);
        List<Version> versionList2 = newKeyValueTable2.replaceAll(keyFamily, entryList).join();
        assertNotNull("Version list should not be empty", versionList2);
        assertEquals("Version list size should be same as entry list size",
                versionList2.size(), entryList.size());
        log.info("Version list: {}", versionList2);

        List<Integer> keyList2 = new ArrayList<>();
        for (int i = 0; i < keyArray.length; i++) {
            keyList2.add(keyArray[i]);
        }
        log.info("Key list: {}", keyList2);
        List<TableEntry<Integer, String>> getEntryList3 = newKeyValueTable2.getAll(keyFamily, keyList2).join();
        assertNotNull("Get Entry List 3 should not be empty", getEntryList3);
        assertEquals("Get Entry List size 3 should be same as get entry list size",
                getEntryList3.size(), entryList.size());
        log.info("Get Entry List: {}", getEntryList3);
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("Corresponding key for getEntryList3 should be as inserted",
                    keyList2.get(i), getEntryList3.get(i).getKey().getKey());
            assertEquals("Corresponding key for entryList should be as inserted",
                    entryList.get(i).getKey().getKey(), getEntryList3.get(i).getKey().getKey());
        }

        // Comparing each & every entry values of both entryList and getEntryList.
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("All string values of both entryList and getEntryList3 should be equal",
                    entryList.get(i).getValue(), getEntryList3.get(i).getValue());
        }
    }

    private KeyValueTable<Integer, String> getKeyValueTable(KeyValueTableInfo newKVT) {
        assertTrue("The requested KVT must get created",
                this.controller.createKeyValueTable(SCOPE, newKVT.getKeyValueTableName(),
                        DEFAULT_CONFIG).join());

        assertEquals("Table partition value should be same as passed", DEFAULT_CONFIG.getPartitionCount(),
                Futures.getAndHandleExceptions(this.controller.getCurrentSegmentsForKeyValueTable(SCOPE, newKVT.getKeyValueTableName()),
                        RuntimeException::new).getSegments().size());

        return this.keyValueTableFactory.forKeyValueTable(newKVT.getKeyValueTableName(),
                KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());

    }

    private Integer getKeyID() {
        return random.nextInt(endKeyRange - startKeyRange) + startKeyRange;
    }

    private String getValue() {
        String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";

        StringBuilder sb = new StringBuilder(valueSize);

        for (int i = 0; i < valueSize; i++) {
            int index = (int) (alphaNumericString.length() * random.nextDouble());
            sb.append(alphaNumericString.charAt(index));
        }
        return sb.toString();
    }

    @Override
    protected KeyValueTable<Integer, String> createKeyValueTable() {
        return createKeyValueTable(KEY_SERIALIZER, VALUE_SERIALIZER);
    }

    @Override
    protected <K, V> KeyValueTable<K, V> createKeyValueTable(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        val kvt = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);
        return this.keyValueTableFactory.forKeyValueTable(kvt.getKeyValueTableName(), keySerializer, valueSerializer,
                KeyValueTableClientConfiguration.builder().build());
    }

    private KeyValueTableInfo newKeyValueTableName() {
        return new KeyValueTableInfo(SCOPE, String.format("KVT-%d", System.nanoTime()));
    }
}

