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
<<<<<<< HEAD
<<<<<<< HEAD
=======
import io.pravega.client.stream.mock.MockController;
>>>>>>> Issue 4570: (KeyValue Tables) Client Data Path Implementation (#4687)
=======
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.Version;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.NoSuchKeyException;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.client.tables.impl.KeyValueTableTestBase;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> Issue 4879: (KeyValueTables) List and Delete API for Key Value Tables on Controller (#4881)
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;

import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
<<<<<<< HEAD
=======
import io.pravega.common.util.ByteArraySegment;
<<<<<<< HEAD
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
>>>>>>> Issue 4570: (KeyValue Tables) Client Data Path Implementation (#4687)
=======

>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
=======
>>>>>>> Issue 4879: (KeyValueTables) List and Delete API for Key Value Tables on Controller (#4881)
=======
import io.pravega.common.util.ByteArraySegment;

>>>>>>> Adding new tests to Integration section of test framework
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import java.util.Collections;
=======

>>>>>>> Adding new tests to Integration section of test framework
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.Assert;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;


/**
 * Integration test for {@link KeyValueTable}s using real Segment Store, Controller and connection.
 *
 */
@Slf4j
=======
import java.time.Duration;
=======
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
import java.util.Collections;
import java.time.Duration;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test for {@link KeyValueTable}s using real Segment Store, Controller and connection.
 *
 */
<<<<<<< HEAD
>>>>>>> Issue 4570: (KeyValue Tables) Client Data Path Implementation (#4687)
=======
@Slf4j
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
public class KeyValueTableTest extends KeyValueTableTestBase {
    private static final String ENDPOINT = "localhost";
    private static final String SCOPE = "Scope";
    private static final KeyValueTableConfiguration DEFAULT_CONFIG = KeyValueTableConfiguration.builder().partitionCount(5).build();
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private ServiceBuilder serviceBuilder;
    private TableStore tableStore;
<<<<<<< HEAD
<<<<<<< HEAD
    private PravegaConnectionListener serverListener = null;
    private ConnectionFactory connectionFactory;
    private TestingServer zkTestServer = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller;
    private KeyValueTableFactory keyValueTableFactory;
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = ENDPOINT;
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
=======
    private PravegaConnectionListener serverListener;
=======
    private PravegaConnectionListener serverListener = null;
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
    private ConnectionFactory connectionFactory;
    private TestingServer zkTestServer = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller;
    private KeyValueTableFactory keyValueTableFactory;
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> Issue 4570: (KeyValue Tables) Client Data Path Implementation (#4687)
=======
=======
    private KeyValueTable<Integer, String> keyValueTable;
>>>>>>> Adding new tests to Integration section of test framework
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = ENDPOINT;
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
<<<<<<< HEAD
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
=======
    private final int startKeyRange = 10000;
    private final int endKeyRange = 99999;
    private final int valueSize = 10312;
    private Random random = new Random();
>>>>>>> Adding new tests to Integration section of test framework

    @Before
    public void setup() throws Exception {
        super.setup();
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SegmentStore service.
<<<<<<< HEAD
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        this.tableStore = serviceBuilder.createTableStoreService();

        this.serverListener = new PravegaConnectionListener(false, servicePort, serviceBuilder.createStreamSegmentService(), this.tableStore, executorService());
        this.serverListener.startListening();

        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();

        //4. Create Scope
        this.isScopeCreated = this.controller.createScope(SCOPE).get();
        this.connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        this.keyValueTableFactory = new KeyValueTableFactoryImpl(SCOPE, this.controller, this.connectionFactory);
    }


    @After
    public void tearDown() throws Exception {
        this.controller.close();
        this.connectionFactory.close();
        this.controllerWrapper.close();
        this.serverListener.close();
        this.serviceBuilder.close();
        this.zkTestServer.close();
    }

    /**
     * Smoke Test. Verify that the KeyValueTable can be created and listed.
     */
    @Test
    public void testCreateListKeyValueTable() {
        Assert.assertTrue(isScopeCreated);
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
=======
=======
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        this.tableStore = serviceBuilder.createTableStoreService();

        this.serverListener = new PravegaConnectionListener(false, servicePort, serviceBuilder.createStreamSegmentService(), this.tableStore, executorService());
        this.serverListener.startListening();

        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();

        // 4. Create Scope
        this.isScopeCreated = this.controller.createScope(SCOPE).get();
        this.connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        this.keyValueTableFactory = new KeyValueTableFactoryImpl(SCOPE, this.controller, this.connectionFactory);

        // 5. Create KVT
        this.keyValueTable = createKeyValueTable();
    }


    @After
    public void tearDown() throws Exception {
        this.controller.close();
        this.connectionFactory.close();
        this.controllerWrapper.close();
        this.serverListener.close();
        this.serviceBuilder.close();
<<<<<<< HEAD
>>>>>>> Issue 4570: (KeyValue Tables) Client Data Path Implementation (#4687)
=======
        this.zkTestServer.close();
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
    }

    /**
     * Smoke Test. Verify that the KeyValueTable can be created and deleted.
     */
    @Test
    public void testCreateDeleteKeyValueTable() {
        Assert.assertTrue(isScopeCreated);
        val kvt = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);
        val segments = this.controller.getCurrentSegmentsForKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName()).join();
        Assert.assertEquals(DEFAULT_CONFIG.getPartitionCount(), segments.getSegments().size());

        for (val s : segments.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[1])), TIMEOUT).join();
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

    /**
     * Test case-42: Insert Same Key into multiple KeyFamilies
     */
    @Test
    public void test1SameKeyMutipleKF() {
        String kf1 = "KeyFamily1";
        String kf2 = "KeyFamily2";

        Integer key = 22;
        String value1 = "This value belongs to KeyFamily1";
        String value2 = "This value belongs to KeyFamily2";

        Version version1 = this.keyValueTable.put(kf1, key, value1).join();
        Version version2 = this.keyValueTable.put(kf2, key, value2).join();
        log.info("Version1: {}", version1);
        log.info("Version2: {}", version2);

        TableEntry<Integer, String> tableEntry1 = this.keyValueTable.get(kf1, key).join();
        TableEntry<Integer, String> tableEntry2 = this.keyValueTable.get(kf2, key).join();

        assertEquals("Keys should be same for both entries",
                tableEntry1.getKey().getKey(), tableEntry2.getKey().getKey());
        assertEquals("Corresponding value for entry1 should be as inserted",
                value1, tableEntry1.getValue());
        assertEquals("Corresponding value for entry2 should be as inserted",
                value2, tableEntry2.getValue());
        assertNotEquals("Corresponding values should be different for both the entries",
                tableEntry1.getValue(), tableEntry2.getValue());

        log.info("Table entry1, key: {} and value: {}", tableEntry1.getKey(), tableEntry1.getValue());
        log.info("Table entry2, key: {} and value: {}", tableEntry2.getKey(), tableEntry2.getValue());
        log.info("Successfully completed test Insert Same Key into multiple KeyFamilies");
    }

    /**
     * Test case-43: Conditional insert KVP, if key not exists already
     */
    @Test
    public void test2InsertIfExists() {
        Integer key = getKeyID();
        String value = getValue();

        Version version1 = this.keyValueTable.putIfAbsent(null, key, value).join();
        assertNotNull("Vesrion1 should not be null", version1);
        log.info("version1: {}", version1);
        TableEntry<Integer, String> tableEntry1 = this.keyValueTable.get(null, key).join();
        assertEquals("Corresponding key for entry1 should be as inserted",
                key, tableEntry1.getKey().getKey());
        log.info("Get tableEntry1: {}", tableEntry1);
        assertEquals("Corresponding value for entry1 should be as inserted",
                value, tableEntry1.getValue());

        AssertExtensions.assertSuppliedFutureThrows(
                "putIfAbsent did not throw Exception for already existing key",
                () -> this.keyValueTable.putIfAbsent(null, key, value),
                ex -> ex instanceof BadKeyVersionException
        );
        log.info("Found BadKeyVersionException for putIfAbsent insert for already existing key");

        List<Integer> searchKey = new ArrayList<Integer>();
        searchKey.add(key);
        List<TableEntry<Integer, String>> getAllEntry = this.keyValueTable.getAll(null, searchKey).join();
        assertEquals("Only one entry should be present from the fetched list with the given key",
                1, getAllEntry.size());
        log.info("Only one entry is present from the Fetched list of 'if-exists-test': {}", getAllEntry);
        log.info("Successfully completed test Conditional insert KVP, if key not exists already");
    }

    /**
     * Test case-44: Conditional update Key value using its version
     */
    @Test
    public void test3ConditionUpdateKeyValue() {
        Integer key = getKeyID();
        String value1 = getValue();

        // Inserting first set of values in the table.
        Version version1 = this.keyValueTable.putIfAbsent(null, key, value1).join();
        assertNotNull("Vesrion1 should not be null", version1);
        log.info("version1: {}", version1);
        TableEntry<Integer, String> tableEntry1 = this.keyValueTable.get(null, key).join();
        log.info("Get tableEntry1: {}", tableEntry1);
        assertEquals("Corresponding key for entry1 should be as inserted",
                key, tableEntry1.getKey().getKey());
        assertEquals("Corresponding value for entry1 should be as inserted",
                value1, tableEntry1.getValue());

        // Updating with second set of values in the table.
        String value2 = getValue();
        Version version2 = this.keyValueTable.put(null, key, value2).join();
        assertNotNull("Vesrion2 should not be null", version1);
        log.info("version2: {}", version2);
        TableEntry<Integer, String> tableEntry2 = this.keyValueTable.get(null, key).join();
        log.info("Get tableEntry2: {}", tableEntry2);
        assertEquals("Corresponding key for entry2 should be as inserted",
                key, tableEntry2.getKey().getKey());
        assertEquals("Corresponding value for entry2 should be as inserted",
                value2, tableEntry2.getValue());
        assertNotEquals("Corresponding value for entry2 should not remian as entry1",
                value1, tableEntry2.getValue());
        assertNotEquals("Version1 and Version2 should be different in values", version1, version2);

        // Updating third set of values in the table using version of second entry as condition.
        String value3 = getValue();
        Version version3 = this.keyValueTable.replace(null, key, value3, version2).join();
        assertNotNull("Vesrion3 should not be null", version3);
        log.info("version3: {}", version3);
        TableEntry<Integer, String> tableEntry3 = this.keyValueTable.get(null, key).join();
        log.info("Get tableEntry3: {}", tableEntry3);
        assertEquals("Corresponding key for entry3 should be as inserted",
                key, tableEntry3.getKey().getKey());
        assertEquals("Corresponding value for entry3 should be as inserted",
                value3, tableEntry3.getValue());
        assertNotEquals("Corresponding value for entry3 should not remian as entry2",
                value2, tableEntry3.getValue());
        assertNotEquals("Version2 and Version3 should be different in values",
                version2, version3);

        // Trying to update fourth set of values in the table using version of first entry as condition.
        String value4 = getValue();
        AssertExtensions.assertSuppliedFutureThrows(
                "Should not allow updating the table entry with an older key version",
                () -> this.keyValueTable.replace(null, key, value4, version1),
                ex -> ex instanceof BadKeyVersionException
        );
        log.info("Found BadKeyVersionException while updating the table entry with an older key version");

        TableEntry<Integer, String> tableEntry4 = this.keyValueTable.get(null, key).join();
        log.info("Get tableEntry4: {}", tableEntry4);
        assertEquals("Corresponding key for entry4 should be as inserted",
                key, tableEntry4.getKey().getKey());
        assertNotEquals("Corresponding value4 for entry4 should not be as inserted",
                value4, tableEntry4.getValue());
        assertEquals("Corresponding value3 for entry4 should be as inserted",
                value3, tableEntry4.getValue());
        assertNotEquals("Corresponding value for entry4 should not remian as entry1",
                value1, tableEntry4.getValue());
        log.info("Successfully completed test Conditional update Key value using its version");
    }

    /**
     * Test case-45: Conditional delete single Key using its version
     */
    @Test
    public void test4ConditionDeleteKey() {
        Integer key = getKeyID();
        String value1 = getValue();

        Version version1 = this.keyValueTable.put(null, key, value1).join();
        assertNotNull("Vesrion1 should not be null", version1);
        log.info("version1: " + version1);
        TableEntry<Integer, String> tableEntry1 = this.keyValueTable.get(null, key).join();
        log.info("Get tableEntry1: {}", tableEntry1);
        assertEquals("Corresponding key for entry1 should be as inserted",
                key, tableEntry1.getKey().getKey());
        assertEquals("Corresponding value for entry1 should be as inserted",
                value1, tableEntry1.getValue());

        String value2 = getValue();
        Version version2 = this.keyValueTable.put(null, key, value2).join();
        assertNotNull("Vesrion2 should not be null", version1);
        log.info("version2: " + version2);
        TableEntry<Integer, String> tableEntry2 = this.keyValueTable.get(null, key).join();
        log.info("Get tableEntry2: {}", tableEntry2);
        assertEquals("Corresponding key for entry2 should be as inserted",
                key, tableEntry2.getKey().getKey());
        assertEquals("Corresponding value for entry2 should be as inserted",
                value2, tableEntry2.getValue());
        assertNotEquals("Corresponding value for entry2 should not remian as entry1",
                value1, tableEntry2.getValue());
        assertNotEquals("Version1 and Version2 should be different in values", version1, version2);

        AssertExtensions.assertSuppliedFutureThrows(
                "Table entry with an older key version should not get removed",
                () -> this.keyValueTable.remove(null, key, version1),
                ex -> ex instanceof BadKeyVersionException
        );
        log.info("Found BadKeyVersionException while removing the table entry with an older key version");

        assertNull("No error should be obtained during key remove",
                this.keyValueTable.remove(null, key, version2).join());
        log.info("The requested table entry with specified key & its version is removed");

        assertNull("Null should be returned as the reqested key entry is already removed",
                this.keyValueTable.get(null, key).join());
        log.info("Null obtained on requsting to fetch a key entry which does not exists");

        AssertExtensions.assertSuppliedFutureThrows(
                "Error should be obtained on attempt to delete already removed key entry",
                () -> this.keyValueTable.remove(null, key, version2),
                ex -> ex instanceof NoSuchKeyException
        );
        log.info("Found NoSuchKeyException on attempt to delete already removed key entry");
        log.info("Successfully completed test Conditional delete single Key using its version");
    }

    /**
     * Test case-46: Get Key versions List
     */
    @Test
    public void test5GetKeyVersionList() {
        String keyFamily = "KeyFamily1";
        List<TableEntry<Integer, String>> entryList = new ArrayList<>();
        Integer[] keyArray = new Integer[10];
        for (int i = 0; i < keyArray.length; i++) {
            keyArray[i] = getKeyID();
        }
        String[] valueArray = new String[10];
        for (int i = 0; i < valueArray.length; i++) {
            valueArray[i] = getValue();
        }

        // Adding 10 new entries to the Table only if they are not already present.
        for (int i = 0; i < keyArray.length; i++) {
            entryList.add(TableEntry.notExists(keyArray[i], valueArray[i]));
        }
        log.info("Entry list: {}", entryList);
        List<Version> versionList = this.keyValueTable.replaceAll(keyFamily, entryList).join();
        assertNotNull("Version list should not be empty", versionList);
        assertEquals("Version list size should be same as entry list size",
                versionList.size(), entryList.size());
        log.info("Version list: {}", versionList);

        List<Integer> keyList = new ArrayList<>();
        for (int i = 0; i < keyArray.length; i++) {
            keyList.add(keyArray[i]);
        }
        log.info("Key list: {}", keyList);
        List<TableEntry<Integer, String>> getEntryList = this.keyValueTable.getAll(keyFamily, keyList).join();
        assertNotNull("Get Entry List should not be empty", getEntryList);
        assertEquals("Get Entry List size should be same as get entry list size",
                getEntryList.size(), entryList.size());
        log.info("Get Entry List: {}", getEntryList);
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("Corresponding key for getEntryList should be as inserted",
                    keyList.get(i), getEntryList.get(i).getKey().getKey());
            assertEquals("Corresponding key for entryList should be as inserted",
                    entryList.get(i).getKey().getKey(), getEntryList.get(i).getKey().getKey());
        }

        // Comparing version values of both obtained versionList and getEntryList.
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("All version values of both versionList and getEntryList should be equal",
                    versionList.get(i), getEntryList.get(i).getKey().getVersion());
        }
        log.info("Successfully completed test Get Key versions List");
    }

    /**
     * Test case-47: Conditional insert mutiple entries with KeyFamily
     */
    @Test
<<<<<<< HEAD
    public void testCreateDeleteKeyValueTable() {
<<<<<<< HEAD
<<<<<<< HEAD
        Assert.assertTrue(isScopeCreated);
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

=======
=======
        Assert.assertTrue(isScopeCreated);
<<<<<<< HEAD
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
        val kvt = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName(), DEFAULT_CONFIG).join();
=======
        val kvt1 = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName(), DEFAULT_CONFIG).join();
>>>>>>> Issue 4879: (KeyValueTables) List and Delete API for Key Value Tables on Controller (#4881)
        Assert.assertTrue(created);
        val segments = this.controller.getCurrentSegmentsForKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName()).join();
        Assert.assertEquals(DEFAULT_CONFIG.getPartitionCount(), segments.getSegments().size());
=======
    public void test6ConditionInsertMultiKeys() {
        String keyFamily = "KeyFamily1";
        List<TableEntry<Integer, String>> entryList = new ArrayList<>();
        Integer[] keyArray = new Integer[10];
        for (int i = 0; i < keyArray.length; i++) {
            keyArray[i] = getKeyID();
        }
        String[] valueArray = new String[10];
        for (int i = 0; i < valueArray.length; i++) {
            valueArray[i] = getValue();
        }
>>>>>>> Adding new tests to Integration section of test framework

        // Adding 10 new entries to the Table only if they are not already present.
        for (int i = 0; i < keyArray.length; i++) {
            entryList.add(TableEntry.notExists(keyArray[i], valueArray[i]));
        }
        log.info("Entry list: {}", entryList);
        List<Version> versionList = this.keyValueTable.replaceAll(keyFamily, entryList).join();
        assertNotNull("Version list should not be empty", versionList);
        assertEquals("Version list size should be same as entry list size",
                versionList.size(), entryList.size());
        log.info("Version list: {}", versionList);

        List<Integer> keyList = new ArrayList<>();
        for (int i = 0; i < keyArray.length; i++) {
            keyList.add(keyArray[i]);
        }
        log.info("Key list: {}", keyList);
        List<TableEntry<Integer, String>> getEntryList = this.keyValueTable.getAll(keyFamily, keyList).join();
        assertNotNull("Get Entry List should not be empty", getEntryList);
        assertEquals("Get Entry List size should be same as get entry list size",
                getEntryList.size(), entryList.size());
        log.info("Get Entry List: {}", getEntryList);
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("Corresponding key for getEntryList should be as inserted",
                    keyList.get(i), getEntryList.get(i).getKey().getKey());
            assertEquals("Corresponding key for entryList should be as inserted",
                    entryList.get(i).getKey().getKey(), getEntryList.get(i).getKey().getKey());
        }

        // Comparing each & every entry values of both entrylist and getEntryList.
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("All string values of both entryList and getEntryList should be equal",
                    entryList.get(i).getValue(), getEntryList.get(i).getValue());
        }
        log.info("Successfully completed test Conditional insert mutiple entries with KeyFamily");
    }

    /**
     * Test case-48: Conditional update multiple Key value with KF using its version
     */
    @Test
    public void test7ConditionUpdateMultiKeys() {
        String keyFamily = "KeyFamily1";
        List<TableEntry<Integer, String>> entryList1 = new ArrayList<>();
        Integer[] keyArray = new Integer[10];
        for (int i = 0; i < keyArray.length; i++) {
            keyArray[i] = getKeyID();
        }
        String[] valueArray1 = new String[10];
        for (int i = 0; i < valueArray1.length; i++) {
            valueArray1[i] = getValue();
        }

        // Adding 10 new entries to the Table only if they are not already present.
        for (int i = 0; i < keyArray.length; i++) {
            entryList1.add(TableEntry.notExists(keyArray[i], valueArray1[i]));
        }
        log.info("Entry list1: {}", entryList1);
        List<Version> versionList1 = this.keyValueTable.replaceAll(keyFamily, entryList1).join();
        assertNotNull("Version list1 should not be empty", versionList1);
        assertEquals("Version list1 size should be same as entry list1 size",
                versionList1.size(), entryList1.size());
        log.info("Version list1: {}", versionList1);

        List<Integer> keyList = new ArrayList<>();
        for (int i = 0; i < keyArray.length; i++) {
            keyList.add(keyArray[i]);
        }
        log.info("Key list: {}", keyList);
        List<TableEntry<Integer, String>> getEntryList1 = this.keyValueTable.getAll(keyFamily, keyList).join();
        assertNotNull("Get Entry List1 should not be empty", getEntryList1);
        assertEquals("Get Entry List1 size should be same as get entry list1 size",
                getEntryList1.size(), entryList1.size());
        log.info("Get Entry List1: {}", getEntryList1);
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("Corresponding key for getEntryList should be as inserted",
                    keyList.get(i), getEntryList1.get(i).getKey().getKey());
            assertEquals("Corresponding key for entryList should be as inserted",
                    entryList1.get(i).getKey().getKey(), getEntryList1.get(i).getKey().getKey());
        }

        // Comparing each & every entry values of both entrylist and getEntryList.
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("All string values of both entryList1 and getEntryList1 should be equal",
                    entryList1.get(i).getValue(), getEntryList1.get(i).getValue());
        }

        // Preparing entryList2 with versioned table entry as obtained from versionList1.
        List<TableEntry<Integer, String>> entryList2 = new ArrayList<>();
        String[] valueArray2 = new String[10];
        for (int i = 0; i < valueArray1.length; i++) {
            valueArray2[i] = getValue();
        }
        for (int i = 0; i < keyArray.length; i++) {
            entryList2.add(TableEntry.versioned(keyArray[i], versionList1.get(i), valueArray2[i]));
        }
        log.info("Entry list2: {}", entryList2);

        // Conditionally updating the values of all the Keys with the specified key version and KeyFamily.
        List<Version> versionList2 = this.keyValueTable.replaceAll(keyFamily, entryList2).join();
        assertNotNull("Version list2 should not be empty", versionList2);
        assertEquals("Version list2 size should be same as entry list2 size",
                versionList2.size(), entryList2.size());
        log.info("Version list2: {}", versionList2);

        log.info("Key list: {}", keyList);
        List<TableEntry<Integer, String>> getEntryList2 = this.keyValueTable.getAll(keyFamily, keyList).join();
        assertNotNull("Get Entry List2 should not be empty", getEntryList2);
        assertEquals("Get Entry List2 size should be same as get entry list2 size",
                getEntryList2.size(), entryList2.size());
        log.info("Get Entry List2: {}", getEntryList2);
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("Corresponding key for getEntryList should be as inserted",
                    keyList.get(i), getEntryList2.get(i).getKey().getKey());
            assertEquals("Corresponding key for entryList should be as inserted",
                    entryList2.get(i).getKey().getKey(), getEntryList2.get(i).getKey().getKey());
        }

        // Comparing each & every entry values of both entrylist and getEntryList.
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("All string values of both entryList2 and getEntryList2 should be equal",
                    entryList2.get(i).getValue(), getEntryList2.get(i).getValue());
        }

        // Comparing each & every version values of both versionList1 and versionList2.
        for (int i = 0; i < keyArray.length; i++) {
            assertNotEquals("All version values of both getEntryList1 and getEntryList2 should not be equal",
                    getEntryList1.get(i).getKey().getVersion(), getEntryList2.get(i).getKey().getVersion());
            assertNotEquals("All version values of both versionList1 and versionList2 should not be equal",
                    versionList1.get(i), versionList2.get(i));
        }
        log.info("Successfully completed test Conditional update multiple Key values with KF using its version");
    }

    /**
     * Test case-49: Conditional delete Key with KeyFamily using its version
     */
    @Test
    public void test8ConditionDeleteMultikeys() {
        String keyFamily = "KeyFamily1";
        List<TableEntry<Integer, String>> entryList1 = new ArrayList<>();
        Integer[] keyArray = new Integer[10];
        for (int i = 0; i < keyArray.length; i++) {
            keyArray[i] = getKeyID();
        }
        String[] valueArray1 = new String[10];
        for (int i = 0; i < valueArray1.length; i++) {
            valueArray1[i] = getValue();
        }

        // Adding 10 new entries to the Table only if they are not already present.
        for (int i = 0; i < keyArray.length; i++) {
            entryList1.add(TableEntry.notExists(keyArray[i], valueArray1[i]));
        }
        log.info("Entry list1: {}", entryList1);
        List<Version> versionList1 = this.keyValueTable.replaceAll(keyFamily, entryList1).join();
        assertNotNull("Version list1 should not be empty", versionList1);
        assertEquals("Version list1 size should be same as entry list1 size",
                versionList1.size(), entryList1.size());
        log.info("Version list1: {}", versionList1);

        List<Integer> keyList = new ArrayList<>();
        for (int i = 0; i < keyArray.length; i++) {
            keyList.add(keyArray[i]);
        }
        log.info("Key list: {}", keyList);
        List<TableEntry<Integer, String>> getEntryList1 = this.keyValueTable.getAll(keyFamily, keyList).join();
        assertNotNull("Get Entry List1 should not be empty", getEntryList1);
        assertEquals("Get Entry List1 size should be same as get entry list1 size",
                getEntryList1.size(), entryList1.size());
        log.info("Get Entry List1: {}", getEntryList1);
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("Corresponding key for getEntryList should be as inserted",
                    keyList.get(i), getEntryList1.get(i).getKey().getKey());
            assertEquals("Corresponding key for entryList should be as inserted",
                    entryList1.get(i).getKey().getKey(), getEntryList1.get(i).getKey().getKey());
        }

        // Comparing each & every entry values of both entrylist and getEntryList.
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("All string values of both entryList1 and getEntryList1 should be equal",
                    entryList1.get(i).getValue(), getEntryList1.get(i).getValue());

        }

        List<TableKey<Integer>> tableKeyList = new ArrayList<>();
        for (int i = 0; i < getEntryList1.size(); i++) {
            tableKeyList.add(getEntryList1.get(i).getKey());
        }
        log.info("Table Key List: {}", tableKeyList);

        assertNull("No error should be obtained during key remove",
                this.keyValueTable.removeAll(keyFamily, tableKeyList).join());
        log.info("All the requested table entries with specified key & its version are removed");

        List<TableEntry<Integer, String>> getEntryList2 = this.keyValueTable.getAll(keyFamily, keyList).join();
        log.info("Get entry list2: {}", getEntryList2);
        for (int i = 0; i < getEntryList2.size(); i++) {
            assertNull("All table entries fetched for deleted/not available keys should be null",
                    getEntryList2.get(i));
        }

        AssertExtensions.assertSuppliedFutureThrows(
                "",
                () -> this.keyValueTable.removeAll(keyFamily, tableKeyList),
                ex -> ex instanceof NoSuchKeyException
        );
        log.info("Found NoSuchKeyException on attempt to delete already removed key entries");
        log.info("Successfully completed test Conditional delete Key with KeyFamily using its version");
    }

    /**
     * Test case-50: Delete KVT and check KVPs are not accessible
     * @throws UnsupportedOperationException
     */
    @Test
    @Ignore
    public void test9DeleteKVTRetriveKVP() throws UnsupportedOperationException {
        try {
            KeyValueTableInfo newKVT = newKeyValueTableName();

            boolean created = this.controller.createKeyValueTable(SCOPE, newKVT.getKeyValueTableName(),
                    DEFAULT_CONFIG).join();
            assertTrue("The requested KVT must get created", created);
            KeyValueTable<Integer, String> newKeyValueTable = this.keyValueTableFactory.
                    forKeyValueTable(newKVT.getKeyValueTableName(),
                            KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());

            String keyFamily = "KeyFamily1";
            LinkedHashMap<Integer, String> entryMap1 = new LinkedHashMap<>();
            Integer[] keyArray = new Integer[10];
            for (int i = 0; i < keyArray.length; i++) {
                keyArray[i] = getKeyID();
            }
            String[] valueArray1 = new String[10];
            for (int i = 0; i < valueArray1.length; i++) {
                valueArray1[i] = getValue();
            }

            // Adding 10 new entries to the Table only if they are not already present.
            for (int i = 0; i < keyArray.length; i++) {
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

            // Comparing each & every entry values of both entrylist and getEntryList.
            for (int i = 0; i < keyArray.length; i++) {
                assertEquals("All string values of both entryList1 and getEntryList1 should be equal",
                        entryMap1.get(entryKeyList.get(i)), getEntryList1.get(i).getValue());
            }

            boolean deleted = this.controller.deleteKeyValueTable(SCOPE, newKVT.getKeyValueTableName()).join();
            assertTrue("The requested KVT must get deleted", deleted);
            log.info("KVT '{}' got deleted", newKVT.getKeyValueTableName());

            // Any operation performed over the above deleted Table must fail there upon.
            List<TableEntry<Integer, String>> getEntryList2 = newKeyValueTable.getAll(keyFamily, keyList).join();
            for (int i = 0; i < getEntryList2.size(); i++) {
                assertNull("All entries of getEntryList2 should be null", getEntryList2.get(i));
            }
            log.info("Successfully completed test Delete KVT and check KVPs are not accessible");
        } catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException();
        }
    }

    private Integer getKeyID() {
        Integer keyId = random.nextInt(endKeyRange - startKeyRange) + startKeyRange;
        return keyId;
    }

    private String getValue() {
        String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";

        StringBuilder sb = new StringBuilder(valueSize);

        for (int i = 0; i < valueSize; i++) {
            int index = (int) (alphaNumericString.length() * Math.random());
            sb.append(alphaNumericString.charAt(index));
        }
        return sb.toString();
    }

<<<<<<< HEAD
    @Test
    @Override
    @Ignore
    public void testIterators() {
        // TODO: iterators not yet supported server-side.
        super.testIterators();
>>>>>>> Issue 4570: (KeyValue Tables) Client Data Path Implementation (#4687)
    }

=======
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
    @Override
    protected KeyValueTable<Integer, String> createKeyValueTable() {
        val kvt = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);
        return this.keyValueTableFactory.forKeyValueTable(kvt.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER,
                KeyValueTableClientConfiguration.builder().build());
    }

    private KeyValueTableInfo newKeyValueTableName() {
<<<<<<< HEAD
<<<<<<< HEAD
        return new KeyValueTableInfo(SCOPE, String.format("KVT-%d", System.nanoTime()));
=======
        return new KeyValueTableInfo(SCOPE, String.format("KVT_%d", System.nanoTime()));
>>>>>>> Issue 4570: (KeyValue Tables) Client Data Path Implementation (#4687)
=======
        return new KeyValueTableInfo(SCOPE, String.format("KVT-%d", System.nanoTime()));
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
    }

}
