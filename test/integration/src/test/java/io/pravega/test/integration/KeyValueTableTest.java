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

import io.netty.handler.codec.TooLongFrameException;
import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.tables.Version;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.client.tables.impl.KeyValueTableTestBase;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;

import java.time.Duration;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.CompletableFuture;
import java.util.Iterator;
import java.util.Collections;
import java.util.List;

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
@Slf4j
public class KeyValueTableTest extends KeyValueTableTestBase {
    private static final String ENDPOINT = "localhost";
    private static final String SCOPE = "Scope";
    private static final KeyValueTableConfiguration DEFAULT_CONFIG = KeyValueTableConfiguration.builder().partitionCount(5).build();
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final long  MEGABYTE = 1024L * 1024L;
    private ServiceBuilder serviceBuilder;
    private TableStore tableStore;
    private PravegaConnectionListener serverListener = null;
    private ConnectionFactory connectionFactory;
    private TestingServer zkTestServer = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller;
    private KeyValueTableFactory keyValueTableFactory;
    private KeyValueTable<Integer, String> keyValueTable;
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = ENDPOINT;
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;


    @Before
    public void setup() throws Exception {
        super.setup();

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SegmentStore service.
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
        System.gc();
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
    }

    /**
     * Smoke Test. Verify that the KeyValueTable can be created and deleted.
     */
    @Test
    public void testCreateDeleteKeyValueTable() {
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

    }

    @Test
    public void testCreateGetUpdateDeleteLargeSizeValue() {
        Integer keyId = generateKeyId();
        String valueLT1MB = generateValue("Hello World", 1040000);
        keyValueTable = createKeyValueTable();

        // 1. Inserting entry in KVP with large value size but less than 1 MB
        Version insertEntry = keyValueTable.put(null, keyId, valueLT1MB).join();
        log.info("Put inserted " + insertEntry);

        // 2. Get the entry from KVP having large value size but less than 1 MB
        Assert.assertEquals(valueLT1MB.length(), keyValueTable.get(null, keyId).join().getValue().length());

        // 3. Replace entry of KVP having large value size but less than 1 MB
        String replaceValue = generateValue("Hello Pravega", 1040000);
        Version replaceEntry = keyValueTable.replace(null, keyId, replaceValue, insertEntry).join();
        Assert.assertEquals(replaceValue.length(), keyValueTable.get(null, keyId).join().getValue().length());

        // 4. Delete entry from KVP having large value size but less than 1 MB
        log.info("Get key value: {}", keyValueTable.get(null, keyId).join().getKey());
        keyValueTable.remove(null, keyId).join();
        Assert.assertNull("Not null", keyValueTable.get(null, keyId).join());

        // 5. Insert entry in KVP with Keyfamily size greater than 1KB
        String keyF = generateValue("TestKeyFamily", 1024);
        keyId = generateKeyId();
        try {
            Assert.assertNull("Not Null", keyValueTable.put(keyF, keyId, valueLT1MB).join());
        } catch (IllegalArgumentException error) {
            error.getMessage();
        }
        Assert.assertNull("Not null", keyValueTable.get(null, keyId).join());

        // 6. Inserting entry in KVP with large value size greater than 1 MB (1048576 byte)
        keyId = generateKeyId();
        String valueGT1MB = generateValue("Hello World", 1048576);
        log.info("Key value size: {} ", valueGT1MB.length());
        try {
            Assert.assertNull("Not Null", keyValueTable.put(null, keyId, valueGT1MB).join());
        } catch (IllegalArgumentException error) {
            error.getMessage();
        }
        Assert.assertNull("Not null", keyValueTable.get(null, keyId).join());

        //7. Add key size more than 8KB
        String largeKey = "Key";
        String value1 = "Hello world";
        while (largeKey.getBytes().length < 8192) {
            largeKey = largeKey + "TestLargeKey";
        }
        log.info("key size: {}", largeKey.length());
        Assert.assertNotNull(TableEntry.notExists(largeKey, value1));
    }

    @Test
    public void testCrtGetUpdDelWithKeyFamilyLargeSizeValue() {
        String keyFamily = "TestkeyFamily";
        Integer keyId;
        String valueLT1MB = generateValue("Hello World", 1040000);

        // 8. Inserting multi entry in KVP with size greater than 32MB
        // Used 33 loop and in each loop inserting key and value approximately less than 1 MB
        keyValueTable = createKeyValueTable();
        Map<Integer, String> multiKVP = new HashMap<>();
        for (int loop = 0; loop < 33; loop++) {
            keyId = generateKeyId();
            multiKVP.put(keyId, valueLT1MB);
        }
        CompletableFuture<List<Version>> insertMultiEntry = keyValueTable.putAll(keyFamily, multiKVP.entrySet());
        for (int i = 0; i < multiKVP.size(); i++) {
            Assert.assertNull(keyValueTable.getAll(keyFamily, multiKVP.keySet()).join().get(i));
        }

        // 19. Retrieve KVP entry size greater than 32MB,
        // First adding multiple KBP entry one by one in tableEntry and each entry size just less than 1 MB
        Map<Integer, String> multiKVP1 = new HashMap<>();
        List<TableKey<Integer>> tableKeyEntry = new ArrayList<>();
        List<TableEntry<Integer, String>> replaceEntry = new ArrayList<>();
        for (int loop = 0; loop < 33; loop++) {
            keyId = generateKeyId();
            multiKVP1.put(keyId, valueLT1MB);
            tableKeyEntry.add(TableKey.unversioned(keyId));
            CompletableFuture<Version> multiEntry = keyValueTable.put(keyFamily, keyId, valueLT1MB);
        }
        try {
            val getAllEntry = keyValueTable.getAll(keyFamily, multiKVP1.keySet()).join().stream();
        } catch (CompletionException | RetriesExhaustedException | TooLongFrameException error) {
            error.getMessage();
        }

        // 10. Replace KVP entry size greater than 32MB
        String replaceValue = generateValue("Hello Pravega", 1040000);
        Map<Integer, String> multiKVP2 = new HashMap<>();
        for (int i = 0; i < 35; i++) {
            keyId = generateKeyId();
            replaceEntry.add(TableEntry.notExists(keyId, replaceValue));
            multiKVP2.put(keyId, replaceValue);
        }
        CompletableFuture<List<Version>> versionList = keyValueTable.replaceAll(keyFamily, replaceEntry);
        Assert.assertNull(keyValueTable.getAll(keyFamily, multiKVP2.keySet()).join().get(1));
        // 11. Delete KVP entry size greater than 32MB
        Assert.assertNull("Not null ", keyValueTable.removeAll(keyFamily, tableKeyEntry).join());
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
        return new KeyValueTableInfo(SCOPE, String.format("KVT-%d", System.nanoTime()));
    }

    private Integer generateKeyId() {
        return ThreadLocalRandom.current().nextInt(1000, 99999);
    }

    // Generate large size kvp value, used size 1048576 for greater than 1MB
    public static String generateValue(String value, int size) {
        String largeSizeValue = convertStringToBinary(value);
        while (largeSizeValue.getBytes().length <= size) {
            largeSizeValue = largeSizeValue + convertStringToBinary(value);
        }
        return largeSizeValue;
    }

    public static String convertStringToBinary(String input) {

        StringBuilder result = new StringBuilder();
        char[] chars = input.toCharArray();
        for (char aChar : chars) {
            result.append(String.format("%8s", Integer.toBinaryString(aChar)).replaceAll(" ", "0")
            );
        }
        return result.toString();

    }

}
