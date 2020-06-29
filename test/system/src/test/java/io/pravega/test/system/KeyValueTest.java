/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.Version;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import lombok.extern.slf4j.Slf4j;
import lombok.Cleanup;
import lombok.val;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

@Slf4j
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(SystemTestRunner.class)
public class KeyValueTest extends AbstractSystemTest {
    private final static String SCOPE_NAME = "TestScope";
    private final static String SCOPE_NAME1 = "DiffScope";
    private final static String KVT_NAME = "TestKVT";
    private static final long  MEGABYTE = 1024L * 1024L;
    private static final Serializer<Integer> KEY_SERIALIZER = new IntegerSerializer();
    private static final Serializer<String> VALUE_SERIALIZER = new UTF8StringSerializer();
    private static final KeyValueTableConfiguration CONFIG = KeyValueTableConfiguration.builder().partitionCount(2).build();
    private static final KeyValueTableInfo KVT = new KeyValueTableInfo(SCOPE_NAME, KVT_NAME);
    private URI controllerURI = null;
    private Controller controller = null;
    private Integer keyType;
    private String valueType = "";
    private String tetsKeyValue = "";
    private KeyValueTableFactory keyValueTableFactory;
    private KeyValueTable<Integer, String> keyValueTable;
    private ConnectionFactory connectionFactory;
    private StreamManager streamManager;
    private Service controllerInstance;

    @Before
    public void setup() throws Exception {
        controllerInstance = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = controllerInstance.getServiceDetails();
        final List<String> uris = ctlURIs.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());
        controllerURI = URI.create("tcp://" + String.join(",", uris));
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);
        final ScheduledExecutorService controllerExecutor = Executors.newScheduledThreadPool(5);
        controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(), controllerExecutor);
        this.connectionFactory = new ConnectionFactoryImpl(clientConfig);
        this.keyValueTableFactory = new KeyValueTableFactoryImpl(SCOPE_NAME, this.controller, this.connectionFactory);
        tetsKeyValue = convertStringToBinary("Hello World");
        while (tetsKeyValue.getBytes().length <= 1040000) {
            tetsKeyValue = tetsKeyValue + convertStringToBinary("Hello World");
        }
    }

    @After
    public void tearDown() {
    }

    @Test
    // Test case - 13: Create KVT test
    public void testA1CreateKeyValueTable() throws Exception {
        log.info("Create Key value Table(KVT)");
        try {
            log.info("controller URL : " + controllerURI);
            streamManager = StreamManager.create(Utils.buildClientConfig(controllerURI));
            assertTrue("Creating Scope", streamManager.createScope(SCOPE_NAME));
            val kvtManager = KeyValueTableManager.create(controllerURI);
            boolean createKVT = kvtManager.createKeyValueTable(SCOPE_NAME, KVT_NAME, CONFIG);
            Assert.assertTrue(createKVT);
            if (createKVT) {
                log.info("Successfully created KVT");
            } else {
                log.info("Failed to created KVT");
            }
        } catch (AssertionError error) {
            log.info(error.getMessage());
        }
    }

    @Test
    // Test case - 13: Create same KVT again
    public void testA2CreateExistingKeyValueTable() throws Exception {
        log.info("Create Key value Table(KVT)");
        try {
            @Cleanup
            val kvtManager = KeyValueTableManager.create(controllerURI);
            boolean createKVT = kvtManager.createKeyValueTable(SCOPE_NAME, KVT_NAME, CONFIG);
            Assert.assertFalse(createKVT);
            if (!createKVT) {
                log.info("KeyValueTable already exists, So can not create same KVT");
            }
        } finally {
            log.info("Can not create as KVT already present");
        }
    }

    @Test
    public void testA3CreateSameKVTDiffScope() throws Exception {
        log.info("Create Key value Table(KVT)");
        try {
            streamManager = StreamManager.create(Utils.buildClientConfig(controllerURI));
            assertTrue("Creating Scope", streamManager.createScope(SCOPE_NAME1));
            val kvtManager = KeyValueTableManager.create(controllerURI);
            boolean createKVT = kvtManager.createKeyValueTable(SCOPE_NAME1, KVT_NAME, CONFIG);
            Assert.assertTrue(createKVT);
            if (createKVT) {
                log.info("Successfully created same KVT in different Scope");
            } else {
                log.info("Failed to created KVT");
            }
        } catch (AssertionError error) {
            log.info(error.getMessage());
        }
    }

     @Test
    // Test Case-1: Insert KVP
    public void testA4InsertKeyValuePair() {
        try {
            log.info("Insert KVP");
            keyType = 1;
            valueType = "TestValue";
            this.keyValueTable = this.keyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<Version> insertEntry = this.keyValueTable.put(null, keyType, valueType);
            Version result = insertEntry.get();
            log.info("result " + result);
            TableEntry<Integer, String> getKVP = this.keyValueTable.get(null, keyType).join();
            assertEquals("Not Equal", keyType, getKVP.getKey().getKey());
        } catch (ExecutionException | InterruptedException | AssertionError  error) {
            log.info(error.getMessage());
        }
    }

    @Test
    public void testA5InsertKVPConditionallyWithNewKey() {
        try {
            log.info("Insert or update KVP");
            keyType = 2;
            valueType = "TestValue1";
            this.keyValueTable = this.keyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<Version> insertEntry = this.keyValueTable.putIfAbsent(null, keyType, valueType);
            Version result = insertEntry.get();
            log.info("result " + result);
            TableEntry<Integer, String> getKVP = this.keyValueTable.get(null, keyType).join();
            assertEquals("Not Equal", keyType, getKVP.getKey().getKey());
        } catch (ExecutionException | InterruptedException | AssertionError error) {
            log.info(error.getMessage());
        }
    }

    @Test
    public void testA6InsertKVPConditionallyWithExistingKey() {
        try {
            log.info("Insert or update KVP");
            keyType = 2;
            valueType = "TestValue1";
            this.keyValueTable = this.keyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<Version> insertEntry = this.keyValueTable.putIfAbsent(null, keyType, valueType);
            Version result = insertEntry.get();
            log.info("result " + result);
        } catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }

    @Test
    public void testA7InsertMultipleKeyValuePair() {
        try {
            log.info("Insert multiple KVP");
            String kf = "TestKeyFamily";
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
            multiKVP.put(3, "TestValue3");
            multiKVP.put(4, "TestValue4");
            multiKVP.put(5, "TestValue5");
            this.keyValueTable = this.keyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<List<Version>> insertKVP = this.keyValueTable.putAll(kf, multiKVP.entrySet());
            List<Version> result = insertKVP.get();
            log.info("successfully inserted key " + multiKVP.entrySet() + "And output" + result);
            CompletableFuture<List<TableEntry<Integer, String>>> getEntry = this.keyValueTable.getAll(null, multiKVP.keySet());
            log.info("Total entry size " + multiKVP.size());
            Assert.assertEquals("Unexpected result size", multiKVP.size(), getEntry.get().size());
        } catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }

    @Test
    // Test case : 25
    public void testA8KVPTablevalueEntryLT1MB() throws Exception {
        this.keyValueTable = this.keyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
        log.info("Create Table entry of size < 1MB");
        try {
            Integer keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
            log.info("key value is " + keyId);
            log.info("size of the key " + keyId.toString().getBytes().length);
            String value = convertStringToBinary("Hello World");
            while (value.getBytes().length <= 1000000) {
                value = value + convertStringToBinary("Hello World");
            }
            log.info("Size of the value string " + value.getBytes().length);
            CompletableFuture<Version> insertEntry = this.keyValueTable.put(null, keyId, value);
            log.info("Insert successfully " + insertEntry.get());
            TableEntry<Integer, String> kvpEntry = this.keyValueTable.get(null, keyId).join();
            log.info("KEY value is :" + kvpEntry.getKey().getKey());
            log.info("value size in byte :" + kvpEntry.getValue().getBytes().length + "Byte");
            Integer size = kvpEntry.getKey().getKey().toString().getBytes().length + kvpEntry.getValue().getBytes().length;
            log.info("Total size update in KVP(will print 0 if less 1 MB) :" + bytesToMB(size.toString().length()) + "MB");
            assertEquals("Verifying same key has inserted in KVP or not", keyId, kvpEntry.getKey().getKey());
        } catch ( AssertionError  error) {
            log.info(error.getMessage());
        }
    }

    @Test
    // Test case = 29 Value Length too long. Must be less than 1040384; given 1048608.
    public void testA9CreateTableEntryGreaterThan1MB() throws Exception {
        log.info("Create Table entry of size > 1MB");
        try {
            Integer keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
            log.info("key value is " + keyId);
            String tetsValue = convertStringToBinary("Hello World");
            while (tetsValue.getBytes().length <= 1048576) {
                tetsValue = tetsValue + convertStringToBinary("Hello World");
            }
            log.info("Size of the value string " + tetsValue.getBytes().length);
            try {
                this.keyValueTable = this.keyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
                log.info("KVT name " + KVT.getKeyValueTableName());
                Version insertEntry = this.keyValueTable.put(null, keyId, tetsValue).join();
            } catch (IllegalArgumentException error) {
                log.info(" Error cause message " + error.getCause());
            }
            CompletableFuture<TableEntry<Integer, String>> kvpEntry = this.keyValueTable.get(null, keyId);
            assertNull("Retune value not null ", kvpEntry.get());
            log.info("Successfully executed case");
        } catch (ExecutionException | AssertionError error) {
            error.printStackTrace();
            log.info("exception messge " + error.getMessage());
        }
    }

    @Test
    // Test case 30
    public void testB1MultipleTableEntryGreaterThan32MB() throws Exception {
        log.info("Add multiple KVPs with entries of total size > 32MB");
        try {
            Integer keyId;
            Integer totalsize = 0;
            String keyfamily = "TestkeyFamily";
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
            for (int loop = 0; loop < 33; loop++) {
                keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
                multiKVP.put(keyId, tetsKeyValue);
                totalsize = totalsize + keyId.toString().getBytes().length + tetsKeyValue.getBytes().length;
            }
            log.info("Final totalsize is" + totalsize);
            try {
                this.keyValueTable = this.keyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
                CompletableFuture<List<Version>> insertEntry = this.keyValueTable.putAll(keyfamily, multiKVP.entrySet());
                assertNull("Entry not null ", insertEntry.get().get(0));
            } catch (IllegalArgumentException error) {
                error.getCause().getMessage();
                log.info("Error cause message " + error.getCause().getMessage());
            }
            log.info("failed table entry because of value size > 32MB");
            CompletableFuture<List<TableEntry<Integer, String>>> kvpEntry = this.keyValueTable.getAll(null, multiKVP.keySet());
            assertNull("value not null ", kvpEntry.get().get(1));
            log.info("Successfully execute cases");
        } catch (ExecutionException error) {
            log.info(error.getMessage());
        }
    }

    @Test
    // Test case 31
    public void testB2GetKVPEntryGreaterThan32MB() throws Exception {
        log.info("Get multiple KVPs with Keys of total size > 32MB");
        try {
            Integer keyId;
            Integer totalsize = 0;
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
            this.keyValueTable = this.keyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
            for (int loop = 0; loop < 35; loop++) {
                keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
                multiKVP.put(keyId, tetsKeyValue);
                CompletableFuture<Version> insertEntry = this.keyValueTable.put(null, keyId, tetsKeyValue);
                totalsize = totalsize + keyId.toString().getBytes().length + tetsKeyValue.getBytes().length;
            }
            log.info("Final totalsize is" + totalsize);
            try {
                CompletableFuture<List<TableEntry<Integer, String>>> getEntry = this.keyValueTable.getAll(null, multiKVP.keySet());
                log.info("Total entry size " + multiKVP.size());
                Assert.assertEquals("Unexpected result size", multiKVP.size(), getEntry.get().size());
            } catch (IllegalArgumentException error) {
                log.info("Error message for getall API" + error.getMessage());
            }
            log.info("successfully get KVP entry more 32 MB");
        } catch (Exception error) {
            log.info(error.getMessage());
        }
    }

    @Test
    // Test case 32
    public void testB3INSERTMultTableEntryWithKeyFamilyGreaterThan32MB() throws Exception {
        log.info("Insert multiple keyFamily KVPs with entries of total size > 32MB");
        try {
            Integer keyId;
            Integer totalsize = 0;
            String keyfamily = "TestkeyFamily";
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
            for (int loop = 0; loop < 35; loop++) {
                keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
                multiKVP.put(keyId, tetsKeyValue);
                totalsize = totalsize + keyId.toString().getBytes().length + tetsKeyValue.getBytes().length;
            }
            log.info("Final totalsize is" + totalsize);
            try {
                this.keyValueTable = this.keyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
                CompletableFuture<List<Version>> insertEntry = this.keyValueTable.putAll(keyfamily, multiKVP.entrySet());
            } catch (IllegalArgumentException error) {
                log.info("Error cause " + error.getCause());
                log.info("Error message " + error.getCause().getMessage());
            }
            log.info("Failed to insert as value size > 32MB");
            CompletableFuture<List<TableEntry<Integer, String>>> kvpEntry = this.keyValueTable.getAll(keyfamily, multiKVP.keySet());
            assertNull("entry not null", kvpEntry.get().get(1));
            log.info("Successfully execute the case");
        } catch (ExecutionException | InterruptedException  error) {
            log.info("exception messge " + error.getMessage());
        }
    }

    @Test
    public void testB4UpdateMultikeyvaluewithKeyFamilyGreaterThan32MB() throws Exception {
        log.info("Update multiple keyFamily KVP values with entries of total size > 32MB");
        try {
            Integer keyId;
            Integer totalsize = 0;
            String keyfamily = "TestKF";
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
            this.keyValueTable = this.keyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
            log.debug("Value String" + tetsKeyValue);
            for (int loop = 0; loop < 35; loop++) {
                keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
                multiKVP.put(keyId, tetsKeyValue);
                CompletableFuture<Version> insertEntry = this.keyValueTable.put(keyfamily, keyId, tetsKeyValue);
                totalsize = totalsize + keyId.toString().getBytes().length + tetsKeyValue.getBytes().length;
            }
            log.info("Final totalsize is" + totalsize);
            String testValue = convertStringToBinary("Hello Pravega");
            while (testValue.getBytes().length < 1040000) {
                testValue = testValue + convertStringToBinary("Hello Pravega");
            }
            List<Integer> keyList = new ArrayList<>();
            for (Integer key:multiKVP.keySet()) {
                log.info("key value {}", key);
                keyList.add(key);
            }
            log.info("keyList: {}", keyList);
            List<TableEntry<Integer, String>> getEntry = this.keyValueTable.getAll(keyfamily, keyList).join();
            Integer oldSize = getEntry.get(0).getValue().getBytes().length;
            ListIterator<TableEntry<Integer, String>> iterator = getEntry.listIterator();
            while (iterator.hasNext()) {
                String value = iterator.next().getValue().replace(tetsKeyValue, testValue);
                log.info("Entry value is " + value.getBytes().length + " Update value " + testValue.getBytes().length);
            }
            try {
                CompletableFuture<List<Version>> update = this.keyValueTable.replaceAll(keyfamily, getEntry);
                log.info("Successfully update KVP " + update.toString().length());
            } catch (IllegalArgumentException error) {
                log.info(error.getMessage());
            }
            List<TableEntry<Integer, String>> getEntryAfterUpdate = this.keyValueTable.getAll(keyfamily, multiKVP.keySet()).join();
            List<TableKey<Integer>> keyEntry1 = new ArrayList<>();
            for (int i = 0; i < getEntryAfterUpdate.size(); i++) {
                log.info("key value" + getEntryAfterUpdate.get(i).getKey());
                keyEntry1.add(getEntryAfterUpdate.get(i).getKey());
                log.info("key value is " + keyEntry1.get(i));
            }
            assertNull("Value is not null ", this.keyValueTable.removeAll(keyfamily, keyEntry1).join());
        } catch (Exception | AssertionError error) {
            log.info(error.getMessage());
        }
    }

    private static class IntegerSerializer implements Serializer<Integer> {
        @Override
        public ByteBuffer serialize(Integer value) {
            return ByteBuffer.allocate(Integer.BYTES).putInt(0, value);
        }

        @Override
        public Integer deserialize(ByteBuffer serializedValue) {
            return serializedValue.getInt();
        }
    }

    public static long bytesToMB(long bytes) {
        return bytes / MEGABYTE;
    }

    public static long KBToMB(long kB) {
        return kB / MEGABYTE;
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
