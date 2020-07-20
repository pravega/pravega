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

import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.Version;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.services.Service;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;


@Slf4j
abstract class AbstractKVSFailoverTest extends AbstractSystemTest {
    static final int WAIT_AFTER_FAILOVER_MILLIS = 40 * 1000;
    private static final Serializer<Integer> KEY_SERIALIZER = new IntegerSerializer();
    private static final Serializer<String> VALUE_SERIALIZER = new UTF8StringSerializer();
    KeyValueTableFactory keyValueTableFactory;
    KeyValueTableConfiguration config = KeyValueTableConfiguration.builder().partitionCount(2).build();
    Service controllerInstance;
    Service segmentStoreInstance;
    URI controllerURIDirect = null;
    Controller controller;
    ScheduledExecutorService controllerExecutorService;
    int kvpListCount = 0;
    private KeyValueTable<Integer, String> keyValueTable;

    private static class IntegerSerializer implements Serializer<Integer> {
        public ByteBuffer serialize(Integer value) {
            return ByteBuffer.allocate(Integer.BYTES).putInt(0, value);
        }

        public Integer deserialize(ByteBuffer serializedValue) {
            return serializedValue.getInt();
        }
    }

    public static String convertStringToBinary(String input) {
        StringBuilder result = new StringBuilder();
        char[] chars = input.toCharArray();
        for (char aChar : chars) {
            result.append(String.format("%8s", Integer.toBinaryString(aChar)).replaceAll(" ", "0"));
        } return result.toString();
    }

    void createScope(String scopeName, StreamManager streamManager) {
        Boolean createScopeStatus = streamManager.createScope(scopeName);
        if (createScopeStatus) {
            log.info("Successfully created Scope {}", scopeName);
        }
    }

    void createKVT(String scopeName, String kvtName, KeyValueTableConfiguration config, URI controllerURIDirect) {
            log.info("KVT name {}", kvtName);
            Boolean createKvtStatus = KeyValueTableManager.create(controllerURIDirect).createKeyValueTable(scopeName, kvtName, config);
            if (createKvtStatus) {
                log.info("Successfully created KVT {}", kvtName);
            }
    }

    void performFailoverTest() throws ExecutionException {
        int kvpEntryCount = getListKvp();
        log.info("Current insert KVP count {}", kvpEntryCount);
        log.info("ScaleUp one Segmentstore and Controller");

        // Case-64 Scale up SegmentStore one instances from 3 to 4
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(4), ExecutionException::new);
        // Case-60 Scale up controller one instances from 2 to 3
        Futures.getAndHandleExceptions(controllerInstance.scaleService(3), ExecutionException::new);
        log.info("Successfully scale up Controller 2 -> 3 and Segmentstore 3 -> 4");
        log.info("Sleeping for {} ", WAIT_AFTER_FAILOVER_MILLIS);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        Assert.assertTrue("Current KVP entry count is not greater than previousKVPCount", kvpEntryCount < getListKvp());
        kvpEntryCount = getListKvp();
        log.info("Insert KVP count: {} without any failover after sleep before scaling", kvpEntryCount);
        log.info("ScaleUp more than one Segmentstore and Controller");

        // Case-65 Scale up SegmentStore to 2 instances from 4 to 6
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(6), ExecutionException::new);
        // Case-61 Scale up controller two instances from 3 to 5
        Futures.getAndHandleExceptions(controllerInstance.scaleService(5), ExecutionException::new);
        log.info("Successfully scale up Segmentstore 4 -> 6 and Controller 3 -> 5");
        log.info("Sleeping for {} ", WAIT_AFTER_FAILOVER_MILLIS);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        Assert.assertTrue("Current KVP count is not greater than previousKVPCount", kvpEntryCount < getListKvp());
        kvpEntryCount = getListKvp();
        log.info("Insert KVP count: {} without any failover after sleep before more than one component scaling", kvpEntryCount);

        // Case-66 Scale Down SegmentStore to more than one instances from 6 to 3
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(3), ExecutionException::new);
        // Case-62 Scale Down controller to more than one instances from 5 to 2
        Futures.getAndHandleExceptions(controllerInstance.scaleService(2), ExecutionException::new);
        log.info("Successfully scale down Segmentstore 6 -> 3 and Controller 5 -> 2 ");
        log.info("Sleeping for {} ", WAIT_AFTER_FAILOVER_MILLIS);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        Assert.assertTrue("Current list is not greater than initialKVPCount", kvpEntryCount < getListKvp());
        kvpEntryCount = getListKvp();
        log.info("Insert KVP count: {} without any failover after sleep and more than one component scale down", kvpEntryCount);

        // Case-67 Scale Down SegmentStore one instances from 3 to 2
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(1), ExecutionException::new);
        // Case-63 Scale Down controller to one instances from 2 to 1
        Futures.getAndHandleExceptions(controllerInstance.scaleService(1), ExecutionException::new);
        log.info("Successfully scale down Segmentstore 3 -> 2 and Controller 2 -> 1");
        log.info("Sleeping for {} ", WAIT_AFTER_FAILOVER_MILLIS);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        Assert.assertTrue("Current list is not greater than initialKVPCount", kvpEntryCount < getListKvp());
        kvpEntryCount = getListKvp();
        log.info("Insert KVP count: {} without any failover after sleep and one component scale down", kvpEntryCount);
    }

    void startKVPCreate(String scope, String kvtName, int kvpCount, KeyValueTableFactory keyValueTableFactory, URI controllerURIDirect) {
        testInsertUpdateGetKVP(scope, kvtName, kvpCount, keyValueTableFactory, controllerURIDirect);
    }

    private CompletableFuture<Void> testInsertUpdateGetKVP(String scope, String kvtName, int kvpCount, KeyValueTableFactory keyValueTableFactory, URI controllerURIDirect) {
        String value = convertStringToBinary("Hello World");
        return CompletableFuture.runAsync(() -> {
                    log.info("KVT Table name {}", kvtName);
                    KeyValueTableInfo kvt = new KeyValueTableInfo(scope, kvtName);
                keyValueTable = keyValueTableFactory.forKeyValueTable(kvt.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
                for (Integer keyId = 1; keyId < kvpCount; keyId++) {
                    log.info("KVP entry Key ID value {}", keyId);
                    // Insert KVP with key and value
                    Version insertKvp = keyValueTable.put(null, keyId, value).join();

                    // get KVP and verify the insert value length
                    Assert.assertEquals("Value length is not matching", value.length(),
                            keyValueTable.get(null, keyId).join().getValue().length());

                    // Replace existing KVP entry value with new value
                    Version keyVersion = keyValueTable.get(null, keyId).join().getKey().getVersion();
                    String replaceValue = value + convertStringToBinary("Hello Pravega");
                    Version replaceKvp = keyValueTable.replace(null, keyId, replaceValue, keyVersion).join();
                    Assert.assertEquals("Value length is not matching", replaceValue.length(), keyValueTable.get(null, keyId).join().getValue().length());
                    kvpListCount = kvpListCount + 1;
                    log.info("KVP list count {}", kvpListCount);
                }
        });

    }

    private int getListKvp() {
        return kvpListCount;
    }
}
