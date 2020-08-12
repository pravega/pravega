/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.TableEntry;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the {@link KeyValueTableImpl} class. This uses mocked {@link TableSegment}s so it does not actually
 * verify over-the-wire commands. Integration tests (`io.pravega.test.integration.KeyValueTableImplTests`) cover end-to-end
 * scenarios instead.
 */
public class KeyValueTableImplTests extends KeyValueTableTestBase {
    private static final KeyValueTableInfo KVT = new KeyValueTableInfo("Scope", "KVT");
    private MockConnectionFactoryImpl connectionFactory;
    private MockTableSegmentFactory segmentFactory;
    private MockController controller;
    private KeyValueTable<Integer, String> keyValueTable;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Override
    protected KeyValueTable<Integer, String> createKeyValueTable() {
        return this.keyValueTable;
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        this.connectionFactory = new MockConnectionFactoryImpl();
        this.controller = new MockController("localhost", 0, this.connectionFactory, false);
        this.isScopeCreated = this.controller.createScope(KVT.getScope()).get().booleanValue();
        this.controller.createKeyValueTable(KVT.getScope(), KVT.getKeyValueTableName(),
                KeyValueTableConfiguration.builder().partitionCount(getSegmentCount()).build());
        this.segmentFactory = new MockTableSegmentFactory(getSegmentCount(), executorService());
        this.keyValueTable = new KeyValueTableImpl<>(KVT, this.segmentFactory, this.controller, KEY_SERIALIZER, VALUE_SERIALIZER);
    }

    @After
    public void tearDown() {
        this.keyValueTable.close();
        this.controller.close();
        this.connectionFactory.close();
    }

    /**
     * Tests the {@link KeyValueTable#close()} method.
     */
    @Test
    public void testClose() {
        @Cleanup
        val kvt = createKeyValueTable();
        val iteration = new AtomicInteger(0);
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val entry = TableEntry.notExists(getKey(0), getValue(0, iteration.get()));
            kvt.replaceAll(keyFamily, Collections.singletonList(entry)).join();
        });

        Assert.assertEquals("Unexpected number of open segments before closing.", getSegmentCount(), this.segmentFactory.getOpenSegmentCount());
        kvt.close();
        Assert.assertEquals("Not expecting any open segments after closing.", 0, this.segmentFactory.getOpenSegmentCount());
    }
}
