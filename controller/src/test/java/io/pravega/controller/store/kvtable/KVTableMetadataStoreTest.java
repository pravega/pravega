/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable;

import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.store.kvtable.records.KVTSegmentRecord;
import io.pravega.controller.store.stream.StreamMetadataStore;
import org.junit.Rule;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.rules.Timeout;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertEquals;


/**
 * Stream metadata test.
 */
public abstract class KVTableMetadataStoreTest {

    //Ensure each test completes within 10 seconds.
    @Rule 
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);
    protected KVTableMetadataStore store;
    protected StreamMetadataStore streamStore;
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    protected final String scope = "scope";
    protected final String kvtable1 = "kvt1";
    protected final String kvtable2 = "kvt2";
    protected final KeyValueTableConfiguration configuration1 = KeyValueTableConfiguration.builder().partitionCount(2).build();
    protected final KeyValueTableConfiguration configuration2 = KeyValueTableConfiguration.builder().partitionCount(3).build();

    @Before
    public abstract void setupStore() throws Exception;

    @After
    public abstract void cleanupStore() throws Exception;

    @After
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test
    public void testKVTableMetadataStore() throws InterruptedException, ExecutionException {

        // region createStream
        streamStore.createScope(scope).get();

        byte[] newUUID1 = store.newScope(scope).newId();
        store.createEntryForKVTable(scope, kvtable1, newUUID1, executor);
        long start = System.currentTimeMillis();
        store.createKeyValueTable(scope, kvtable1, configuration1, start, null, executor).get();
        store.setState(scope, kvtable1, KVTableState.ACTIVE, null, executor).get();

        byte[] newUUID2 = store.newScope(scope).newId();
        store.createEntryForKVTable(scope, kvtable2, newUUID2, executor);
        store.createKeyValueTable(scope, kvtable2, configuration2, start, null, executor).get();
        store.setState(scope, kvtable2, KVTableState.ACTIVE, null, executor).get();

        assertEquals(configuration1, store.getConfiguration(scope, kvtable1, null, executor).get());
        // endregion

        // region checkSegments
        List<KVTSegmentRecord> segments = store.getActiveSegments(scope, kvtable1, null, executor).get();
        assertEquals(2, segments.size());

        segments = store.getActiveSegments(scope, kvtable2, null, executor).get();
        assertEquals(3, segments.size());

        // endregion
    }
}
