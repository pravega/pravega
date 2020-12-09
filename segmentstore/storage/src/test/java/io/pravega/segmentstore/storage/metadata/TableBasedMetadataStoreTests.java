/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryTableStore;
import java.util.concurrent.ExecutorService;
import lombok.val;
import org.junit.Before;

/**
 *  Note that this is just a test for key-value store. Here the storage is NOT using this implementation.
 */
public class TableBasedMetadataStoreTests extends ChunkMetadataStoreTests {
    @Before
    public void setUp() throws Exception {
        val tableStore = new InMemoryTableStore(executorService());
        metadataStore = new TableBasedMetadataStore("TEST", tableStore, executorService());
    }

    /**
     * Unit tests for {@link TableBasedMetadataStore} with {@link InMemoryChunkStorage} using {@link SimpleStorageTests}.
     */
    public static class TableBasedMetadataSimpleStorageTests extends SimpleStorageTests {

        protected ChunkStorage getChunkStorage() throws Exception {
            return new InMemoryChunkStorage(executorService());
        }

        protected ChunkMetadataStore getMetadataStore() throws Exception {
            TableStore tableStore = new InMemoryTableStore(executorService());
            String tableName = "TableBasedMetadataSimpleStorageTests";
            return new TableBasedMetadataStore(tableName, tableStore, executorService());
        }

        @Override
        protected ChunkMetadataStore getCloneMetadataStore(ChunkMetadataStore metadataStore) throws Exception {
            TableBasedMetadataStore tableBasedMetadataStore = (TableBasedMetadataStore) metadataStore;
            TableStore tableStore = InMemoryTableStore.clone((InMemoryTableStore) tableBasedMetadataStore.getTableStore());
            String tableName =  tableBasedMetadataStore.getTableName();
            val retValue = new TableBasedMetadataStore(tableName, tableStore, executorService());
            TableBasedMetadataStore.copyVersion(tableBasedMetadataStore, retValue);
            return retValue;
        }
    }

    /**
     * Unit tests for {@link TableBasedMetadataStore} with {@link InMemoryChunkStorage} using {@link ChunkedRollingStorageTests}.
     */
    public static class InMemorySimpleStorageRollingTests extends ChunkedRollingStorageTests {
        protected ChunkStorage getChunkStorage() throws Exception {
            return new InMemoryChunkStorage(executorService());
        }

        protected ChunkMetadataStore getMetadataStore() throws Exception {
            TableStore tableStore = new InMemoryTableStore(executorService());
            String tableName = "TableBasedMetadataSimpleStorageTests";
            return new TableBasedMetadataStore(tableName, tableStore, executorService());
        }
    }

    /**
     * Unit tests for {@link TableBasedMetadataStore} with {@link InMemoryChunkStorage} using {@link ChunkedSegmentStorageTests}.
     */
    public static class TableBasedMetadataChunkedSegmentStorageTests extends ChunkedSegmentStorageTests {
        @Override
        public ChunkMetadataStore createMetadataStore() throws Exception {
            TableStore tableStore = new InMemoryTableStore(executorService());
            String tableName = "TableBasedMetadataSimpleStorageTests";
            return new TableBasedMetadataStore(tableName, tableStore, executorService());
        }

        public TestContext getTestContext() throws Exception {
            return new TableBasedMetadataTestContext(executorService());
        }

        public static class TableBasedMetadataTestContext extends TestContext {
            TableBasedMetadataTestContext() {
            }

            TableBasedMetadataTestContext(ExecutorService executorService) throws Exception {
                super(executorService);
            }

            @Override
            public ChunkMetadataStore createMetadataStore() throws Exception {
                return createChunkMetadataStore();
            }

            @Override
            protected TestContext createNewInstance() {
                return new TableBasedMetadataTestContext();
            }

            @Override
            public ChunkMetadataStore getForkedMetadataStore() {
                val thisMetadataStore = (TableBasedMetadataStore) this.metadataStore;
                TableStore tableStore = InMemoryTableStore.clone((InMemoryTableStore) thisMetadataStore.getTableStore());
                String tableName =  thisMetadataStore.getTableName();
                val retValue = new TableBasedMetadataStore(tableName, tableStore, executor);
                TableBasedMetadataStore.copyVersion(thisMetadataStore, retValue);
                return retValue;
            }

            private ChunkMetadataStore createChunkMetadataStore() {
                TableStore tableStore = new InMemoryTableStore(executor);
                String tableName = "TableBasedMetadataSimpleStorageTests";
                return new TableBasedMetadataStore(tableName, tableStore, executor);
            }
        }
    }
}
