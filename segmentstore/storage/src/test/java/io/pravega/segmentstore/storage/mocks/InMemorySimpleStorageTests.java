/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.chunklayer.ChunkManagerRollingTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageManagerConfig;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageManagerTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProviderTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * Unit tests for {@link InMemorySimpleStorage} using {@link SimpleStorageTests}.
 */
public class InMemorySimpleStorageTests extends SimpleStorageTests {
    private static ChunkStorageProvider getChunkStorageProvider() throws IOException {
        return new InMemoryChunkStorageProvider();
    }

    protected ChunkStorageProvider getChunkStorage() throws Exception {
        return getChunkStorageProvider();
    }

    /**
     * Unit tests for {@link InMemorySimpleStorage} using {@link ChunkManagerRollingTests}.
     */
    public static class InMemorySimpleStorageRollingTests extends ChunkManagerRollingTests {
        protected ChunkStorageProvider getChunkStorage() throws Exception {
            return getChunkStorageProvider();
        }
    }

    /**
     * Unit tests for {@link InMemorySimpleStorage} using {@link ChunkStorageProviderTests}.
     */
    public static class InMemorySimpleStorageProviderTests extends ChunkStorageProviderTests {
        @Override
        protected ChunkStorageProvider createChunkStorageProvider() throws Exception {
            return getChunkStorageProvider();
        }
    }

    /**
     * Unit tests for {@link InMemorySimpleStorage} using {@link ChunkStorageManagerTests}.
     */
    public static class InMemorySimpleStorage extends ChunkStorageManagerTests {

        @Override
        public ChunkStorageProvider createChunkStorageProvider() throws Exception {
            return InMemorySimpleStorageTests.getChunkStorageProvider();
        }

        @Override
        public TestContext getTestContext() throws Exception {
            return new InMemorySimpleStorageTestContext(executorService());
        }

        @Override
        public TestContext getTestContext(ChunkStorageManagerConfig config) throws Exception {
            return new InMemorySimpleStorageTestContext(executorService(), config);
        }

        public class InMemorySimpleStorageTestContext extends ChunkStorageManagerTests.TestContext {
            InMemorySimpleStorageTestContext(ExecutorService executorService) throws Exception {
                super(executorService);
            }

            InMemorySimpleStorageTestContext(ExecutorService executorService, ChunkStorageManagerConfig config) throws Exception {
                super(executorService, config);
            }

            @Override
            public ChunkStorageProvider createChunkStorageProvider() throws Exception {
                return InMemorySimpleStorageTests.getChunkStorageProvider();
            }
        }
    }
}
