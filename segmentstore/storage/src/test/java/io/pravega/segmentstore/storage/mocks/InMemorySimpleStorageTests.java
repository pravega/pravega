/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * Unit tests for {@link InMemorySimpleStorage} using {@link SimpleStorageTests}.
 */
public class InMemorySimpleStorageTests extends SimpleStorageTests {
    private static ChunkStorage getChunkStorageProvider() throws IOException {
        return new InMemoryChunkStorage();
    }

    protected ChunkStorage getChunkStorage() throws Exception {
        return getChunkStorageProvider();
    }

    /**
     * Unit tests for {@link InMemorySimpleStorage} using {@link ChunkedRollingStorageTests}.
     */
    public static class InMemorySimpleStorageRollingStorageTests extends ChunkedRollingStorageTests {
        protected ChunkStorage getChunkStorage() throws Exception {
            return getChunkStorageProvider();
        }
    }

    /**
     * Unit tests for {@link InMemorySimpleStorage} using {@link ChunkStorageTests}.
     */
    public static class InMemorySimpleStorageProviderTests extends ChunkStorageTests {
        @Override
        protected ChunkStorage createChunkStorage() throws Exception {
            return getChunkStorageProvider();
        }
    }

    /**
     * Unit tests for {@link InMemorySimpleStorage} using {@link ChunkedSegmentStorageTests}.
     */
    public static class InMemorySimpleStorage extends ChunkedSegmentStorageTests {

        @Override
        public ChunkStorage createChunkStorageProvider() throws Exception {
            return InMemorySimpleStorageTests.getChunkStorageProvider();
        }

        @Override
        public TestContext getTestContext() throws Exception {
            return new InMemorySimpleStorageTestContext(executorService());
        }

        @Override
        public TestContext getTestContext(ChunkedSegmentStorageConfig config) throws Exception {
            return new InMemorySimpleStorageTestContext(executorService(), config);
        }

        public class InMemorySimpleStorageTestContext extends ChunkedSegmentStorageTests.TestContext {
            InMemorySimpleStorageTestContext(ExecutorService executorService) throws Exception {
                super(executorService);
            }

            InMemorySimpleStorageTestContext(ExecutorService executorService, ChunkedSegmentStorageConfig config) throws Exception {
                super(executorService, config);
            }

            @Override
            public ChunkStorage createChunkStorageProvider() throws Exception {
                return InMemorySimpleStorageTests.getChunkStorageProvider();
            }
        }
    }
}
