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

import io.pravega.segmentstore.storage.chunklayer.ChunkManagerRollingTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageManagerTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProviderTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Unit tests for InMemorySimpleStorage.
 */
public class InMemorySimpleStorageTests extends SimpleStorageTests {
    private static ChunkStorageProvider getChunkStorageProvider(Executor executor) throws IOException {
        return new InMemoryChunkStorageProvider(executor);
    }

    protected ChunkStorageProvider getChunkStorage(Executor executor)  throws Exception {
        return getChunkStorageProvider(executor);
    }

    public static class InMemorySimpleStorageRollingTests extends ChunkManagerRollingTests {
        protected ChunkStorageProvider getChunkStorage(Executor executor)  throws Exception {
            return getChunkStorageProvider(executor);
        }
    }

    public static class InMemorySimpleStorageProviderTests extends ChunkStorageProviderTests {
        @Override
        protected ChunkStorageProvider createChunkStorageProvider() throws Exception {
            return getChunkStorageProvider(new ScheduledThreadPoolExecutor(getThreadPoolSize()));
        }
    }

    public static class InMemorySimpleStorage extends ChunkStorageManagerTests {

        @Override
        public ChunkStorageProvider getChunkStorageProvider() throws Exception {
            return InMemorySimpleStorageTests.getChunkStorageProvider(executorService());
        }

        public TestContext getTestContext()  throws Exception {
            return new InMemorySimpleStorageTestContext(executorService());
        }

        public class InMemorySimpleStorageTestContext extends ChunkStorageManagerTests.TestContext {
            InMemorySimpleStorageTestContext(ExecutorService executorService)  throws Exception {
                super(executorService);
            }

            @Override
            public ChunkStorageProvider getChunkStorageProvider() throws Exception {
                return InMemorySimpleStorageTests.getChunkStorageProvider(executorService());
            }
        }
    }
}
