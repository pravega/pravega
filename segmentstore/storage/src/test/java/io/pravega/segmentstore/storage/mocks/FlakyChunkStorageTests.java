/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Unit tests for {@link FlakyChunkStorage} using {@link SimpleStorageTests}.
 */
public class FlakyChunkStorageTests extends SimpleStorageTests {
    @Override
    protected ChunkStorage getChunkStorage() {
        return getFlakyChunkStorage(executorService());
    }

    static io.pravega.segmentstore.storage.mocks.FlakyChunkStorage getFlakyChunkStorage(ScheduledExecutorService executorService) {
        ChunkStorage inner = new InMemoryChunkStorage(executorService);
        return new io.pravega.segmentstore.storage.mocks.FlakyChunkStorage(inner, executorService, Duration.ZERO);
    }

    /*
     * Unit tests for {@link FlakyChunkStorage} using {@link ChunkedRollingStorageTests}.
     */
    public static class FlakyChunkStorageRollingStorageTests extends ChunkedRollingStorageTests {
        @Override
        protected ChunkStorage getChunkStorage() {
            return getFlakyChunkStorage(executorService());
        }
    }

    /**
     * Unit tests for {@link FlakyChunkStorage} using {@link ChunkStorageTests}.
     */
    public static class FlakyChunkStorageTest extends ChunkStorageTests {
        @Override
        protected ChunkStorage createChunkStorage() {
            return getFlakyChunkStorage(executorService());
        }
    }
}
