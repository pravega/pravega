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

import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Unit tests for {@link InMemorySimpleStorage} using {@link SimpleStorageTests}.
 */
public class InMemorySimpleStorageTests extends SimpleStorageTests {
    @Override
    protected ChunkStorage getChunkStorage() {
        return new InMemoryChunkStorage(executorService());
    }

    /**
     * Unit tests for {@link InMemorySimpleStorage} using {@link ChunkedRollingStorageTests}.
     */
    public static class InMemorySimpleStorageRollingStorageTests extends ChunkedRollingStorageTests {
        @Override
        protected ChunkStorage getChunkStorage() {
            return new InMemoryChunkStorage(executorService());
        }
    }

    /**
     * Unit tests for {@link InMemorySimpleStorage} using {@link ChunkStorageTests}.
     */
    public static class InMemoryChunkStorageTests extends ChunkStorageTests {
        @Override
        protected ChunkStorage createChunkStorage() {
            return new InMemoryChunkStorage(executorService());
        }
    }

    /**
     * Unit tests for {@link InMemorySimpleStorage} using {@link ChunkedSegmentStorageTests}.
     */
    public static class InMemorySimpleStorage extends ChunkedSegmentStorageTests {

        @Override
        public ChunkStorage createChunkStorage() {
            return new InMemoryChunkStorage(executorService());
        }

        @Override
        public TestContext getTestContext() throws Exception {
            return new InMemorySimpleStorageTestContext(executorService());
        }

        @Override
        public TestContext getTestContext(ChunkedSegmentStorageConfig config) throws Exception {
            return new InMemorySimpleStorageTestContext(executorService(), config);
        }

        @Override
        protected void populate(byte[] data) {
            rnd.nextBytes(data);
        }

        @Override
        protected void checkData(byte[] expected, byte[] output) {
            Assert.assertArrayEquals(expected, output);
        }

        @Override
        protected void checkData(byte[] expected, byte[] output, int expectedStartIndex, int outputStartIndex, int length) {
            AssertExtensions.assertArrayEquals("Data check failed", expected, expectedStartIndex, output, outputStartIndex, length);
        }

        @Override
        public void testReadHugeChunks() {
            // Do not execute this test because it creates very large chunks (few multiples of Integer.MAX_VALUE).
            // Allocating such huge byte arrays is not desirable with InMemoryChunkStorage.
        }

        @Override
        public void testConcatHugeChunks(){
            // Do not execute this test because it creates very large chunks (few multiples of Integer.MAX_VALUE).
            // Allocating such huge byte arrays is not desirable with InMemoryChunkStorage.
        }

        @Override
        public void testRelocateHugeChunks(){
            // Do not execute this test because it creates very large chunks (few multiples of Integer.MAX_VALUE).
            // Allocating such huge byte arrays is not desirable with InMemoryChunkStorage.
        }

        public class InMemorySimpleStorageTestContext extends ChunkedSegmentStorageTests.TestContext {
            InMemorySimpleStorageTestContext(ScheduledExecutorService executorService) throws Exception {
                super(executorService);
            }

            InMemorySimpleStorageTestContext(ScheduledExecutorService executorService, ChunkedSegmentStorageConfig config) throws Exception {
                super(executorService, config);
            }

            @Override
            public ChunkStorage createChunkStorage() {
                return new InMemoryChunkStorage(executorService());
            }
        }
    }
}
