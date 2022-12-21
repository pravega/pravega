/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.noop;

import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import org.junit.Test;

/**
 * Unit tests for {@link ConditionalNoOpChunkStorage} using {@link SimpleStorageTests}.
 */
public class ConditionalNoOpSimpleStorageTests extends SimpleStorageTests {
    @Override
    protected ChunkStorage getChunkStorage() throws Exception {
        return new ConditionalNoOpChunkStorage(new InMemoryChunkStorage(executorService()), executorService());
    }

    @Override
    protected void populate(byte[] data) {
        // Do nothing keep data uninitialized.
    }

    /**
     * Unit tests for {@link ConditionalNoOpRollingStorageTests} using {@link ChunkedRollingStorageTests}.
     */
    public static class ConditionalNoOpRollingStorageTests extends ChunkedRollingStorageTests {
        @Override
        protected ChunkStorage getChunkStorage() {
            return new ConditionalNoOpChunkStorage(new InMemoryChunkStorage(executorService()), executorService());
        }

        @Override
        protected void populate(byte[] data) {
            // Do nothing keep data uninitialized.
        }
    }

    /**
     * Unit tests for {@link ConditionalNoOpChunkStorageTests} using {@link ChunkStorageTests}.
     */
    public static class ConditionalNoOpChunkStorageTests extends ChunkStorageTests {
        @Override
        protected ChunkStorage createChunkStorage() {
            return new ConditionalNoOpChunkStorage(new InMemoryChunkStorage(executorService()), executorService());
        }

        @Override
        protected void populate(byte[] data) {
            // Do nothing keep data uninitialized.
        }
    }

    /**
     * Unit tests for {@link ConditionalNoOpChunkStorageMetadataTest} using {@link ChunkStorageTests}.
     */
    public static class ConditionalNoOpChunkStorageMetadataTest extends ChunkStorageTests {

        @Override
        protected ChunkStorage createChunkStorage() {
            return new ConditionalNoOpChunkStorage(new InMemoryChunkStorage(executorService()), executorService());
        }

        /**
         * Test basic chunk lifecycle.
         */
        @Override
        @Test
        public void testChunkLifeCycle() throws Exception {
            testChunkLifeCycle("_system/testchunk");
        }

        @Override
        @Test
        public void testChunkLifeCycleCreateWithContent() throws Exception {
            testChunkLifeCycleCreateWithContent("_system/testchunk");
        }

        /**
         * Test basic read and write.
         */
        @Override
        @Test
        public void testSimpleReadWrite() throws Exception {
            testSimpleReadWrite("_system/testchunk");
        }

        @Override
        @Test
        public void testSimpleReadWriteCreateWithContent() throws Exception {
           testSimpleReadWriteCreateWithContent("_system/testchunk");
        }

        @Override
        protected void populate(byte[] data) {
            // Do nothing keep data uninitialized.
        }

    }
}
