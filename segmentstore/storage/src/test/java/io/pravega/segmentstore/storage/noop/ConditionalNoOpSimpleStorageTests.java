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

import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkNotFoundException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

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
        @Test
        public void testChunkLifeCycle() throws Exception {
            // try reading non existent chunk
            String chunkName = "_system_testchunk";
            testNotExists(chunkName);

            ChunkStorage chunkStorage = super.getChunkStorage();
            // Perform basic operations.
            ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, 1, new ByteArrayInputStream(new byte[1])).get();
            assertEquals(chunkName, chunkHandle.getChunkName());
            assertFalse(chunkHandle.isReadOnly());

            chunkHandle = chunkStorage.openRead(chunkName).get();
            assertEquals(chunkName, chunkHandle.getChunkName());
            assertTrue(chunkHandle.isReadOnly());

            chunkHandle = chunkStorage.openWrite(chunkName).get();
            assertEquals(chunkName, chunkHandle.getChunkName());
            assertFalse(chunkHandle.isReadOnly());
            testAlreadyExists(chunkName);

            chunkStorage.delete(chunkHandle).join();
            testNotExists(chunkName);
        }

        @Test
        public void testChunkLifeCycleCreateWithContent() throws Exception {
            // try reading non existent chunk
            String chunkName = "_system_testchunk";
            testNotExists(chunkName);

            ChunkStorage chunkStorage = super.getChunkStorage();
            // Perform basic operations.
            byte[] writeBuffer = new byte[10];
            populate(writeBuffer);
            ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
            assertEquals(chunkName, chunkHandle.getChunkName());
            assertFalse(chunkHandle.isReadOnly());

            chunkHandle = chunkStorage.openRead(chunkName).get();
            assertEquals(chunkName, chunkHandle.getChunkName());
            assertTrue(chunkHandle.isReadOnly());

            chunkHandle = chunkStorage.openWrite(chunkName).get();
            assertEquals(chunkName, chunkHandle.getChunkName());
            assertFalse(chunkHandle.isReadOnly());
            testAlreadyExists(chunkName);

            chunkStorage.delete(chunkHandle).join();
            testNotExists(chunkName);
        }

        /**
         * Test basic read and write.
         */
        @Test
        public void testSimpleReadWrite() throws Exception {
            ChunkStorage chunkStorage = super.getChunkStorage();
            if (!chunkStorage.supportsAppend()) {
                return;
            }

            String chunkName = "_system_testchunk";

            // Create.
            ChunkHandle chunkHandle = chunkStorage.create(chunkName).get();
            assertEquals(chunkName, chunkHandle.getChunkName());
            assertFalse(chunkHandle.isReadOnly());

            // Write.
            byte[] writeBuffer = new byte[10];
            populate(writeBuffer);
            int bytesWritten = chunkStorage.write(chunkHandle, 0, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
            assertEquals(writeBuffer.length, bytesWritten);

            // Read back.
            byte[] readBuffer = new byte[writeBuffer.length];
            int bytesRead = chunkStorage.read(chunkHandle, 0, writeBuffer.length, readBuffer, 0).get();
            assertEquals(writeBuffer.length, bytesRead);
            assertArrayEquals(writeBuffer, readBuffer);

            // Delete.
            chunkStorage.delete(chunkHandle).join();
        }

        @Test
        public void testSimpleReadWriteCreateWithContent() throws Exception {
            ChunkStorage chunkStorage = super.getChunkStorage();
            String chunkName = "_system_testchunk";

            // Create.
            byte[] writeBuffer = new byte[10];
            populate(writeBuffer);
            ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
            assertEquals(chunkName, chunkHandle.getChunkName());
            assertFalse(chunkHandle.isReadOnly());

            // Read back.
            byte[] readBuffer = new byte[writeBuffer.length];
            int bytesRead = chunkStorage.read(chunkHandle, 0, writeBuffer.length, readBuffer, 0).get();
            assertEquals(writeBuffer.length, bytesRead);
            assertArrayEquals(writeBuffer, readBuffer);

            // Delete.
            chunkStorage.delete(chunkHandle).join();
        }

        private void testAlreadyExists(String chunkName) throws Exception {
            ChunkStorage chunkStorage = super.getChunkStorage();
            try {
                chunkStorage.create(chunkName).get();
                Assert.fail("ChunkAlreadyExistsException was expected");
            } catch (ExecutionException e) {
            }
        }

        private void testNotExists(String chunkName) throws Exception {
            ChunkStorage chunkStorage = super.getChunkStorage();
            assertFalse(chunkStorage.exists(chunkName).get());

            AssertExtensions.assertFutureThrows(
                    " getInfo should throw exception.",
                    chunkStorage.getInfo(chunkName),
                    ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

            AssertExtensions.assertFutureThrows(
                    " openRead should throw exception.",
                    chunkStorage.openRead(chunkName),
                    ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

            AssertExtensions.assertFutureThrows(
                    " openWrite should throw exception.",
                    chunkStorage.openWrite(chunkName),
                    ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

            AssertExtensions.assertFutureThrows(
                    " getInfo should throw exception.",
                    chunkStorage.getInfo(chunkName),
                    ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

            if (chunkStorage.supportsAppend()) {
                AssertExtensions.assertFutureThrows(
                        " write should throw exception.",
                        chunkStorage.write(ChunkHandle.writeHandle(chunkName), 0, 1, new ByteArrayInputStream(new byte[1])),
                        ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));
            }
            AssertExtensions.assertFutureThrows(
                    " setReadOnly should throw exception.",
                    chunkStorage.setReadOnly(ChunkHandle.writeHandle(chunkName), false),
                    ex -> (ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName))
                            || ex.getCause() instanceof UnsupportedOperationException || ex instanceof UnsupportedOperationException);

            AssertExtensions.assertFutureThrows(
                    " read should throw exception.",
                    chunkStorage.read(ChunkHandle.writeHandle(chunkName), 0, 1, new byte[1], 0),
                    ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

            AssertExtensions.assertFutureThrows(
                    " delete should throw exception.",
                    chunkStorage.delete(ChunkHandle.writeHandle(chunkName)),
                    ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));
        }

        @Override
        protected void populate(byte[] data) {
            // Do nothing keep data uninitialized.
        }
    }
}
