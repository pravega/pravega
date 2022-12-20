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

package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.common.Exceptions;
import io.pravega.common.io.BoundedInputStream;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Getter;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests specifically targeted at test {@link ChunkStorage} implementation.
 */
public class ChunkStorageTests extends ThreadPooledTestSuite {
    private static final int THREAD_POOL_SIZE = 3;
    Random rnd = new Random(0);

    @Getter
    ChunkStorage chunkStorage;

    /**
     * Derived classes should return appropriate {@link ChunkStorage}.
     */
    protected ChunkStorage createChunkStorage() throws Exception {
        return new InMemoryChunkStorage(executorService());
    }

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        chunkStorage = createChunkStorage();
    }

    @Override
    @After
    public void after() throws Exception {
        if (null != chunkStorage) {
            chunkStorage.close();
        }
        super.before();
    }

    @Override
    protected int getThreadPoolSize() {
        return THREAD_POOL_SIZE;
    }

    /**
     * Populate the data.
     *
     * @param data
     */
    protected void populate(byte[] data) {
        rnd.nextBytes(data);
    }

    /**
     * Test basic chunk lifecycle.
     */
    @Test
    public void testChunkLifeCycle() throws Exception {
        testChunkLifeCycle("testchunk");
    }

    protected void testChunkLifeCycle(String chunkName) throws Exception {
        // try reading non existent chunk
        testNotExists(chunkName);

        // Perform basic operations.
        ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, 1, new ByteArrayInputStream(new byte[1])).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        chunkHandle = chunkStorage.openRead(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(true, chunkHandle.isReadOnly());

        chunkHandle = chunkStorage.openWrite(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());
        testAlreadyExists(chunkName);

        chunkStorage.delete(chunkHandle).join();
        testNotExists(chunkName);
    }

    @Test
    public void testChunkLifeCycleCreateWithContent() throws Exception {
        this.testChunkLifeCycleCreateWithContent("testchunk");
    }

    protected void testChunkLifeCycleCreateWithContent(String chunkName) throws Exception {
        // try reading non existent chunk
        testNotExists(chunkName);

        // Perform basic operations.
        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        chunkHandle = chunkStorage.openRead(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(true, chunkHandle.isReadOnly());

        chunkHandle = chunkStorage.openWrite(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());
        testAlreadyExists(chunkName);

        chunkStorage.delete(chunkHandle).join();
        testNotExists(chunkName);
    }

    /**
     * Test basic read and write.
     */
    @Test
    public void testSimpleReadWrite() throws Exception {
        this.testSimpleReadWrite("testchunk");
    }

    protected void testSimpleReadWrite(String chunkName) throws Exception {
        if (!chunkStorage.supportsAppend()) {
            return;
        }

        // Create.
        ChunkHandle chunkHandle = chunkStorage.create(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

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
        this.testSimpleReadWriteCreateWithContent("testchunk");
    }

    protected void testSimpleReadWriteCreateWithContent(String chunkName) throws Exception {
        // Create.
        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Read back.
        byte[] readBuffer = new byte[writeBuffer.length];
        int bytesRead = chunkStorage.read(chunkHandle, 0, writeBuffer.length, readBuffer, 0).get();
        assertEquals(writeBuffer.length, bytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        chunkStorage.delete(chunkHandle).join();
    }

    /**
     * Test consecutive reads.
     */
    @Test
    public void testConsecutiveReads() throws Exception {
        String chunkName = "testchunk";

        // Create. Write
        byte[] writeBuffer = new byte[15];
        populate(writeBuffer);
        ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());
        val chunkInfo = chunkStorage.getInfo(chunkName).get();
        assertEquals(writeBuffer.length, chunkInfo.getLength());

        // Read back in multiple reads.
        byte[] readBuffer = new byte[writeBuffer.length];

        int totalBytesRead = 0;
        for (int i = 1; i <= 5; i++) {
            int remaining = i;
            while (remaining > 0) {
                int bytesRead = chunkStorage.read(chunkHandle, totalBytesRead, remaining, readBuffer, totalBytesRead).get();
                remaining -= bytesRead;
                totalBytesRead += bytesRead;
            }
        }
        assertEquals(writeBuffer.length, totalBytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        chunkStorage.delete(chunkHandle).join();
    }

    /**
     * Test consecutive reads.
     */
    @Test
    public void testConsecutiveReadsCreateWithContent() throws Exception {
        String chunkName = "testchunk";

        // Create.
        byte[] writeBuffer = new byte[15];
        populate(writeBuffer);
        // Create.
        ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Read back in multiple reads.
        byte[] readBuffer = new byte[writeBuffer.length];

        int totalBytesRead = 0;
        for (int i = 1; i <= 5; i++) {
            int remaining = i;
            while (remaining > 0) {
                int bytesRead = chunkStorage.read(chunkHandle, totalBytesRead, remaining, readBuffer, totalBytesRead).get();
                remaining -= bytesRead;
                totalBytesRead += bytesRead;
            }
        }
        assertEquals(writeBuffer.length, totalBytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        chunkStorage.delete(chunkHandle).join();
    }


    /**
     * Test consecutive writes.
     */
    @Test
    public void testConsecutiveWrites() throws Exception {
        if (!chunkStorage.supportsAppend()) {
            return;
        }
        String chunkName = "testchunk";

        // Create.
        ChunkHandle chunkHandle = chunkStorage.create(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Write multiple times.
        byte[] writeBuffer = new byte[15];
        populate(writeBuffer);
        int totalBytesWritten = 0;
        for (int i = 1; i <= 5; i++) {
            int bytesWritten = chunkStorage.write(chunkHandle, totalBytesWritten, i, new ByteArrayInputStream(writeBuffer, totalBytesWritten, i)).get();
            assertEquals(i, bytesWritten);
            totalBytesWritten += i;
        }

        // Read back.
        byte[] readBuffer = new byte[writeBuffer.length];
        int totalBytesRead = 0;
        int remaining = writeBuffer.length;
        while (remaining > 0) {
            int bytesRead = chunkStorage.read(chunkHandle, totalBytesRead, remaining, readBuffer, totalBytesRead).get();
            remaining -= bytesRead;
            totalBytesRead += bytesRead;
        }
        assertEquals(writeBuffer.length, totalBytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        chunkStorage.delete(chunkHandle).join();
    }

    /**
     * Test simple reads and writes for exceptions.
     */
    @Test
    public void testSimpleReadExceptions() throws Exception {
        String chunkName = "testchunk";

        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        int length = writeBuffer.length;
        val chunkHandle = chunkStorage.createWithContent(chunkName, 10, new ByteArrayInputStream(writeBuffer)).get();
        int bytesWritten = Math.toIntExact(chunkStorage.getInfo(chunkName).get().getLength());
        assertEquals(length, bytesWritten);
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        byte[] readBuffer = new byte[writeBuffer.length];

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, -1, readBuffer, 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, -1, 0, readBuffer, 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, 0, readBuffer, -1).get(),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, 0, null, 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, length + 1, readBuffer, 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, length, new byte[length - 1], 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, length, readBuffer, length).get(),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 11, 1, readBuffer, 0).get(),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Test simple reads and writes for exceptions.
     */
    @Test
    public void testSimpleWriteExceptions() throws Exception {
        if (!chunkStorage.supportsAppend()) {
            return;
        }
        String chunkName = "testchunk";

        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        int length = writeBuffer.length;
        val chunkHandle = chunkStorage.createWithContent(chunkName, 10, new ByteArrayInputStream(writeBuffer)).get();
        int bytesWritten = Math.toIntExact(chunkStorage.getInfo(chunkName).get().getLength());
        assertEquals(length, bytesWritten);
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Write exceptions.
        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, -1, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, -1, 0, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, 0, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof InvalidOffsetException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, 0, null).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, length + 1, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof InvalidOffsetException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 11, length, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof InvalidOffsetException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(null, 0, 1, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(ChunkHandle.readHandle("test"), 0, 1, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);

        chunkStorage.delete(chunkHandle).join();
    }

    /**
     * Test simple reads and writes for exceptions.
     */
    @Test
    public void testCreateWithContentExceptions() throws Exception {
        String chunkName = "testchunk";

        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        // Create exceptions.
        AssertExtensions.assertThrows(
                " createWithContent should throw exception.",
                () -> chunkStorage.createWithContent(null, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                " createWithContent should throw exception.",
                () -> chunkStorage.createWithContent(chunkName, -1, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                " createWithContent should throw exception.",
                () -> chunkStorage.createWithContent(chunkName, 0, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                " createWithContent should throw exception.",
                () -> chunkStorage.createWithContent(chunkName, 1, null).get(),
                ex -> ex instanceof IllegalArgumentException);

    }

    /**
     * Test one simple scenario involving all operations.
     */
    @Test
    public void testSimpleScenario() throws Exception {
        String chunknameA = "A";
        String chunknameB = "B";
        assertFalse(chunkStorage.exists(chunknameA).get());
        assertFalse(chunkStorage.exists(chunknameB).get());

        // Open writable handles
        ChunkHandle handleA;
        ChunkInfo chunkInfoA;
        long multiple = getMinimumConcatSize();

        byte[] bytes = new byte[Math.toIntExact(10 * multiple)];
        populate(bytes);
        int totalBytesWritten = 0;

        // Write some data to A
        if (chunkStorage.supportsAppend()) {
            // Create chunks
            chunkStorage.create(chunknameA).get();
            assertTrue(chunkStorage.exists(chunknameA).get());

            chunkInfoA = chunkStorage.getInfo(chunknameA).get();
            assertEquals(chunknameA, chunkInfoA.getName());
            assertEquals(0, chunkInfoA.getLength());

            handleA = chunkStorage.openWrite(chunknameA).get();
            assertFalse(handleA.isReadOnly());
            for (int i = 1; i < 5; i++) {
                val size = i * multiple;
                try (BoundedInputStream bis = new BoundedInputStream(new ByteArrayInputStream(bytes), Math.toIntExact(size))) {
                    int bytesWritten = chunkStorage.write(handleA, totalBytesWritten, Math.toIntExact(size), bis).get();
                    assertEquals(size, bytesWritten);
                }
                totalBytesWritten += size;
            }
        } else {
            handleA = chunkStorage.createWithContent(chunknameA, bytes.length, new ByteArrayInputStream(bytes)).get();
            totalBytesWritten = Math.toIntExact(10 * multiple);
        }

        chunkInfoA = chunkStorage.getInfo(chunknameA).get();
        assertEquals(totalBytesWritten, chunkInfoA.getLength());

        // Write some data to segment B
        byte[] bytes2 = new byte[Math.toIntExact(5 * multiple)];
        populate(bytes2);
        ChunkHandle handleB = chunkStorage.createWithContent(chunknameB, bytes2.length, new ByteArrayInputStream(bytes2)).get();
        totalBytesWritten += bytes.length;
        ChunkInfo chunkInfoB = chunkStorage.getInfo(chunknameB).get();
        assertEquals(chunknameB, chunkInfoB.getName());
        assertEquals(bytes2.length, chunkInfoB.getLength());
        assertFalse(handleA.isReadOnly());
        assertFalse(handleB.isReadOnly());
        // Read some data
        int totalBytesRead = 0;
        byte[] buffer = new byte[Math.toIntExact(10 * multiple)];
        for (int i = 1; i < 5; i++) {
            totalBytesRead += chunkStorage.read(handleA, totalBytesRead, i, buffer, totalBytesRead).get();
        }

        // Concat
        if (chunkStorage.supportsConcat() || chunkStorage.supportsAppend()) {
            chunkInfoA = chunkStorage.getInfo(chunknameA).get();
            chunkInfoB = chunkStorage.getInfo(chunknameB).get();
            if (chunkStorage.supportsConcat()) {
                chunkStorage.concat(new ConcatArgument[]{ConcatArgument.fromChunkInfo(chunkInfoA), ConcatArgument.fromChunkInfo(chunkInfoB)}).join();
                ChunkInfo chunkInfoAAfterConcat = chunkStorage.getInfo(chunknameA).get();
                assertEquals(chunkInfoA.getLength() + chunkInfoB.getLength(), chunkInfoAAfterConcat.getLength());
            }
        }
        // delete
        chunkStorage.delete(handleA).join();
        try {
            chunkStorage.delete(handleB).join();
        } catch (CompletionException e) {
            // chunkStorage may have natively deleted it as part of concat Eg. HDFS.
        }
    }

    protected int getMinimumConcatSize() {
        return 1;
    }

    /**
     * Test concat operation for non-existent chunks.
     */
    @Test
    public void testConcatNotExists() throws Exception {
        String existingChunkName = "test";
        val size = getMinimumConcatSize() + 1;
        ChunkHandle existingChunkHandle = chunkStorage.createWithContent(existingChunkName, size, new ByteArrayInputStream(new byte[size])).get();
        try {
            AssertExtensions.assertFutureThrows(
                    " concat should throw ChunkNotFoundException.",
                    chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName).length(size).build(),
                                    ConcatArgument.builder().name("NonExistent").length(1).build()
                            }
                    ),
                    ex -> ex instanceof ChunkNotFoundException || ex instanceof UnsupportedOperationException);
            AssertExtensions.assertFutureThrows(
                    " concat should throw ChunkNotFoundException.",
                    chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name("NonExistent").length(1).build(),
                                    ConcatArgument.builder().name(existingChunkName).length(size).build(),
                            }
                    ),
                    ex -> ex instanceof ChunkNotFoundException || ex instanceof UnsupportedOperationException);
        } catch (Exception e) {
            val ex = Exceptions.unwrap(e);
            // The storage provider may not have native concat.
            Assert.assertTrue(ex instanceof UnsupportedOperationException);
        } finally {
            chunkStorage.delete(existingChunkHandle).join();
        }
    }

    /**
     * Test concat operation for non-existent chunks.
     */
    @Test
    public void testConcatException() throws Exception {
        String existingChunkName1 = "test1";
        String existingChunkName2 = "test2";
        ChunkHandle existingChunkHandle1 = chunkStorage.createWithContent(existingChunkName1, 1, new ByteArrayInputStream(new byte[1])).get();
        ChunkHandle existingChunkHandle2 = chunkStorage.createWithContent(existingChunkName2, 1, new ByteArrayInputStream(new byte[1])).get();
        try {
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(0).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof UnsupportedOperationException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(-1).build(),
                                    ConcatArgument.builder().name(existingChunkName2).length(0).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof UnsupportedOperationException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                                    ConcatArgument.builder().name(existingChunkName2).length(-1).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof UnsupportedOperationException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof UnsupportedOperationException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                                    null,
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof UnsupportedOperationException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    null,
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof UnsupportedOperationException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                                    ConcatArgument.builder().name(existingChunkName2).length(1).build(),
                                    ConcatArgument.builder().name(existingChunkName2).length(1).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof UnsupportedOperationException);
        } catch (UnsupportedOperationException e) {
            // The storage provider may not have native concat.
        } finally {
            chunkStorage.delete(existingChunkHandle1).join();
            chunkStorage.delete(existingChunkHandle2).join();
        }
    }


    /**
     * Test operations on open handles when underlying chunk is deleted.
     */
    @Test
    public void testDeleteAfterOpen() throws Exception {
        String testChunkName = "test";
        ChunkHandle writeHandle = chunkStorage.createWithContent(testChunkName, 1, new ByteArrayInputStream(new byte[1])).get();
        ChunkHandle readHandle = chunkStorage.openRead(testChunkName).get();
        chunkStorage.delete(writeHandle).join();
        byte[] bufferRead = new byte[10];
        AssertExtensions.assertFutureThrows(
                " read should throw ChunkNotFoundException.",
                chunkStorage.read(readHandle, 0, 10, bufferRead, 0),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName));
        if (chunkStorage.supportsAppend()) {
            AssertExtensions.assertFutureThrows(
                    " write should throw ChunkNotFoundException.",
                    chunkStorage.write(writeHandle, 0, 1, new ByteArrayInputStream(new byte[1])),
                    ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName));
        }
        AssertExtensions.assertFutureThrows(
                " truncate should throw ChunkNotFoundException.",
                chunkStorage.truncate(writeHandle, 0),
                ex -> (ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName))
                        || ex instanceof UnsupportedOperationException);
        AssertExtensions.assertFutureThrows(
                " setReadOnly should throw ChunkNotFoundException.",
                chunkStorage.setReadOnly(writeHandle, false),
                ex -> (ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName))
                        || ex.getCause() instanceof UnsupportedOperationException || ex instanceof UnsupportedOperationException);
        AssertExtensions.assertFutureThrows(
                " delete should throw ChunkNotFoundException.",
                chunkStorage.delete(writeHandle),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName));

    }

    /**
     * Test one simple concat operation.
     */
    @Test
    public void testConcat() throws Exception {
        String tragetChunkName = "Concat_0";
        try {

            ChunkHandle[] chunksToConcat = new ChunkHandle[5];
            ConcatArgument[] chunkInfos = new ConcatArgument[5];
            byte[] bufferWritten = new byte[10];
            populate(bufferWritten);
            int offset = 0;
            for (int i = 0; i < 5; i++) {
                String chunkname = "Concat_" + i;
                chunkInfos[i] = new ConcatArgument(i, chunkname);
                // Write some data to chunk
                if (i > 0) {
                    try (BoundedInputStream bis = new BoundedInputStream(new ByteArrayInputStream(bufferWritten, offset, i), i)) {
                        chunksToConcat[i] = chunkStorage.createWithContent(chunkname, i, bis).get();
                    }
                    ChunkInfo chunkInfo = chunkStorage.getInfo(chunkname).get();
                    assertEquals(i, chunkInfo.getLength());
                    offset += i;
                } else {
                    chunksToConcat[i] = chunkStorage.create(chunkname).get();
                }
            }

            chunkStorage.concat(chunkInfos).get();

            ChunkInfo chunkInfoAfterConcat = chunkStorage.getInfo(tragetChunkName).get();
            assertEquals(10, chunkInfoAfterConcat.getLength());
            // Read some data
            byte[] bufferRead = new byte[10];

            int bytesRead = 0;
            while (bytesRead < 10) {
                bytesRead += chunkStorage.read(ChunkHandle.readHandle(tragetChunkName), bytesRead, bufferRead.length, bufferRead, bytesRead).get();
            }
            assertArrayEquals(bufferWritten, bufferRead);
        } catch (ExecutionException | UnsupportedOperationException e) {
            val ex = Exceptions.unwrap(e);
            // The storage provider may not have native concat.
            Assert.assertTrue(ex instanceof UnsupportedOperationException);
        } finally {
            for (int i = 0; i < 5; i++) {
                String chunkname = "Concat_" + i;
                if (chunkStorage.exists(chunkname).get()) {
                    chunkStorage.delete(ChunkHandle.writeHandle(chunkname)).join();
                }
            }
        }
    }

    /**
     * Test readonly.
     */
    @Test
    public void testReadonly() throws Exception {
        String chunkName = "chunk";
        // Create chunks
        chunkStorage.createWithContent(chunkName, 1, new ByteArrayInputStream(new byte[1])).get();
        assertTrue(chunkStorage.exists(chunkName).get());

        AssertExtensions.assertThrows(
                " delete should throw IllegalArgumentException.",
                () -> chunkStorage.delete(ChunkHandle.readHandle(chunkName)).get(),
                ex -> ex instanceof IllegalArgumentException);
        try {
            // Open writable handle
            ChunkHandle hWrite = chunkStorage.openWrite(chunkName).get();
            assertFalse(hWrite.isReadOnly());
            int bytesWritten = 0;

            // Write some data
            AssertExtensions.assertThrows(
                    " write should throw IllegalArgumentException.",
                    () -> chunkStorage.write(ChunkHandle.readHandle(chunkName), 1, 1, new ByteArrayInputStream(new byte[1])).get(),
                    ex -> ex instanceof IllegalArgumentException);

            // Make readonly and open.
            chunkStorage.setReadOnly(hWrite, true).join();

            // Make writable and open again.
            chunkStorage.setReadOnly(hWrite, false).join();

            ChunkHandle hWrite2 = chunkStorage.openWrite(chunkName).get();
            assertFalse(hWrite2.isReadOnly());
            if (chunkStorage.supportsAppend()) {
                bytesWritten = chunkStorage.write(hWrite2, 1, 1, new ByteArrayInputStream(new byte[1])).get();
                assertEquals(1, bytesWritten);
            }
            chunkStorage.delete(hWrite).join();
        } catch (Exception e) {
            val ex = Exceptions.unwrap(e);
            Assert.assertTrue(ex instanceof UnsupportedOperationException || ex.getCause() instanceof UnsupportedOperationException);
        }
    }

    /**
     * Test truncate.
     */
    @Test
    public void testTruncate() throws Exception {
        try {
            String chunkName = "chunk";
            // Create chunks
            val h = chunkStorage.createWithContent(chunkName, 9, new ByteArrayInputStream(new byte[9])).get();
            int bytesWritten = Math.toIntExact(chunkStorage.getInfo(chunkName).get().getLength());
            assertEquals(9, bytesWritten);
            assertTrue(chunkStorage.exists(chunkName).get());

            // Open writable handle
            ChunkHandle hWrite = chunkStorage.openWrite(chunkName).get();
            assertFalse(hWrite.isReadOnly());

            // Write some data
            chunkStorage.truncate(hWrite, 4).get();
            val info = chunkStorage.getInfo(chunkName).get();
            Assert.assertEquals(4, info.getLength());
            // truncate at end.
            chunkStorage.truncate(hWrite, 1).get();
            val info2 = chunkStorage.getInfo(chunkName).get();
            Assert.assertEquals(1, info2.getLength());
        } catch (ExecutionException e) {
            assertTrue( e.getCause() instanceof UnsupportedOperationException);
            assertFalse(chunkStorage.supportsTruncation());
        }
    }

    /**
     * Test truncate.
     */
    @Test
    public void testTruncateExceptions() throws Exception {
        try {
            String chunkName = "chunk";
            // Create chunks
            chunkStorage.createWithContent(chunkName, 1, new ByteArrayInputStream(new byte[1])).get();
            assertTrue(chunkStorage.exists(chunkName).get());

            // Open writable handle
            ChunkHandle hWrite = chunkStorage.openWrite(chunkName).get();
            assertFalse(hWrite.isReadOnly());

            AssertExtensions.assertThrows(
                    " truncate should throw IllegalArgumentException.",
                    () -> chunkStorage.truncate(ChunkHandle.readHandle(chunkName), 0).get(),
                    ex -> ex instanceof IllegalArgumentException);

            AssertExtensions.assertThrows(
                    " truncate should throw IllegalArgumentException.",
                    () -> chunkStorage.truncate(ChunkHandle.writeHandle(chunkName), -1).get(),
                    ex -> ex instanceof IllegalArgumentException);
        } catch (ExecutionException e) {
            assertTrue( e.getCause() instanceof UnsupportedOperationException);
            assertFalse(chunkStorage.supportsTruncation());
        }
    }

    @Test
    public void testGetUsedSpace() throws Exception {
        val used = chunkStorage.getUsedSpace().get();
        Assert.assertTrue(used >= 0);
    }

    /**
     * Test default capabilities.
     */
    @Test
    public void testCapabilities() {
        assertEquals(true, chunkStorage.supportsAppend());
        assertEquals(true, chunkStorage.supportsTruncation());
        assertEquals(false, chunkStorage.supportsConcat());
    }

    private void testAlreadyExists(String chunkName) throws Exception {
        try {
            ChunkHandle chunkHandle = chunkStorage.create(chunkName).get();
            Assert.fail("ChunkAlreadyExistsException was expected");
        } catch (ExecutionException e) {
            //Assert.assertTrue(e.getCause().getClass().getName().endsWith("FileAlreadyExistsException"));
            //Assert.assertTrue(e.getCause().getMessage().endsWith(chunkName));
        }
    }

    private void testNotExists(String chunkName) throws Exception {
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

    @Test
    public void testInvalidName() {
        String emptyChunkName = "";

        AssertExtensions.assertThrows(
                " getInfo should throw exception.",
                () -> chunkStorage.getInfo(emptyChunkName).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " openRead should throw exception.",
                () -> chunkStorage.openRead(emptyChunkName).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " openWrite should throw exception.",
                () -> chunkStorage.openWrite(emptyChunkName).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " getInfo should throw exception.",
                () -> chunkStorage.getInfo(emptyChunkName).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(ChunkHandle.writeHandle(emptyChunkName), 0, 1, new ByteArrayInputStream(new byte[1])).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " setReadOnly should throw exception.",
                () -> chunkStorage.setReadOnly(ChunkHandle.writeHandle(emptyChunkName), false).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(ChunkHandle.writeHandle(emptyChunkName), 0, 1, new byte[1], 0).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " delete should throw exception.",
                () -> chunkStorage.delete(ChunkHandle.writeHandle(emptyChunkName)).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " concat should throw ChunkNotFoundException or UnSupportedOperationException",
                () -> chunkStorage.concat(
                        new ConcatArgument[]{
                                ConcatArgument.builder().name(emptyChunkName).length(0).build(),
                                ConcatArgument.builder().name("NonExistent").length(1).build()
                        }
                ).get(),
                ex -> (ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName)) || ex instanceof UnsupportedOperationException);

        AssertExtensions.assertThrows(
                " concat should throw ChunkNotFoundException.",
                () -> chunkStorage.concat(
                        new ConcatArgument[]{
                                ConcatArgument.builder().name("test").length(0).build(),
                                ConcatArgument.builder().name(emptyChunkName).length(0).build(),
                        }
                ).get(),
                ex -> (ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName)) || ex instanceof UnsupportedOperationException);
    }

    @Test
    public void testNullHandle() {
        AssertExtensions.assertThrows(
                " getInfo should throw IllegalArgumentException.",
                () -> chunkStorage.getInfo(null).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " openRead should throw IllegalArgumentException.",
                () -> chunkStorage.openRead(null).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " openWrite should throw IllegalArgumentException.",
                () -> chunkStorage.openWrite(null).get(),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                " write should throw IllegalArgumentException.",
                () -> chunkStorage.write(null, 0, 1, new ByteArrayInputStream(new byte[1])).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " setReadOnly should throw IllegalArgumentException.",
                () -> chunkStorage.setReadOnly(null, false).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw IllegalArgumentException.",
                () -> chunkStorage.read(null, 0, 1, new byte[1], 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " delete should throw IllegalArgumentException.",
                () -> chunkStorage.delete(null).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " truncate should throw IllegalArgumentException.",
                () -> chunkStorage.truncate(null, 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " concat should throw IllegalArgumentException.",
                () -> chunkStorage.concat(null).get(),
                ex -> ex instanceof IllegalArgumentException || ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                " concat should throw IllegalArgumentException.",
                () -> chunkStorage.concat(new ConcatArgument[]{null}).get(),
                ex -> ex instanceof IllegalArgumentException || ex instanceof UnsupportedOperationException);

    }

    @Test
    public void testUnsupported() throws ExecutionException, InterruptedException {
        String chunkName = "testchunk1";
        byte[] writeBuffer = new byte[10];
        ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());
        if (!chunkStorage.supportsAppend()) {
            AssertExtensions.assertFutureThrows(
                    " write should throw UnsupportedOperationException.",
                    chunkStorage.write(chunkHandle, 0, writeBuffer.length, new ByteArrayInputStream(writeBuffer)),
                    ex -> ex.getCause() instanceof UnsupportedOperationException || ex instanceof UnsupportedOperationException);
        }

        String newChunkName = "testchunk2";
        ChunkHandle newChunkHandle = chunkStorage.createWithContent(newChunkName, 1, new ByteArrayInputStream(new byte[1])).get();
        assertEquals(newChunkName, newChunkHandle.getChunkName());
        assertEquals(false, newChunkHandle.isReadOnly());

        if (!chunkStorage.supportsConcat()) {
            ConcatArgument[] concatArguments = new ConcatArgument[]{
                    new ConcatArgument(writeBuffer.length, chunkName),
                    new ConcatArgument(1, newChunkName)
            };
            AssertExtensions.assertThrows(
                    " concat should throw UnsupportedOperationException.",
                    () -> chunkStorage.concat(concatArguments).get(),
                    ex -> ex instanceof UnsupportedOperationException || ex.getCause() instanceof UnsupportedOperationException);
        }
    }
}
