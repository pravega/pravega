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

import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.metadata.BaseMetadataStore;
import io.pravega.segmentstore.storage.metadata.StorageMetadataException;
import io.pravega.segmentstore.storage.metadata.StorageMetadataVersionMismatchException;
import io.pravega.segmentstore.storage.metadata.StorageMetadataWritesFencedOutException;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.segmentstore.storage.noop.NoOpChunkStorage;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
public class ChunkedSegmentStorageMockTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 42;

    @Test
    public void testStorageMetadataWritesFencedOutExceptionDuringCommit() throws Exception {
        Exception exceptionToThrow = new StorageMetadataWritesFencedOutException("Test Exception");
        val clazz = StorageNotPrimaryException.class;
        testExceptionDuringCommit(exceptionToThrow, clazz);
    }

    @Test
    public void testRandomExceptionDuringCommit() throws Exception {
        Exception exceptionToThrow = new UnsupportedOperationException("Test Exception");
        val clazz = UnsupportedOperationException.class;
        testExceptionDuringCommit(exceptionToThrow, clazz);
    }

    @Test
    public void testStorageMetadataVersionMismatchExceptionDuringCommit() throws Exception {
        Exception exceptionToThrow = new StorageMetadataVersionMismatchException("Test Exception");
        val clazz = StorageMetadataVersionMismatchException.class;
        testExceptionDuringCommit(exceptionToThrow, clazz);
    }

    public void testExceptionDuringCommit(Exception exceptionToThrow, Class clazz) throws Exception {
        testExceptionDuringCommit(exceptionToThrow, clazz, false, false);
    }

    public void testExceptionDuringCommit(Exception exceptionToThrow, Class clazz, boolean skipCreate, boolean skipConcat) throws Exception {
        String testSegmentName = "test";
        String concatSegmentName = "concat";

        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        @Cleanup
        BaseMetadataStore spyMetadataStore = spy(new InMemoryMetadataStore(executorService()));
        @Cleanup
        BaseChunkStorage spyChunkStorage = spy(new NoOpChunkStorage(executorService()));
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID, spyChunkStorage, spyMetadataStore, executorService(), config);
        chunkedSegmentStorage.initialize(1);

        // Step 1: Create segment and write some data.
        val h1 = chunkedSegmentStorage.create(testSegmentName, policy, null).get();

        Assert.assertEquals(h1.getSegmentName(), testSegmentName);
        Assert.assertFalse(h1.isReadOnly());
        chunkedSegmentStorage.write(h1, 0, new ByteArrayInputStream(new byte[10]), 10, null).get();

        // Capture segment layout information, so that we can check that after aborted operation it is still unchanged.
        val expectedSegmentMetadata = TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName);
        val expectedChunkMetadataList = TestUtils.getChunkList(spyMetadataStore, testSegmentName);

        // Make sure mock is working
        Assert.assertEquals(10, expectedSegmentMetadata.getLength());
        Assert.assertEquals(5, expectedChunkMetadataList.size());
        Assert.assertEquals(expectedChunkMetadataList.get(0).getName(), expectedSegmentMetadata.getFirstChunk());
        Assert.assertEquals(expectedChunkMetadataList.get(4).getName(), expectedSegmentMetadata.getLastChunk());

        TestUtils.assertEquals(expectedSegmentMetadata,
                expectedChunkMetadataList,
                TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                TestUtils.getChunkList(spyMetadataStore, testSegmentName));
        TestUtils.checkChunksExistInStorage(spyChunkStorage, spyMetadataStore, testSegmentName);

        // Step 2: Increase epoch.
        chunkedSegmentStorage.initialize(2);

        val h2 = chunkedSegmentStorage.create(concatSegmentName, policy, null).get();
        chunkedSegmentStorage.write(h2, 0, new ByteArrayInputStream(new byte[10]), 10, null).get();
        chunkedSegmentStorage.seal(h2, null).get();

        // Step 3: Inject fault.
        CompletableFuture f = new CompletableFuture();
        f.completeExceptionally(exceptionToThrow);
        doReturn(f).when(spyMetadataStore).commit(any(), anyBoolean(), anyBoolean());

        AssertExtensions.assertFutureThrows(
                "write succeeded when exception was expected.",
                chunkedSegmentStorage.write(h1, 10, new ByteArrayInputStream(new byte[10]), 10, null),
                ex -> clazz.equals(ex.getClass()));

        TestUtils.assertEquals(expectedSegmentMetadata,
                expectedChunkMetadataList,
                TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                TestUtils.getChunkList(spyMetadataStore, testSegmentName));
        TestUtils.checkChunksExistInStorage(spyChunkStorage, spyMetadataStore, testSegmentName);

        // Make sure 15 chunks in total were created and then 5 of them garbage collected later.
        verify(spyChunkStorage, times(15)).doCreate(anyString());
        //verify(spyChunkStorage, times(5)).doDelete(any());

        // seal.
        doReturn(f).when(spyMetadataStore).commit(any(), anyBoolean(), anyBoolean());
        AssertExtensions.assertFutureThrows(
                "Seal succeeded when exception was expected.",
                chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> clazz.equals(ex.getClass()));

        TestUtils.assertEquals(expectedSegmentMetadata,
                expectedChunkMetadataList,
                TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                TestUtils.getChunkList(spyMetadataStore, testSegmentName));
        TestUtils.checkChunksExistInStorage(spyChunkStorage, spyMetadataStore, testSegmentName);

        // openWrite.
        doReturn(f).when(spyMetadataStore).commit(any(), anyBoolean(), anyBoolean());
        AssertExtensions.assertFutureThrows(
                "openWrite succeeded when exception was expected.",
                chunkedSegmentStorage.openWrite(testSegmentName),
                ex -> clazz.equals(ex.getClass()));

        TestUtils.assertEquals(expectedSegmentMetadata,
                expectedChunkMetadataList,
                TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                TestUtils.getChunkList(spyMetadataStore, testSegmentName));

        // delete.
        doReturn(f).when(spyMetadataStore).commit(any(), anyBoolean(), anyBoolean());
        AssertExtensions.assertFutureThrows(
                "delete succeeded when exception was expected.",
                chunkedSegmentStorage.delete(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> clazz.equals(ex.getClass()));
        TestUtils.assertEquals(expectedSegmentMetadata,
                expectedChunkMetadataList,
                TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                TestUtils.getChunkList(spyMetadataStore, testSegmentName));
        TestUtils.checkChunksExistInStorage(spyChunkStorage, spyMetadataStore, testSegmentName);

        // truncate.
        doReturn(f).when(spyMetadataStore).commit(any(), anyBoolean(), anyBoolean());
        AssertExtensions.assertFutureThrows(
                "truncate succeeded when exception was expected.",
                chunkedSegmentStorage.truncate(SegmentStorageHandle.writeHandle(testSegmentName),
                        2, null),
                ex -> clazz.equals(ex.getClass()));
        TestUtils.assertEquals(expectedSegmentMetadata,
                expectedChunkMetadataList,
                TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                TestUtils.getChunkList(spyMetadataStore, testSegmentName));
        TestUtils.checkChunksExistInStorage(spyChunkStorage, spyMetadataStore, testSegmentName);

        if (!skipCreate) {
            // create.
            doReturn(f).when(spyMetadataStore).commit(any(), anyBoolean(), anyBoolean());
            AssertExtensions.assertFutureThrows(
                    "create succeeded when exception was expected.",
                    chunkedSegmentStorage.create("foo", policy, null),
                    ex -> clazz.equals(ex.getClass()));
        }

        if (!skipConcat) {
            // concat.
            doReturn(f).when(spyMetadataStore).commit(any(), anyBoolean(), anyBoolean());
            AssertExtensions.assertFutureThrows(
                    "concat succeeded when exception was expected.",
                    chunkedSegmentStorage.concat(h1, 10, h2.getSegmentName(), null),
                    ex -> clazz.equals(ex.getClass()));
            TestUtils.assertEquals(expectedSegmentMetadata,
                    expectedChunkMetadataList,
                    TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                    TestUtils.getChunkList(spyMetadataStore, testSegmentName));
            TestUtils.checkChunksExistInStorage(spyChunkStorage, spyMetadataStore, testSegmentName);
        }
    }

    @Test
    public void testExceptionDuringMetadataRead() throws Exception {
        Exception exceptionToThrow = new CompletionException(new StorageMetadataException("Test Exception"));
        val clazz = StorageMetadataException.class;
        testExceptionDuringMetadataRead(exceptionToThrow, clazz);
    }

    public void testExceptionDuringMetadataRead(Exception exceptionToThrow, Class clazz) throws Exception {
        String testSegmentName = "test";
        String concatSegmentName = "concat";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();
        @Cleanup
        BaseMetadataStore spyMetadataStore = spy(new InMemoryMetadataStore(executorService()));
        spyMetadataStore.setMaxEntriesInTxnBuffer(0);
        @Cleanup
        BaseChunkStorage spyChunkStorage = spy(new NoOpChunkStorage(executorService()));

        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID, spyChunkStorage, spyMetadataStore, executorService(), config);
        chunkedSegmentStorage.initialize(1);

        // Step 1: Create segment and write some data.
        val h1 = chunkedSegmentStorage.create(testSegmentName, policy, null).get();

        Assert.assertEquals(h1.getSegmentName(), testSegmentName);
        Assert.assertFalse(h1.isReadOnly());
        chunkedSegmentStorage.write(h1, 0, new ByteArrayInputStream(new byte[10]), 10, null).get();

        // Step 2: Increase epoch.
        chunkedSegmentStorage.initialize(2);
        val h2 = chunkedSegmentStorage.create(concatSegmentName, policy, null).get();
        chunkedSegmentStorage.write(h2, 0, new ByteArrayInputStream(new byte[10]), 10, null).get();
        chunkedSegmentStorage.seal(h2, null).get();

        // Step 3: Inject fault.
        CompletableFuture f = new CompletableFuture();
        f.completeExceptionally(exceptionToThrow);
        doReturn(f).when(spyMetadataStore).get(any(), anyString());

        // These calls are all read calls they can potentially run in parallel,
        // therefore we must force them to be synchronous to avoid org.mockito.exceptions.misusing.UnfinishedStubbingException
        AssertExtensions.assertThrows(
                "write succeeded when exception was expected.",
                () -> chunkedSegmentStorage.write(h2, 10, new ByteArrayInputStream(new byte[10]), 10, null).get(),
                ex -> clazz.equals(ex.getClass()));
        AssertExtensions.assertThrows(
                "Seal succeeded when exception was expected.",
                () -> chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle(testSegmentName), null).get(),
                ex -> clazz.equals(ex.getClass()));
        AssertExtensions.assertThrows(
                "openWrite succeeded when exception was expected.",
                () -> chunkedSegmentStorage.openWrite(testSegmentName).get(),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertThrows(
                "delete succeeded when exception was expected.",
                () -> chunkedSegmentStorage.delete(SegmentStorageHandle.writeHandle(testSegmentName), null).get(),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertThrows(
                "truncate succeeded when exception was expected.",
                () -> chunkedSegmentStorage.truncate(SegmentStorageHandle.writeHandle(testSegmentName),
                        2, null).get(),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertThrows(
                "create succeeded when exception was expected.",
                () -> chunkedSegmentStorage.create("foo", policy, null).get(),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertThrows(
                "concat succeeded when exception was expected.",
                () -> chunkedSegmentStorage.concat(h1, 10, h2.getSegmentName(), null).get(),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertThrows(
                "getStreamSegmentInfo succeeded  when exception was expected.",
                () -> chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get(),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertThrows(
                "read  succeeded when exception was expected.",
                () -> chunkedSegmentStorage.read(SegmentStorageHandle.readHandle(testSegmentName), 0, new byte[1], 0, 1, null).get(),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertThrows(
                "openRead  succeeded when exception was expected.",
                () -> chunkedSegmentStorage.openRead(testSegmentName).get(),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertThrows(
                "exists  succeeded when exception was expected.",
                () -> chunkedSegmentStorage.exists(testSegmentName, null).get(),
                ex -> clazz.equals(ex.getClass()));
    }

    @Test
    public void testIOExceptionDuringWrite() throws Exception {
        String testSegmentName = "test";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        @Cleanup
        BaseMetadataStore spyMetadataStore = spy(new InMemoryMetadataStore(executorService()));
        @Cleanup
        BaseChunkStorage spyChunkStorage = spy(new NoOpChunkStorage(executorService()));
        ((NoOpChunkStorage) spyChunkStorage).setShouldSupportConcat(false);
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID, spyChunkStorage, spyMetadataStore, executorService(), config);
        chunkedSegmentStorage.initialize(1);

        // Step 1: Create segment and write some data.
        val h1 = chunkedSegmentStorage.create(testSegmentName, policy, null).get();

        Assert.assertEquals(h1.getSegmentName(), testSegmentName);
        Assert.assertFalse(h1.isReadOnly());
        chunkedSegmentStorage.write(h1, 0, new ByteArrayInputStream(new byte[10]), 10, null).get();

        // Step 2: Inject fault.
        Exception exceptionToThrow = new ChunkStorageException("test", "Test Exception", new IOException("Test Exception"));
        val clazz = ChunkStorageException.class;
        doThrow(exceptionToThrow).when(spyChunkStorage).doWrite(any(), anyLong(), anyInt(), any());

        AssertExtensions.assertFutureThrows(
                "write succeeded when exception was expected.",
                chunkedSegmentStorage.write(h1, 10, new ByteArrayInputStream(new byte[10]), 10, null),
                ex -> clazz.equals(ex.getClass()));

        //verify(spyChunkStorage, times(1)).doDelete(any());
    }

    @Test
    public void testFileNotFoundExceptionDuringGarbageCollection() throws Exception {
        String testSegmentName = "test";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .garbageCollectionDelay(Duration.ZERO)
                .build();
        @Cleanup
        BaseMetadataStore spyMetadataStore = spy(new InMemoryMetadataStore(executorService()));
        @Cleanup
        BaseChunkStorage spyChunkStorage = spy(new NoOpChunkStorage(executorService()));
        ((NoOpChunkStorage) spyChunkStorage).setShouldSupportConcat(false);
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID, spyChunkStorage, spyMetadataStore, executorService(), config);
        chunkedSegmentStorage.initialize(1);
        chunkedSegmentStorage.getGarbageCollector().setSuspended(true);

        // Step 1: Create segment and write some data.
        val h1 = chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        chunkedSegmentStorage.write(h1, 0, new ByteArrayInputStream(new byte[10]), 10, null).get();

        Assert.assertEquals(h1.getSegmentName(), testSegmentName);
        Assert.assertFalse(h1.isReadOnly());
        // Step 2: Inject fault.
        Exception exceptionToThrow = new ChunkNotFoundException("Test Exception", "Mock Exception", new Exception("Mock Exception"));
        doThrow(exceptionToThrow).when(spyChunkStorage).doDelete(any());

        chunkedSegmentStorage.delete(h1, null).get();
        Assert.assertEquals(5, chunkedSegmentStorage.getGarbageCollector().getGarbageChunks().size());
        chunkedSegmentStorage.getGarbageCollector().deleteGarbage(false, 100).get();
        verify(spyChunkStorage, times(5)).doDelete(any());
    }

    @Test
    public void testExceptionDuringGarbageCollection() throws Exception {
        String testSegmentName = "test";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .defaultRollingPolicy(policy)
                .garbageCollectionDelay(Duration.ZERO)
                .build();
        @Cleanup
        BaseMetadataStore spyMetadataStore = spy(new InMemoryMetadataStore(executorService()));
        @Cleanup
        BaseChunkStorage spyChunkStorage = spy(new NoOpChunkStorage(executorService()));
        ((NoOpChunkStorage) spyChunkStorage).setShouldSupportConcat(false);
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID, spyChunkStorage, spyMetadataStore, executorService(), config);
        chunkedSegmentStorage.initialize(1);
        chunkedSegmentStorage.getGarbageCollector().setSuspended(true);

        // Step 1: Create segment and write some data.
        val h1 = chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        chunkedSegmentStorage.write(h1, 0, new ByteArrayInputStream(new byte[10]), 10, null).get();

        Assert.assertEquals(h1.getSegmentName(), testSegmentName);
        Assert.assertFalse(h1.isReadOnly());
        // Step 2: Inject fault.
        Exception exceptionToThrow = new IllegalStateException("Test Exception");
        doThrow(exceptionToThrow).when(spyChunkStorage).doDelete(any());

        chunkedSegmentStorage.delete(h1, null).get();
        Assert.assertEquals(5, chunkedSegmentStorage.getGarbageCollector().getGarbageChunks().size());
        chunkedSegmentStorage.getGarbageCollector().deleteGarbage(false, 100).get();
        verify(spyChunkStorage, times(5)).doDelete(any());
    }

    @Test
    public void testReport() {
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        @Cleanup
        BaseMetadataStore spyMetadataStore = spy(new InMemoryMetadataStore(executorService()));
        @Cleanup
        BaseChunkStorage spyChunkStorage = spy(new NoOpChunkStorage(executorService()));
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID, spyChunkStorage, spyMetadataStore, executorService(), config);
        chunkedSegmentStorage.initialize(1);

        chunkedSegmentStorage.report();

        // Not possible to mock any other reporter except metadata store.
        verify(spyMetadataStore).report();
    }
}
