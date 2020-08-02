/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class ChunkedSegmentStorageMockTests extends ThreadPooledTestSuite {

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

        BaseMetadataStore spyMetadataStore = spy(InMemoryMetadataStore.class);
        BaseChunkStorage spyChunkStorageProvider = spy(NoOpChunkStorage.class);
        ChunkedSegmentStorage storageManager = new ChunkedSegmentStorage(spyChunkStorageProvider, spyMetadataStore, executorService(), config);
        storageManager.initialize(1);

        // Step 1: Create segment and write some data.
        val h1 = storageManager.create(testSegmentName, policy, null).get();

        Assert.assertEquals(h1.getSegmentName(), testSegmentName);
        Assert.assertFalse(h1.isReadOnly());
        storageManager.write(h1, 0, new ByteArrayInputStream(new byte[10]), 10, null);

        // Step 2: Increase epoch.
        storageManager.initialize(2);
        val h2 = storageManager.create(concatSegmentName, policy, null).get();
        storageManager.write(h2, 0, new ByteArrayInputStream(new byte[10]), 10, null);
        storageManager.seal(h2, null);

        // Capture segment layout information, so that we can check that after aborted operation it is still unchanged.
        val expectedSegmentMetadata = TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName);
        val expectedChunkMetadataList = TestUtils.getChunkList(spyMetadataStore, testSegmentName);

        // Step 3: Inject fault.
        doThrow(exceptionToThrow).when(spyMetadataStore).commit(any(), anyBoolean(), anyBoolean());

        AssertExtensions.assertFutureThrows(
                "openWrite succeeded when exception was expected.",
                storageManager.write(h1, 10, new ByteArrayInputStream(new byte[10]), 10, null),
                ex -> clazz.equals(ex.getClass()));

        // Make sure 15 chunks in total were created and then 5 of them garbage collected later.
        verify(spyChunkStorageProvider, times(15)).doCreate(anyString());
        verify(spyChunkStorageProvider, times(5)).doDelete(any());

        // seal.
        AssertExtensions.assertFutureThrows(
                "Seal succeeded when exception was expected.",
                storageManager.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> clazz.equals(ex.getClass()));

        TestUtils.assertEquals(expectedSegmentMetadata,
                expectedChunkMetadataList,
                TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                TestUtils.getChunkList(spyMetadataStore, testSegmentName));
        TestUtils.checkChunksExistInStorage(spyChunkStorageProvider, spyMetadataStore, testSegmentName);

        // openWrite.
        AssertExtensions.assertFutureThrows(
                "openWrite succeeded when exception was expected.",
                storageManager.openWrite(testSegmentName),
                ex -> clazz.equals(ex.getClass()));

        TestUtils.assertEquals(expectedSegmentMetadata,
                expectedChunkMetadataList,
                TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                TestUtils.getChunkList(spyMetadataStore, testSegmentName));

        // delete.
        AssertExtensions.assertFutureThrows(
                "delete succeeded when exception was expected.",
                storageManager.delete(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> clazz.equals(ex.getClass()));
        TestUtils.checkChunksExistInStorage(spyChunkStorageProvider, spyMetadataStore, testSegmentName);

        TestUtils.assertEquals(expectedSegmentMetadata,
                expectedChunkMetadataList,
                TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                TestUtils.getChunkList(spyMetadataStore, testSegmentName));

        // truncate.
        AssertExtensions.assertFutureThrows(
                "truncate succeeded when exception was expected.",
                storageManager.truncate(SegmentStorageHandle.writeHandle(testSegmentName),
                        2, null),
                ex -> clazz.equals(ex.getClass()));
        TestUtils.assertEquals(expectedSegmentMetadata,
                expectedChunkMetadataList,
                TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                TestUtils.getChunkList(spyMetadataStore, testSegmentName));
        TestUtils.checkChunksExistInStorage(spyChunkStorageProvider, spyMetadataStore, testSegmentName);

        if (!skipCreate) {
            // create.
            AssertExtensions.assertFutureThrows(
                    "create succeeded when exception was expected.",
                    storageManager.create("foo", policy, null),
                    ex -> clazz.equals(ex.getClass()));
        }
        if (!skipConcat) {
            // concat.
            AssertExtensions.assertFutureThrows(
                    "concat succeeded when exception was expected.",
                    storageManager.concat(h1, 10, h2.getSegmentName(), null),
                    ex -> clazz.equals(ex.getClass()));
            TestUtils.assertEquals(expectedSegmentMetadata,
                    expectedChunkMetadataList,
                    TestUtils.getSegmentMetadata(spyMetadataStore, testSegmentName),
                    TestUtils.getChunkList(spyMetadataStore, testSegmentName));
            TestUtils.checkChunksExistInStorage(spyChunkStorageProvider, spyMetadataStore, testSegmentName);
        }
    }

    @Test
    public void testExceptionDuringMetadataRead() throws Exception {
        Exception exceptionToThrow = new StorageMetadataException("Test Exception");
        val clazz = StorageMetadataException.class;
        testExceptionDuringMetadataRead(exceptionToThrow, clazz);
    }

    public void testExceptionDuringMetadataRead(Exception exceptionToThrow, Class clazz) throws Exception {
        String testSegmentName = "test";
        String concatSegmentName = "concat";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        BaseMetadataStore spyMetadataStore = spy(InMemoryMetadataStore.class);
        ChunkStorage spyChunkStorage = spy(NoOpChunkStorage.class);
        ChunkedSegmentStorage storageManager = new ChunkedSegmentStorage(spyChunkStorage, spyMetadataStore, executorService(), config);
        storageManager.initialize(1);

        // Step 1: Create segment and write some data.
        val h1 = storageManager.create(testSegmentName, policy, null).get();

        Assert.assertEquals(h1.getSegmentName(), testSegmentName);
        Assert.assertFalse(h1.isReadOnly());
        storageManager.write(h1, 0, new ByteArrayInputStream(new byte[10]), 10, null);

        // Step 2: Increase epoch.
        storageManager.initialize(2);
        val h2 = storageManager.create(concatSegmentName, policy, null).get();
        storageManager.write(h2, 0, new ByteArrayInputStream(new byte[10]), 10, null);
        storageManager.seal(h2, null);

        // Step 3: Inject fault.
        doThrow(exceptionToThrow).when(spyMetadataStore).get(any(), anyString());

        AssertExtensions.assertFutureThrows(
                "openWrite succeeded when exception was expected.",
                storageManager.write(h2, 10, new ByteArrayInputStream(new byte[10]), 10, null),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertFutureThrows(
                "Seal succeeded when exception was expected.",
                storageManager.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertFutureThrows(
                "openWrite succeeded when exception was expected.",
                storageManager.openWrite(testSegmentName),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertFutureThrows(
                "delete succeeded when exception was expected.",
                storageManager.delete(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertFutureThrows(
                "truncate succeeded when exception was expected.",
                storageManager.truncate(SegmentStorageHandle.writeHandle(testSegmentName),
                        2, null),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertFutureThrows(
                "create succeeded when exception was expected.",
                storageManager.create("foo", policy, null),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertFutureThrows(
                "concat succeeded when exception was expected.",
                storageManager.concat(h1, 10, h2.getSegmentName(), null),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertFutureThrows(
                "getStreamSegmentInfo succeeded  when exception was expected.",
                storageManager.getStreamSegmentInfo(testSegmentName, null),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertFutureThrows(
                "read  succeeded when exception was expected.",
                storageManager.read(SegmentStorageHandle.readHandle(testSegmentName), 0, new byte[1], 0, 1, null),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertFutureThrows(
                "openRead  succeeded when exception was expected.",
                storageManager.openRead(testSegmentName),
                ex -> clazz.equals(ex.getClass()));

        AssertExtensions.assertFutureThrows(
                "exists  succeeded when exception was expected.",
                storageManager.exists(testSegmentName, null),
                ex -> clazz.equals(ex.getClass()));

    }

    @Test
    public void testIOExceptionDuringWrite() throws Exception {
        String testSegmentName = "test";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        BaseMetadataStore spyMetadataStore = spy(InMemoryMetadataStore.class);
        BaseChunkStorage spyChunkStorageProvider = spy(NoOpChunkStorage.class);
        ((NoOpChunkStorage) spyChunkStorageProvider).setShouldSupportConcat(false);
        ChunkedSegmentStorage storageManager = new ChunkedSegmentStorage(spyChunkStorageProvider, spyMetadataStore, executorService(), config);
        storageManager.initialize(1);

        // Step 1: Create segment and write some data.
        val h1 = storageManager.create(testSegmentName, policy, null).get();

        Assert.assertEquals(h1.getSegmentName(), testSegmentName);
        Assert.assertFalse(h1.isReadOnly());
        storageManager.write(h1, 0, new ByteArrayInputStream(new byte[10]), 10, null);

        // Step 2: Inject fault.
        Exception exceptionToThrow = new ChunkStorageException("test", "Test Exception", new IOException("Test Exception"));
        val clazz = ChunkStorageException.class;
        doThrow(exceptionToThrow).when(spyChunkStorageProvider).doWrite(any(), anyLong(), anyInt(), any());

        AssertExtensions.assertFutureThrows(
                "write succeeded when exception was expected.",
                storageManager.write(h1, 10, new ByteArrayInputStream(new byte[10]), 10, null),
                ex -> clazz.equals(ex.getClass()));

        verify(spyChunkStorageProvider, times(1)).doDelete(any());
    }

    @Test
    public void testFileNotFoundExceptionDuringGarbageCollection() throws Exception {
        String testSegmentName = "test";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        BaseMetadataStore spyMetadataStore = spy(InMemoryMetadataStore.class);
        BaseChunkStorage spyChunkStorageProvider = spy(NoOpChunkStorage.class);
        ((NoOpChunkStorage) spyChunkStorageProvider).setShouldSupportConcat(false);
        ChunkedSegmentStorage storageManager = new ChunkedSegmentStorage(spyChunkStorageProvider, spyMetadataStore, executorService(), config);
        storageManager.initialize(1);

        // Step 1: Create segment and write some data.
        val h1 = storageManager.create(testSegmentName, policy, null).get();
        storageManager.write(h1, 0, new ByteArrayInputStream(new byte[10]), 10, null);

        Assert.assertEquals(h1.getSegmentName(), testSegmentName);
        Assert.assertFalse(h1.isReadOnly());
        // Step 2: Inject fault.
        Exception exceptionToThrow = new ChunkNotFoundException("Test Exception", "Mock Exception", new Exception("Mock Exception"));
        doThrow(exceptionToThrow).when(spyChunkStorageProvider).doDelete(any());

        storageManager.delete(h1, null);
        verify(spyChunkStorageProvider, times(5)).doDelete(any());
    }

    @Test
    public void testExceptionDuringGarbageCollection() throws Exception {
        String testSegmentName = "test";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        BaseMetadataStore spyMetadataStore = spy(InMemoryMetadataStore.class);
        BaseChunkStorage spyChunkStorageProvider = spy(NoOpChunkStorage.class);
        ((NoOpChunkStorage) spyChunkStorageProvider).setShouldSupportConcat(false);
        ChunkedSegmentStorage storageManager = new ChunkedSegmentStorage(spyChunkStorageProvider, spyMetadataStore, executorService(), config);
        storageManager.initialize(1);

        // Step 1: Create segment and write some data.
        val h1 = storageManager.create(testSegmentName, policy, null).get();
        storageManager.write(h1, 0, new ByteArrayInputStream(new byte[10]), 10, null);

        Assert.assertEquals(h1.getSegmentName(), testSegmentName);
        Assert.assertFalse(h1.isReadOnly());
        // Step 2: Inject fault.
        Exception exceptionToThrow = new IllegalStateException("Test Exception");
        doThrow(exceptionToThrow).when(spyChunkStorageProvider).doDelete(any());

        storageManager.delete(h1, null);
        verify(spyChunkStorageProvider, times(5)).doDelete(any());
    }
}
