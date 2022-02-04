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
package io.pravega.segmentstore.storage.metadata;

import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.mocks.InMemoryTableStore;
import io.pravega.segmentstore.storage.mocks.MockStorageMetadata;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 *  Test TableBasedMetadataStore using mock {@link TableStore}.
 */

public class TableBasedMetadataStoreMockTests extends ThreadPooledTestSuite {
    @Test
    public void testIllegalStateExceptionDuringRead() {
        TableStore mockTableStore = mock(TableStore.class);
        @Cleanup
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());

        when(mockTableStore.createSegment(any(), any(), any())).thenReturn(Futures.failedFuture(new CompletionException(new StreamSegmentExistsException("test"))));
        when(mockTableStore.get(anyString(), any(), any())).thenThrow(new IllegalStateException());
        AssertExtensions.assertFutureThrows(
                "read should throw an exception",
                tableBasedMetadataStore.read("test"),
                ex -> ex instanceof IllegalStateException);
    }

    @Test
    public void testBadReadExceptionDuringRead() {
        TableStore mockTableStore = mock(TableStore.class);
        TableBasedMetadataStore tableBasedMetadataStore = spy(new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService()));

        when(mockTableStore.createSegment(any(), any(), any())).thenReturn(Futures.failedFuture(new CompletionException(new StreamSegmentExistsException("test"))));
        when(tableBasedMetadataStore.read("test")).thenReturn(CompletableFuture.completedFuture(null));
        val txn = tableBasedMetadataStore.beginTransaction(true, "test");
        AssertExtensions.assertFutureThrows(
                "read should throw an exception",
                tableBasedMetadataStore.get(txn, "test"),
                ex -> ex instanceof IllegalStateException);
    }

    @Test
    public void testBadReadMissingDbObjectDuringRead() {
        TableStore mockTableStore = mock(TableStore.class);
        TableBasedMetadataStore tableBasedMetadataStore = spy(new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService()));

        when(mockTableStore.createSegment(any(), any(), any())).thenReturn(Futures.failedFuture(new CompletionException(new StreamSegmentExistsException("test"))));
        when(tableBasedMetadataStore.read("test")).thenReturn(CompletableFuture.completedFuture(BaseMetadataStore.TransactionData.builder()
                .key("test")
                .build()));
        val txn = tableBasedMetadataStore.beginTransaction(true, "test");
        AssertExtensions.assertFutureThrows(
                "read should throw an exception",
                tableBasedMetadataStore.get(txn, "test"),
                ex -> ex instanceof IllegalStateException);
    }

    @Test
    public void testBadReadMissingNoVersionDuringRead() {
        TableStore mockTableStore = mock(TableStore.class);
        @Cleanup
        TableBasedMetadataStore tableBasedMetadataStore = spy(new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService()));

        when(mockTableStore.createSegment(any(), any(), any())).thenReturn(Futures.failedFuture(new CompletionException(new StreamSegmentExistsException("test"))));
        when(tableBasedMetadataStore.read("test")).thenReturn(CompletableFuture.completedFuture(BaseMetadataStore.TransactionData.builder()
                .key("test")
                .value(new MockStorageMetadata("key", "value"))
                .dbObject(Long.valueOf(10))
                .build()));
        val txn = tableBasedMetadataStore.beginTransaction(true, "test");
        AssertExtensions.assertFutureThrows(
                "read should throw an exception",
                tableBasedMetadataStore.get(txn, "test"),
                ex -> ex instanceof IllegalStateException);
    }

    @Test
    public void testRandomExceptionDuringRead() {
        TableStore mockTableStore = mock(TableStore.class);
        @Cleanup
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());

        when(mockTableStore.createSegment(any(), any(), any())).thenReturn(Futures.failedFuture(new CompletionException(new StreamSegmentExistsException("test"))));
        // Throw random exception
        Exception e = new ArithmeticException();
        val f = new CompletableFuture<List<TableEntry>>();
        f.completeExceptionally(e);
        when(mockTableStore.get(anyString(), any(), any())).thenReturn(f);
        AssertExtensions.assertFutureThrows(
                "read should throw an exception",
                tableBasedMetadataStore.read("test"),
                ex -> ex instanceof StorageMetadataException && ex.getCause() == e);
    }

    @Test
    public void testDataLogWriterNotPrimaryExceptionDuringWrite() {
        TableStore mockTableStore = mock(TableStore.class);
        @Cleanup
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());

        when(mockTableStore.createSegment(any(), any(), any())).thenReturn(Futures.failedFuture(new CompletionException(new StreamSegmentExistsException("test"))));

        // Throw DataLogWriterNotPrimaryException exception
        Exception e = new CompletionException(new DataLogWriterNotPrimaryException("test"));
        val td = BaseMetadataStore.TransactionData.builder().key("foo").version(1L).dbObject(2L).build();
        CompletableFuture<List<Long>> f = new CompletableFuture<>();
        f.completeExceptionally(e);
        when(mockTableStore.put(anyString(), any(), any())).thenReturn(f);

        AssertExtensions.assertFutureThrows(
                "write should throw an exception",
                tableBasedMetadataStore.writeAll(Collections.singleton(td)),
                ex -> ex instanceof StorageMetadataWritesFencedOutException && ex.getCause() == e.getCause());
    }

    @Test
    public void testBadKeyVersionExceptionDuringWrite() {
        TableStore mockTableStore = mock(TableStore.class);
        @Cleanup
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());

        when(mockTableStore.createSegment(any(), any(), any())).thenReturn(Futures.failedFuture(new CompletionException(new StreamSegmentExistsException("test"))));

        // Throw BadKeyVersionException exception
        Exception e = new CompletionException(new BadKeyVersionException("test", new HashMap<>()));
        val td = BaseMetadataStore.TransactionData.builder().key("foo").version(1L).dbObject(2L).build();
        CompletableFuture<List<Long>> f = new CompletableFuture<>();
        f.completeExceptionally(e);
        when(mockTableStore.put(anyString(), any(), any())).thenReturn(f);

        AssertExtensions.assertFutureThrows(
                "write should throw an excpetion",
                tableBasedMetadataStore.writeAll(Collections.singleton(td)),
                ex -> ex instanceof StorageMetadataVersionMismatchException && ex.getCause() == e.getCause());
    }

    @Test
    public void testRandomRuntimeExceptionDuringWrite() {
        TableStore mockTableStore = mock(TableStore.class);
        @Cleanup
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());

        when(mockTableStore.createSegment(any(), any(), any())).thenReturn(Futures.failedFuture(new CompletionException(new StreamSegmentExistsException("test"))));

        // Throw random exception
        Exception e = new ArithmeticException();
        val td = BaseMetadataStore.TransactionData.builder().key("foo").version(1L).dbObject(2L).build();
        CompletableFuture<List<Long>> f = new CompletableFuture<>();
        f.completeExceptionally(e);
        when(mockTableStore.put(anyString(), any(), any())).thenReturn(f);

        AssertExtensions.assertFutureThrows(
                "write should throw an exception",
                tableBasedMetadataStore.writeAll(Collections.singleton(td)),
                ex -> ex instanceof StorageMetadataException && ex.getCause() == e);
    }

    @Test
    public void testExceptionDuringRemove() throws Exception {
        TableStore mockTableStore = mock(TableStore.class);
        @Cleanup
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());

        when(mockTableStore.createSegment(any(), any(), any())).thenReturn(Futures.failedFuture(new CompletionException(new StreamSegmentExistsException("test"))));

        // Throw random exception
        Exception e = new ArithmeticException();
        val td = BaseMetadataStore.TransactionData.builder().key("foo").version(1L).dbObject(2L).build();
        val toRet = new ArrayList<Long>();
        toRet.add(3L);
        CompletableFuture<Void> f = new CompletableFuture<>();
        f.completeExceptionally(e);
        when(mockTableStore.remove(anyString(), any(), any())).thenReturn(f);
        when(mockTableStore.put(anyString(), any(), any())).thenReturn(CompletableFuture.completedFuture(toRet));
        tableBasedMetadataStore.writeAll(Collections.singleton(td)).get();
    }

    @Test
    public void testExceptionDuringRemoveWithSpy() throws Exception {
        TableStore mockTableStore = spy(new InMemoryTableStore(executorService()));
        @Cleanup
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());

        // Step 1 - set up keys
        try (val txn = tableBasedMetadataStore.beginTransaction(false, "TEST")) {
            txn.create(new MockStorageMetadata("key1", "A"));
            txn.create(new MockStorageMetadata("key2", "B"));
            txn.create(new MockStorageMetadata("key3", "C"));
            txn.commit().join();
        }

        // Step 2 - Throw random exception
        Exception e = new ArithmeticException();
        val td = BaseMetadataStore.TransactionData.builder().key("foo").version(1L).dbObject(2L).build();
        CompletableFuture<Void> f = new CompletableFuture<>();
        f.completeExceptionally(e);
        when(mockTableStore.remove(anyString(), any(), any())).thenReturn(f);

        // Step 3 - modify some keys and delete a key. This uses mock method that throws exception
        try (val txn = tableBasedMetadataStore.beginTransaction(false, "TEST")) {
            Assert.assertEquals("A", ((MockStorageMetadata) txn.get("key1").get()).getValue());
            Assert.assertEquals("B", ((MockStorageMetadata) txn.get("key2").get()).getValue());
            Assert.assertEquals("C", ((MockStorageMetadata) txn.get("key3").get()).getValue());
            txn.update(new MockStorageMetadata("key1", "a"));
            txn.update(new MockStorageMetadata("key2", "b"));
            txn.delete("key3");
            txn.commit().join();
        }

        // Step 4 - Validate results and then retry delete with no mocking.
        try (val txn = tableBasedMetadataStore.beginTransaction(false, "TEST")) {
            Assert.assertEquals("a", ((MockStorageMetadata) txn.get("key1").get()).getValue());
            Assert.assertEquals("b", ((MockStorageMetadata) txn.get("key2").get()).getValue());
            Assert.assertEquals(null, txn.get("key3").get());
            val direct = tableBasedMetadataStore.read("key3").get();
            Assert.assertNotNull(direct);
            Assert.assertNotEquals(-1L, direct.getDbObject());
            txn.delete("key3");

            // stop mocking
            when(mockTableStore.remove(anyString(), any(), any())).thenCallRealMethod();

            txn.commit().join();
        }

        // Step 5 - Validate results.
        try (val txn = tableBasedMetadataStore.beginTransaction(true, "TEST")) {
            Assert.assertEquals("a", ((MockStorageMetadata) txn.get("key1").get()).getValue());
            Assert.assertEquals("b", ((MockStorageMetadata) txn.get("key2").get()).getValue());
            Assert.assertEquals(null, txn.get("key3").get());
            val direct = tableBasedMetadataStore.read("key3").get();
            Assert.assertNotNull(direct);
            Assert.assertEquals(-1L, direct.getDbObject());
        }
    }

    @Test
    public void testRandomExceptionDuringWrite() {
        TableStore mockTableStore = mock(TableStore.class);
        @Cleanup
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());

        when(mockTableStore.createSegment(any(), any(), any())).thenReturn(Futures.failedFuture(new CompletionException(new StreamSegmentExistsException("test"))));

        // Make it throw IllegalStateException
        val td = BaseMetadataStore.TransactionData.builder().key("foo").version(1L).dbObject(null).build();

        AssertExtensions.assertFutureThrows(
                "write should throw an exception",
                tableBasedMetadataStore.writeAll(Collections.singleton(td)),
                ex -> ex instanceof StorageMetadataException && ex.getCause() instanceof IllegalStateException);
    }

    @Test
    public void testExceptionDuringIterator() {
        TableStore mockTableStore = mock(TableStore.class);
        @Cleanup
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());

        when(mockTableStore.entryIterator(anyString(), any())).thenReturn(Futures.failedFuture(new CompletionException(new IOException("test"))));

        AssertExtensions.assertFutureThrows(
                "getAllEntries",
                tableBasedMetadataStore.getAllEntries(),
                ex -> ex instanceof StorageMetadataException && ex.getCause() instanceof IOException);

        AssertExtensions.assertFutureThrows(
                "getAllKeys",
                tableBasedMetadataStore.getAllKeys(),
                ex -> ex instanceof StorageMetadataException && ex.getCause() instanceof IOException);
    }

}
