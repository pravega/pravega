/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.test.common.AssertExtensions;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletionException;
import lombok.val;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *  Test TableBasedMetadataStore using mock {@link TableStore}.
 */
public class TableBasedMetadataStoreMockTests {
    @Test
    public void testIllegalStateExceptionDuringRead() {
        TableStore mockTableStore = mock(TableStore.class);
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore);

        when(mockTableStore.createSegment(any(), any(), any())).thenThrow(new CompletionException(new StreamSegmentExistsException("test")));
        when(mockTableStore.get(anyString(), any(), any())).thenThrow(new IllegalStateException());
        AssertExtensions.assertThrows(
                "read should throw an exception",
                () -> tableBasedMetadataStore.read("test"),
                ex -> ex instanceof IllegalStateException);
    }

    @Test
    public void testRandomExceptionDuringRead() {
        TableStore mockTableStore = mock(TableStore.class);
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore);

        when(mockTableStore.createSegment(any(), any(), any())).thenThrow(new CompletionException(new StreamSegmentExistsException("test")));
        // Throw random exception
        Exception e = new ArithmeticException();
        when(mockTableStore.get(anyString(), any(), any())).thenThrow(e);
        AssertExtensions.assertThrows(
                "read should throw an exception",
                () -> tableBasedMetadataStore.read("test"),
                ex -> ex instanceof StorageMetadataException && ex.getCause() == e);
    }

    @Test
    public void testDataLogWriterNotPrimaryExceptionDuringWrite() {
        TableStore mockTableStore = mock(TableStore.class);
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore);

        when(mockTableStore.createSegment(any(), any(), any())).thenThrow(new CompletionException(new StreamSegmentExistsException("test")));

        // Throw DataLogWriterNotPrimaryException exception
        Exception e = new CompletionException(new DataLogWriterNotPrimaryException("test"));
        val td = BaseMetadataStore.TransactionData.builder().key("foo").version(1L).dbObject(2L).build();
        when(mockTableStore.put(anyString(), any(), any())).thenThrow(e);

        AssertExtensions.assertThrows(
                "write should throw an exception",
                () -> tableBasedMetadataStore.writeAll(Collections.singleton(td)),
                ex -> ex instanceof StorageMetadataWritesFencedOutException && ex.getCause() == e.getCause());
    }

    @Test
    public void testBadKeyVersionExceptionDuringWrite() {
        TableStore mockTableStore = mock(TableStore.class);
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore);

        when(mockTableStore.createSegment(any(), any(), any())).thenThrow(new CompletionException(new StreamSegmentExistsException("test")));

        // Throw BadKeyVersionException exception
        Exception e = new CompletionException(new BadKeyVersionException("test", new HashMap<>()));
        val td = BaseMetadataStore.TransactionData.builder().key("foo").version(1L).dbObject(2L).build();
        when(mockTableStore.put(anyString(), any(), any())).thenThrow(e);

        AssertExtensions.assertThrows(
                "write should throw an excpetion",
                () -> tableBasedMetadataStore.writeAll(Collections.singleton(td)),
                ex -> ex instanceof StorageMetadataVersionMismatchException && ex.getCause() == e.getCause());
    }

    @Test
    public void testRandomRuntimeExceptionDuringWrite() {
        TableStore mockTableStore = mock(TableStore.class);
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore);

        when(mockTableStore.createSegment(any(), any(), any())).thenThrow(new CompletionException(new StreamSegmentExistsException("test")));

        // Throw random exception
        Exception e = new ArithmeticException();
        val td = BaseMetadataStore.TransactionData.builder().key("foo").version(1L).dbObject(2L).build();
        when(mockTableStore.put(anyString(), any(), any())).thenThrow(e);

        AssertExtensions.assertThrows(
                "write should throw an exception",
                () -> tableBasedMetadataStore.writeAll(Collections.singleton(td)),
                ex -> ex instanceof StorageMetadataException && ex.getCause() == e);
    }

    @Test
    public void testRandomExceptionDuringWrite() {
        TableStore mockTableStore = mock(TableStore.class);
        TableBasedMetadataStore tableBasedMetadataStore = new TableBasedMetadataStore("test", mockTableStore);

        when(mockTableStore.createSegment(any(), any(), any())).thenThrow(new CompletionException(new StreamSegmentExistsException("test")));

        // Make it throw IllegalStateException
        val td = BaseMetadataStore.TransactionData.builder().key("foo").version(1L).dbObject(null).build();

        AssertExtensions.assertThrows(
                "write should throw an exception",
                () -> tableBasedMetadataStore.writeAll(Collections.singleton(td)),
                ex -> ex instanceof StorageMetadataException && ex.getCause() instanceof IllegalStateException);
    }
}
