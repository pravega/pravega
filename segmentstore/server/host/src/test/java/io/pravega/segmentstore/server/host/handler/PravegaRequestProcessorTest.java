/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.segmentstore.server.mocks.SynchronousStreamSegmentStore;
import io.pravega.segmentstore.server.reading.ReadResultEntryBase;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.server.store.StreamSegmentService;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.TestUtils;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@Slf4j
public class PravegaRequestProcessorTest {

    private static final int MAX_KEY_LENGTH = 100;
    private static final int MAX_VALUE_LENGTH = 100;

    static {
        MetricsProvider.initialize(MetricsConfig.builder().with(MetricsConfig.ENABLE_STATISTICS, true).build());
    }

    //region Stream Segments

    @Data
    private static class TestReadResult implements ReadResult {
        final long streamSegmentStartOffset;
        final int maxResultLength;
        boolean closed = false;
        final List<ReadResultEntry> results;
        long currentOffset = 0;

        @Override
        public boolean hasNext() {
            return !results.isEmpty();
        }

        @Override
        public ReadResultEntry next() {
            ReadResultEntry result = results.remove(0);
            currentOffset = result.getStreamSegmentOffset();
            return result;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public int getConsumedLength() {
            return (int) (currentOffset - streamSegmentStartOffset);
        }
    }

    private static class TestReadResultEntry extends ReadResultEntryBase {
        TestReadResultEntry(ReadResultEntryType type, long streamSegmentOffset, int requestedReadLength) {
            super(type, streamSegmentOffset, requestedReadLength);
        }

        @Override
        protected void complete(ReadResultEntryContents readResultEntryContents) {
            super.complete(readResultEntryContents);
        }

        @Override
        protected void fail(Throwable exception) {
            super.fail(exception);
        }

        @Override
        public void requestContent(Duration timeout) {
            Preconditions.checkState(getType() != ReadResultEntryType.EndOfStreamSegment, "EndOfStreamSegmentReadResultEntry does not have any content.");
        }
    }

    @Test(timeout = 20000)
    public void testReadSegment() {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "testReadSegment";
        byte[] data = new byte[]{1, 2, 3, 4, 6, 7, 8, 9};
        int readLength = 1000;

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), connection);

        TestReadResultEntry entry1 = new TestReadResultEntry(ReadResultEntryType.Cache, 0, readLength);
        entry1.complete(new ReadResultEntryContents(new ByteArrayInputStream(data), data.length));
        TestReadResultEntry entry2 = new TestReadResultEntry(ReadResultEntryType.Future, data.length, readLength);

        List<ReadResultEntry> results = new ArrayList<>();
        results.add(entry1);
        results.add(entry2);
        CompletableFuture<ReadResult> readResult = new CompletableFuture<>();
        readResult.complete(new TestReadResult(0, readLength, results));
        when(store.read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult);

        // Execute and Verify readSegment calling stack in connection and store is executed as design.
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength, ""));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        verify(connection).send(new WireCommands.SegmentRead(streamSegmentName, 0, true, false, ByteBuffer.wrap(data)));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
        entry2.complete(new ReadResultEntryContents(new ByteArrayInputStream(data), data.length));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 20000)
    public void testReadSegmentEmptySealed() {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "testReadSegment";
        int readLength = 1000;

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection);

        TestReadResultEntry entry1 = new TestReadResultEntry(ReadResultEntryType.EndOfStreamSegment, 0, readLength);

        List<ReadResultEntry> results = new ArrayList<>();
        results.add(entry1);
        CompletableFuture<ReadResult> readResult = new CompletableFuture<>();
        readResult.complete(new TestReadResult(0, readLength, results));
        when(store.read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult);

        // Execute and Verify readSegment calling stack in connection and store is executed as design.
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength, ""));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        verify(connection).send(new WireCommands.SegmentRead(streamSegmentName, 0, false, true, ByteBuffer.wrap(new byte[0])));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 20000)
    public void testReadSegmentWithCancellationException() {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "testReadSegment";
        int readLength = 1000;

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection);

        CompletableFuture<ReadResult> readResult = new CompletableFuture<>();
        readResult.completeExceptionally(new CancellationException("cancel read"));
        // Simulate a CancellationException for a Read Segment.
        when(store.read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult);

        // Execute and Verify readSegment is calling stack in connection and store is executed as design.
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength, ""));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        // Since the underlying store cancels the read request verify if an empty SegmentRead Wirecommand is sent as a response.
        verify(connection).send(new WireCommands.SegmentRead(streamSegmentName, 0, true, false, ByteBuffer.wrap(new byte[0])));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }


    @Test(timeout = 20000)
    public void testReadSegmentTruncated() {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "testReadSegment";
        int readLength = 1000;

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection);

        TestReadResultEntry entry1 = new TestReadResultEntry(ReadResultEntryType.Truncated, 0, readLength);

        List<ReadResultEntry> results = new ArrayList<>();
        results.add(entry1);
        CompletableFuture<ReadResult> readResult = new CompletableFuture<>();
        readResult.complete(new TestReadResult(0, readLength, results));
        when(store.read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult);

        StreamSegmentInformation info = StreamSegmentInformation.builder()
                .name(streamSegmentName)
                .length(1234)
                .startOffset(123)
                .build();
        when(store.getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(info));

        // Execute and Verify readSegment calling stack in connection and store is executed as design.
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength, ""));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        verify(store).getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT);
        verify(connection).send(new WireCommands.SegmentIsTruncated(0, streamSegmentName, info.getStartOffset(), ""));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 20000)
    public void testCreateSegment() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        String streamSegmentName = "testCreateSegment";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(SegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), connection, recorderMock,
                TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(), false);

        // Execute and Verify createSegment/getStreamSegmentInfo calling stack is executed as design.
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, ""));
        verify(recorderMock).createSegment(eq(streamSegmentName), eq(WireCommands.CreateSegment.NO_SCALE), eq(0), any());
        assertTrue(append(streamSegmentName, 1, store));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(1, streamSegmentName, ""));
        assertTrue(append(streamSegmentName, 2, store));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, streamSegmentName));
        order.verify(connection).send(Mockito.any(WireCommands.StreamSegmentInfo.class));

        // TestCreateSealDelete may executed before this test case,
        // so createSegmentStats may record 1 or 2 createSegment operation here.
    }

    @Test(timeout = 20000)
    public void testTransaction() throws Exception {
        String streamSegmentName = "testTxn";
        UUID txnid = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection);

        processor.createSegment(new WireCommands.CreateSegment(0, streamSegmentName,
                WireCommands.CreateSegment.NO_SCALE, 0, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(0, streamSegmentName));

        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid);
        processor.createSegment(new WireCommands.CreateSegment(1, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, ""));
        assertTrue(append(StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), 1, store));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(2, transactionName, ""));
        assertTrue(append(StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), 2, store));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, transactionName));
        order.verify(connection).send(Mockito.argThat(t -> {
            return t instanceof WireCommands.StreamSegmentInfo && ((WireCommands.StreamSegmentInfo) t).exists();
        }));
        processor.mergeSegments(new WireCommands.MergeSegments(3, streamSegmentName, transactionName, ""));
        order.verify(connection).send(new WireCommands.SegmentsMerged(3, streamSegmentName, transactionName));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(4, transactionName, ""));
        order.verify(connection)
                .send(new WireCommands.NoSuchSegment(4, StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), ""));

        txnid = UUID.randomUUID();
        transactionName = StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid);

        processor.createSegment(new WireCommands.CreateSegment(1, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, ""));
        assertTrue(append(StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), 1, store));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, transactionName));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(2, transactionName, ""));
        order.verify(connection).send(Mockito.argThat(t -> {
            return t instanceof WireCommands.StreamSegmentInfo && ((WireCommands.StreamSegmentInfo) t).exists();
        }));
        processor.deleteSegment(new WireCommands.DeleteSegment(3, transactionName, ""));
        order.verify(connection).send(new WireCommands.SegmentDeleted(3, transactionName));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(4, transactionName, ""));
        order.verify(connection)
                .send(new WireCommands.NoSuchSegment(4, StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), ""));

        // Verify the case when the transaction segment is already sealed. This simulates the case when the process
        // crashed after sealing, but before issuing the merge.
        txnid = UUID.randomUUID();
        transactionName = StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid);

        processor.createSegment(new WireCommands.CreateSegment(1, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, ""));
        assertTrue(append(StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), 1, store));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(2, transactionName, ""));
        assertTrue(append(StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), 2, store));

        // Seal the transaction in the SegmentStore.
        String txnName = StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid);
        store.sealStreamSegment(txnName, Duration.ZERO).join();

        processor.mergeSegments(new WireCommands.MergeSegments(3, streamSegmentName, transactionName, ""));
        order.verify(connection).send(new WireCommands.SegmentsMerged(3, streamSegmentName, transactionName));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(4, transactionName, ""));
        order.verify(connection)
                .send(new WireCommands.NoSuchSegment(4, StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), ""));

        order.verifyNoMoreInteractions();
    }

    @Test(timeout = 20000)
    public void testMergedTransaction() throws Exception {
        String streamSegmentName = "testMergedTxn";
        UUID txnid = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        doReturn(Futures.failedFuture(new StreamSegmentMergedException(streamSegmentName))).when(store).sealStreamSegment(
                anyString(), any());
        doReturn(Futures.failedFuture(new StreamSegmentMergedException(streamSegmentName))).when(store).mergeStreamSegment(
                anyString(), anyString(), any());

        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection);

        processor.createSegment(new WireCommands.CreateSegment(0, streamSegmentName,
                WireCommands.CreateSegment.NO_SCALE, 0, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(0, streamSegmentName));

        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid);

        processor.createSegment(new WireCommands.CreateSegment(1, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, transactionName));
        processor.mergeSegments(new WireCommands.MergeSegments(2, streamSegmentName, transactionName, ""));
        order.verify(connection).send(new WireCommands.SegmentsMerged(2, streamSegmentName, transactionName));

        txnid = UUID.randomUUID();
        transactionName = StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid);

        doReturn(Futures.failedFuture(new StreamSegmentNotExistsException(streamSegmentName))).when(store).sealStreamSegment(
                anyString(), any());
        doReturn(Futures.failedFuture(new StreamSegmentNotExistsException(streamSegmentName))).when(store).mergeStreamSegment(
                anyString(), anyString(), any());

        processor.createSegment(new WireCommands.CreateSegment(3, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(3, transactionName));
        processor.mergeSegments(new WireCommands.MergeSegments(4, streamSegmentName, transactionName, ""));

        order.verify(connection).send(new WireCommands.NoSuchSegment(4, StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), ""));
    }

    @Test(timeout = 20000)
    public void testMetricsOnSegmentMerge() throws Exception {
        String streamSegmentName = "txnSegment";
        UUID txnId = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        doReturn(Futures.failedFuture(new StreamSegmentMergedException(streamSegmentName))).when(store).sealStreamSegment(
                anyString(), any());

        //test txn segment merge
        CompletableFuture<SegmentProperties> txnFuture = CompletableFuture.completedFuture(createSegmentProperty(streamSegmentName, txnId));
        doReturn(txnFuture).when(store).mergeStreamSegment(anyString(), anyString(), any());
        SegmentStatsRecorder recorderMock = mock(SegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), connection, recorderMock,
                TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(), false);

        processor.createSegment(new WireCommands.CreateSegment(0, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, ""));
        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnId);
        processor.createSegment(new WireCommands.CreateSegment(1, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, ""));
        processor.mergeSegments(new WireCommands.MergeSegments(2, streamSegmentName, transactionName, ""));
        verify(recorderMock).merge(streamSegmentName, 100L, 10, (long) streamSegmentName.hashCode());
    }

    private SegmentProperties createSegmentProperty(String streamSegmentName, UUID txnId) {

        Map<UUID, Long> attributes = new HashMap<>();
        attributes.put(Attributes.EVENT_COUNT, 10L);
        attributes.put(Attributes.CREATION_TIME, (long) streamSegmentName.hashCode());

        return StreamSegmentInformation.builder()
                .name(txnId == null ? streamSegmentName + "#." : streamSegmentName + "#transaction." + txnId)
                .sealed(true)
                .deleted(false)
                .lastModified(null)
                .startOffset(0)
                .length(100)
                .attributes(attributes)
                .build();
    }

    @Test(timeout = 20000)
    public void testSegmentAttribute() throws Exception {
        String streamSegmentName = "testSegmentAttribute";
        UUID attribute = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection);

        // Execute and Verify createSegment/getStreamSegmentInfo calling stack is executed as design.
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, streamSegmentName));

        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(2, streamSegmentName, attribute, ""));
        order.verify(connection).send(new WireCommands.SegmentAttribute(2, WireCommands.NULL_ATTRIBUTE_VALUE));

        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(2, streamSegmentName, attribute, 1, WireCommands.NULL_ATTRIBUTE_VALUE, ""));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(2, true));
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(3, streamSegmentName, attribute, ""));
        order.verify(connection).send(new WireCommands.SegmentAttribute(3, 1));

        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(4, streamSegmentName, attribute, 5, WireCommands.NULL_ATTRIBUTE_VALUE, ""));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(4, false));
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(5, streamSegmentName, attribute, ""));
        order.verify(connection).send(new WireCommands.SegmentAttribute(5, 1));

        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(6, streamSegmentName, attribute, 10, 1, ""));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(6, true));
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(7, streamSegmentName, attribute, ""));
        order.verify(connection).send(new WireCommands.SegmentAttribute(7, 10));

        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(8, streamSegmentName, attribute, WireCommands.NULL_ATTRIBUTE_VALUE, 10, ""));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(8, true));
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(9, streamSegmentName, attribute, ""));
        order.verify(connection).send(new WireCommands.SegmentAttribute(9, WireCommands.NULL_ATTRIBUTE_VALUE));
    }

    @Test(timeout = 20000)
    public void testCreateSealTruncateDelete() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against.
        String streamSegmentName = "testCreateSealDelete";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection);

        // Create a segment and append 2 bytes.
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, ""));
        assertTrue(append(streamSegmentName, 1, store));
        assertTrue(append(streamSegmentName, 2, store));

        processor.sealSegment(new WireCommands.SealSegment(2, streamSegmentName, ""));
        assertFalse(append(streamSegmentName, 2, store));

        // Truncate half.
        final long truncateOffset = store.getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT)
                .join().getLength() / 2;
        AssertExtensions.assertGreaterThan("Nothing to truncate.", 0, truncateOffset);
        processor.truncateSegment(new WireCommands.TruncateSegment(3, streamSegmentName, truncateOffset, ""));
        assertEquals(truncateOffset, store.getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT)
                .join().getStartOffset());

        // Truncate at the same offset - verify idempotence.
        processor.truncateSegment(new WireCommands.TruncateSegment(4, streamSegmentName, truncateOffset, ""));
        assertEquals(truncateOffset, store.getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT)
                .join().getStartOffset());

        // Truncate at a lower offset - verify failure.
        processor.truncateSegment(new WireCommands.TruncateSegment(5, streamSegmentName, truncateOffset - 1, ""));
        assertEquals(truncateOffset, store.getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT)
                .join().getStartOffset());

        // Delete.
        processor.deleteSegment(new WireCommands.DeleteSegment(6, streamSegmentName, ""));
        assertFalse(append(streamSegmentName, 4, store));

        // Verify connection response with same order.
        order.verify(connection).send(new WireCommands.SegmentCreated(1, streamSegmentName));
        order.verify(connection).send(new WireCommands.SegmentSealed(2, streamSegmentName));
        order.verify(connection).send(new WireCommands.SegmentTruncated(3, streamSegmentName));
        order.verify(connection).send(new WireCommands.SegmentTruncated(4, streamSegmentName));
        order.verify(connection).send(new WireCommands.SegmentIsTruncated(5, streamSegmentName, truncateOffset, ""));
        order.verify(connection).send(new WireCommands.SegmentDeleted(6, streamSegmentName));
        order.verifyNoMoreInteractions();
    }

    @Test(timeout = 20000)
    public void testUnsupportedOperation() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        String streamSegmentName = "testCreateSegment";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getReadOnlyBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection);

        // Execute and Verify createSegment/getStreamSegmentInfo calling stack is executed as design.
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, ""));
        order.verify(connection).send(new WireCommands.OperationUnsupported(1, "createSegment", ""));
    }

    //endregion

    //region Table Segments

    @Test(timeout = 20000)
    public void testCreateTableSegment() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        String tableSegmentName = "testCreateTableSegment";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, connection, SegmentStatsRecorder.noOp(),
                recorderMock, new PassingTokenVerifier(), false);

        // Execute and Verify createTableSegment calling stack is executed as design.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        processor.createTableSegment(new WireCommands.CreateTableSegment(2, tableSegmentName, ""));
        order.verify(connection).send(new WireCommands.SegmentAlreadyExists(2, tableSegmentName, ""));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());
        verifyNoMoreInteractions(recorderMock);
    }

    /**
     * Verifies that the methods that are not yet implemented are not implemented by accident without unit tests.
     * This test should be removed once every method tested in it is implemented.
     */
    @Test(timeout = 20000)
    public void testUnimplementedMethods() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        String streamSegmentName = "test";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, connection);

        assertThrows("seal() is implemented.",
                     () -> processor.sealTableSegment(new WireCommands.SealTableSegment(1, streamSegmentName, "")),
                     ex -> ex instanceof UnsupportedOperationException);
        assertThrows("merge() is implemented.",
                     () -> processor.mergeTableSegments(new WireCommands.MergeTableSegments(1, streamSegmentName, streamSegmentName, "")),
                     ex -> ex instanceof UnsupportedOperationException);
    }

    @Test(timeout = 20000)
    public void testUpdateEntries() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        val rnd = new Random(0);
        String tableSegmentName = "testUpdateEntries";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, connection, SegmentStatsRecorder.noOp(),
                recorderMock, new PassingTokenVerifier(), false);

        //Generate keys
        ArrayList<HashedArray> keys = generateKeys(3, rnd);

        // Execute and Verify createSegment calling stack is executed as design.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());

        // Test with unversioned data.
        TableEntry e1 = TableEntry.unversioned(keys.get(0), generateValue(rnd));
        WireCommands.TableEntries cmd = getTableEntries(singletonList(e1));
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(2, tableSegmentName, "", cmd));
        order.verify(connection).send(new WireCommands.TableEntriesUpdated(2, singletonList(0L)));
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(1), eq(false), any());

        // Test with versioned data.
        e1 = TableEntry.versioned(keys.get(0), generateValue(rnd), 0L);
        cmd = getTableEntries(singletonList(e1));
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(3, tableSegmentName, "", cmd));
        order.verify(connection).send(any());
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(1), eq(true), any());

        // Test with key not present. The table store throws KeyNotExistsException.
        TableEntry e2 = TableEntry.versioned(keys.get(1), generateValue(rnd), 0L);
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(4, tableSegmentName, "", getTableEntries(singletonList(e2))));
        order.verify(connection).send(new WireCommands.TableKeyDoesNotExist(4, tableSegmentName, ""));
        verifyNoMoreInteractions(recorderMock);

        // Test with invalid key version. The table store throws BadKeyVersionException.
        TableEntry e3 = TableEntry.versioned(keys.get(0), generateValue(rnd), 10L);
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(5, tableSegmentName, "", getTableEntries(singletonList(e3))));
        order.verify(connection).send(new WireCommands.TableKeyBadVersion(5, tableSegmentName, ""));
        verifyNoMoreInteractions(recorderMock);
    }

    @Test(timeout = 30000)
    public void testRemoveKeys() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        val rnd = new Random(0);
        String tableSegmentName = "testRemoveEntries";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, connection, SegmentStatsRecorder.noOp(),
                recorderMock, new PassingTokenVerifier(), false);

        // Generate keys.
        ArrayList<HashedArray> keys = generateKeys(2, rnd);

        // Create a table segment and add data.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        TableEntry e1 = TableEntry.unversioned(keys.get(0), generateValue(rnd));
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(2, tableSegmentName, "", getTableEntries(singletonList(e1))));
        order.verify(connection).send(new WireCommands.TableEntriesUpdated(2, singletonList(0L)));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(1), eq(false), any());

        // Remove a Table Key
        WireCommands.TableKey key = new WireCommands.TableKey(wrappedBuffer(e1.getKey().getKey().array()), 0L);
        processor.removeTableKeys(new WireCommands.RemoveTableKeys(3, tableSegmentName, "", singletonList(key)));
        order.verify(connection).send(new WireCommands.TableKeysRemoved(3, tableSegmentName));
        verify(recorderMock).removeKeys(eq(tableSegmentName), eq(1), eq(true), any());

        // Test with non-existent key.
        key = new WireCommands.TableKey(wrappedBuffer(e1.getKey().getKey().array()), 0L);
        processor.removeTableKeys(new WireCommands.RemoveTableKeys(4, tableSegmentName, "", singletonList(key)));
        order.verify(connection).send(new WireCommands.TableKeyBadVersion(4, tableSegmentName, ""));
        verifyNoMoreInteractions(recorderMock);
    }

    @Test(timeout = 30000)
    public void testDeleteEmptyTable() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        String tableSegmentName = "testTable1";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, connection, SegmentStatsRecorder.noOp(),
                recorderMock, new PassingTokenVerifier(), false);

        // Create a table segment.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());

        processor.deleteTableSegment(new WireCommands.DeleteTableSegment(2, tableSegmentName, true, ""));
        order.verify(connection).send(new WireCommands.SegmentDeleted(2, tableSegmentName));
        verify(recorderMock).deleteTableSegment(eq(tableSegmentName), any());
    }

    @Test(timeout = 30000)
    public void testDeleteNonEmptyTable() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        val rnd = new Random(0);
        String tableSegmentName = "testTable";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, connection, SegmentStatsRecorder.noOp(),
                recorderMock, new PassingTokenVerifier(), false);

        // Generate keys.
        ArrayList<HashedArray> keys = generateKeys(2, rnd);

        // Create a table segment and add data.
        processor.createTableSegment(new WireCommands.CreateTableSegment(3, tableSegmentName, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(3, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());

        TableEntry e1 = TableEntry.unversioned(keys.get(0), generateValue(rnd));
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(4, tableSegmentName, "", getTableEntries(singletonList(e1))));
        order.verify(connection).send(new WireCommands.TableEntriesUpdated(4, singletonList(0L)));
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(1), eq(false), any());

        // Delete a table segment which has data.
        processor.deleteTableSegment(new WireCommands.DeleteTableSegment(5, tableSegmentName, true, ""));
        order.verify(connection).send(new WireCommands.TableSegmentNotEmpty(5, tableSegmentName, ""));
        verifyNoMoreInteractions(recorderMock);
    }

    @Test(timeout = 30000)
    public void testReadTable() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        val rnd = new Random(0);
        String tableSegmentName = "testReadTable";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        val recorderMockOrder = inOrder(recorderMock);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, connection, SegmentStatsRecorder.noOp(),
                recorderMock, new PassingTokenVerifier(), false);

        // Generate keys.
        ArrayList<HashedArray> keys = generateKeys(2, rnd);

        // Create a table segment and add data.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        recorderMockOrder.verify(recorderMock).createTableSegment(eq(tableSegmentName), any());
        TableEntry entry = TableEntry.unversioned(keys.get(0), generateValue(rnd));

        // Read value of a non-existent key.
        WireCommands.TableKey key = new WireCommands.TableKey(wrappedBuffer(entry.getKey().getKey().array()), TableKey.NO_VERSION);
        processor.readTable(new WireCommands.ReadTable(2, tableSegmentName, "", singletonList(key)));

        // expected result is Key with an empty TableValue.
        order.verify(connection).send(new WireCommands.TableRead(2, tableSegmentName,
                                                                 new WireCommands.TableEntries(
                                                                         singletonList(new AbstractMap.SimpleImmutableEntry<>(key, WireCommands.TableValue.EMPTY)))));
        recorderMockOrder.verify(recorderMock).getKeys(eq(tableSegmentName), eq(1), any());

        // Update a value to a key.
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(3, tableSegmentName, "", getTableEntries(singletonList(entry))));
        order.verify(connection).send(new WireCommands.TableEntriesUpdated(3, singletonList(0L)));
        recorderMockOrder.verify(recorderMock).updateEntries(eq(tableSegmentName), eq(1), eq(false), any());

        // Read the value of the key.
        key = new WireCommands.TableKey(wrappedBuffer(entry.getKey().getKey().array()), 0L);
        TableEntry expectedEntry = TableEntry.versioned(entry.getKey().getKey(), entry.getValue(), 0L);
        processor.readTable(new WireCommands.ReadTable(4, tableSegmentName, "", singletonList(key)));
        order.verify(connection).send(new WireCommands.TableRead(4, tableSegmentName,
                                                                 getTableEntries(singletonList(expectedEntry))));
        recorderMockOrder.verify(recorderMock).getKeys(eq(tableSegmentName), eq(1), any());
    }

    @Test
    public void testGetTableKeys() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        val rnd = new Random(0);
        String tableSegmentName = "testGetTableKeys";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, connection, SegmentStatsRecorder.noOp(),
                recorderMock, new PassingTokenVerifier(), false);

        // Generate keys.
        ArrayList<HashedArray> keys = generateKeys(3, rnd);
        TableEntry e1 = TableEntry.unversioned(keys.get(0), generateValue(rnd));
        TableEntry e2 = TableEntry.unversioned(keys.get(1), generateValue(rnd));
        TableEntry e3 = TableEntry.unversioned(keys.get(2), generateValue(rnd));

        // Create a table segment and add data.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(2, tableSegmentName, "", getTableEntries(asList(e1, e2, e3))));
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(3), eq(false), any());

        // 1. Now read the table keys where suggestedKeyCount is equal to number of entries in the Table Store.
        processor.readTableKeys(new WireCommands.ReadTableKeys(3, tableSegmentName, "", 3, wrappedBuffer(new byte[0])));

        // Capture the WireCommands sent.
        ArgumentCaptor<WireCommand> wireCommandsCaptor = ArgumentCaptor.forClass(WireCommand.class);
        order.verify(connection, times(2)).send(wireCommandsCaptor.capture());
        verify(recorderMock).iterateKeys(eq(tableSegmentName), eq(3), any());

        // Verify the WireCommands.
        List<Long> keyVersions = ((WireCommands.TableEntriesUpdated) wireCommandsCaptor.getAllValues().get(0)).getUpdatedVersions();
        WireCommands.TableKeysRead getTableKeysReadResponse = (WireCommands.TableKeysRead) wireCommandsCaptor.getAllValues().get(1);
        assertTrue(getTableKeysReadResponse.getKeys().stream().map(WireCommands.TableKey::getKeyVersion).collect(Collectors.toList()).containsAll(keyVersions));

        // 2. Now read the table keys where suggestedKeyCount is less than the number of keys in the Table Store.
        processor.readTableKeys(new WireCommands.ReadTableKeys(3, tableSegmentName, "", 1, wrappedBuffer(new byte[0])));

        // Capture the WireCommands sent.
        ArgumentCaptor<WireCommands.TableKeysRead> tableKeysCaptor = ArgumentCaptor.forClass(WireCommands.TableKeysRead.class);
        order.verify(connection, times(1)).send(tableKeysCaptor.capture());
        verify(recorderMock).iterateKeys(eq(tableSegmentName), eq(1), any());

        // Verify the WireCommands.
        getTableKeysReadResponse =  tableKeysCaptor.getAllValues().get(0);
        assertEquals(1, getTableKeysReadResponse.getKeys().size());
        assertTrue(keyVersions.contains(getTableKeysReadResponse.getKeys().get(0).getKeyVersion()));
        // Get the last state.
        ByteBuf state = getTableKeysReadResponse.getContinuationToken();

        // 3. Now read the remaining table keys by providing a higher suggestedKeyCount and the state to the iterator.
        processor.readTableKeys(new WireCommands.ReadTableKeys(3, tableSegmentName, "", 3, state));
        // Capture the WireCommands sent.
        tableKeysCaptor = ArgumentCaptor.forClass(WireCommands.TableKeysRead.class);
        order.verify(connection, times(1)).send(tableKeysCaptor.capture());
        verify(recorderMock).iterateKeys(eq(tableSegmentName), eq(1), any());

        // Verify the WireCommands.
        getTableKeysReadResponse =  tableKeysCaptor.getAllValues().get(0);
        assertEquals(2, getTableKeysReadResponse.getKeys().size());
        assertTrue(keyVersions.containsAll(getTableKeysReadResponse.getKeys().stream().map(WireCommands.TableKey::getKeyVersion).collect(Collectors.toList())));
    }

    @Test
    public void testGetTableEntries() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        val rnd = new Random(0);
        String tableSegmentName = "testGetTableEntries";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, connection, SegmentStatsRecorder.noOp(),
                recorderMock, new PassingTokenVerifier(), false);

        // Generate keys.
        ArrayList<HashedArray> keys = generateKeys(3, rnd);
        HashedArray testValue = generateValue(rnd);
        TableEntry e1 = TableEntry.unversioned(keys.get(0), testValue);
        TableEntry e2 = TableEntry.unversioned(keys.get(1), testValue);
        TableEntry e3 = TableEntry.unversioned(keys.get(2), testValue);

        // Create a table segment and add data.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, ""));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(2, tableSegmentName, "", getTableEntries(asList(e1, e2, e3))));
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(3), eq(false), any());

        // 1. Now read the table entries where suggestedEntryCount is equal to number of entries in the Table Store.
        processor.readTableEntries(new WireCommands.ReadTableEntries(3, tableSegmentName, "", 3, wrappedBuffer(new byte[0])));

        // Capture the WireCommands sent.
        ArgumentCaptor<WireCommand> wireCommandsCaptor = ArgumentCaptor.forClass(WireCommand.class);
        order.verify(connection, times(2)).send(wireCommandsCaptor.capture());
        verify(recorderMock).iterateEntries(eq(tableSegmentName), eq(3), any());

        // Verify the WireCommands.
        List<Long> keyVersions = ((WireCommands.TableEntriesUpdated) wireCommandsCaptor.getAllValues().get(0)).getUpdatedVersions();
        WireCommands.TableEntriesRead getTableEntriesIteratorsResp =
                (WireCommands.TableEntriesRead) wireCommandsCaptor.getAllValues().get(1);
        assertTrue(getTableEntriesIteratorsResp.getEntries().getEntries().stream().map(e -> e.getKey().getKeyVersion()).collect(Collectors.toList()).containsAll(keyVersions));
        // Verify if the value is correct.
        assertTrue(getTableEntriesIteratorsResp.getEntries().getEntries().stream().allMatch(e -> {
            ByteBuf buf = e.getValue().getData();
            byte[] bytes = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), bytes);
            return testValue.equals(new HashedArray(bytes));
        }));

        // 2. Now read the table keys where suggestedEntryCount is less than the number of entries in the Table Store.
        processor.readTableEntries(new WireCommands.ReadTableEntries(3, tableSegmentName, "", 1, wrappedBuffer(new byte[0])));

        // Capture the WireCommands sent.
        ArgumentCaptor<WireCommands.TableEntriesRead> tableEntriesCaptor =
                ArgumentCaptor.forClass(WireCommands.TableEntriesRead.class);
        order.verify(connection, times(1)).send(tableEntriesCaptor.capture());

        // Verify the WireCommands.
        getTableEntriesIteratorsResp =  tableEntriesCaptor.getAllValues().get(0);
        assertEquals(1, getTableEntriesIteratorsResp.getEntries().getEntries().size());
        assertTrue(keyVersions.contains(getTableEntriesIteratorsResp.getEntries().getEntries().get(0).getKey().getKeyVersion()));
        // Get the last state.
        ByteBuf state = getTableEntriesIteratorsResp.getContinuationToken();

        // 3. Now read the remaining table entries by providing a higher suggestedKeyCount and the state to the iterator.
        processor.readTableEntries(new WireCommands.ReadTableEntries(3, tableSegmentName, "", 3, state));
        // Capture the WireCommands sent.
        tableEntriesCaptor = ArgumentCaptor.forClass(WireCommands.TableEntriesRead.class);
        order.verify(connection, times(1)).send(tableEntriesCaptor.capture());
        verify(recorderMock).iterateEntries(eq(tableSegmentName), eq(1), any());

        // Verify the WireCommands.
        getTableEntriesIteratorsResp =  tableEntriesCaptor.getAllValues().get(0);
        assertEquals(2, getTableEntriesIteratorsResp.getEntries().getEntries().size());
        assertTrue(keyVersions.containsAll(getTableEntriesIteratorsResp.getEntries().getEntries().stream().map(e -> e.getKey().getKeyVersion()).collect(Collectors.toList())));
    }

    private HashedArray generateData(int length, Random rnd) {
        byte[] keyData = new byte[length];
        rnd.nextBytes(keyData);
        return new HashedArray(keyData);
    }

    private WireCommands.TableEntries getTableEntries(List<TableEntry> updateData) {

        List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> entries = updateData.stream().map(te -> {
            if (te == null) {
                return new AbstractMap.SimpleImmutableEntry<>(WireCommands.TableKey.EMPTY, WireCommands.TableValue.EMPTY);
            } else {
                val tableKey = new WireCommands.TableKey(wrappedBuffer(te.getKey().getKey().array()), te.getKey().getVersion());
                val tableValue = new WireCommands.TableValue(wrappedBuffer(te.getValue().array()));
                return new AbstractMap.SimpleImmutableEntry<>(tableKey, tableValue);
            }
        }).collect(toList());

        return new WireCommands.TableEntries(entries);
    }

    private HashedArray generateValue(Random rnd) {
        return generateData(MAX_VALUE_LENGTH, rnd);
    }

    private ArrayList<HashedArray> generateKeys(int keyCount, Random rnd) {
        val result = new ArrayList<HashedArray>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            result.add(generateData(MAX_KEY_LENGTH, rnd));
        }

        return result;
    }

    //endregion

    //region Other Helpers

    private boolean append(String streamSegmentName, int number, StreamSegmentStore store) {
        return Futures.await(store.append(streamSegmentName,
                new byte[]{(byte) number},
                null,
                PravegaRequestProcessor.TIMEOUT));
    }

    private static ServiceBuilderConfig getBuilderConfig() {
        return ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.THREAD_POOL_SIZE, 3)
                        .with(ServiceConfig.LISTENING_PORT, TestUtils.getAvailableListenPort()))
                .build();
    }

    private static ServiceBuilderConfig getReadOnlyBuilderConfig() {
        val baseConfig = getBuilderConfig();
        val props = new Properties();
        baseConfig.forEach(props::put);
        return ServiceBuilderConfig.builder()
                .include(props)
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.READONLY_SEGMENT_STORE, true))
                .build();
    }

    private static ServiceBuilder newInlineExecutionInMemoryBuilder(ServiceBuilderConfig config) {
        return ServiceBuilder.newInMemoryBuilder(config, (size, name) -> new InlineExecutor())
                             .withStreamSegmentStore(setup -> new SynchronousStreamSegmentStore(new StreamSegmentService(
                                     setup.getContainerRegistry(), setup.getSegmentToContainerMapper())));
    }

    //endregion
}
