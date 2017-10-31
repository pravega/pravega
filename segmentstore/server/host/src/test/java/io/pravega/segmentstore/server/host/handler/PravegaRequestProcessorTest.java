/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.mocks.SynchronousStreamSegmentStore;
import io.pravega.segmentstore.server.reading.ReadResultEntryBase;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.server.store.StreamSegmentService;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsData;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.TransactionInfo;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.TestUtils;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@Slf4j
public class PravegaRequestProcessorTest {
    static {
        MetricsProvider.initialize(MetricsConfig.builder().with(MetricsConfig.ENABLE_STATISTICS, true).build());
    }

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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, connection);

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
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength));
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, connection);

        TestReadResultEntry entry1 = new TestReadResultEntry(ReadResultEntryType.EndOfStreamSegment, 0, readLength);

        List<ReadResultEntry> results = new ArrayList<>();
        results.add(entry1);
        CompletableFuture<ReadResult> readResult = new CompletableFuture<>();
        readResult.complete(new TestReadResult(0, readLength, results));
        when(store.read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult);

        // Execute and Verify readSegment calling stack in connection and store is executed as design.
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        verify(connection).send(new WireCommands.SegmentRead(streamSegmentName, 0, false, true, ByteBuffer.wrap(new byte[0])));
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, connection);

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
        when(store.getStreamSegmentInfo(streamSegmentName, false, PravegaRequestProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(info));

        // Execute and Verify readSegment calling stack in connection and store is executed as design.
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        verify(store).getStreamSegmentInfo(streamSegmentName, false, PravegaRequestProcessor.TIMEOUT);
        verify(connection).send(new WireCommands.SegmentIsTruncated(0, streamSegmentName, info.getStartOffset()));
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, connection);

        // Execute and Verify createSegment/getStreamSegmentInfo calling stack is executed as design.
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0));
        assertTrue(append(streamSegmentName, 1, store));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(1, streamSegmentName));
        assertTrue(append(streamSegmentName, 2, store));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, streamSegmentName));
        order.verify(connection).send(Mockito.any(WireCommands.StreamSegmentInfo.class));

        // TestCreateSealDelete may executed before this test case,
        // so createSegmentStats may record 1 or 2 createSegment operation here.
        OpStatsData createSegmentStats = PravegaRequestProcessor.CREATE_STREAM_SEGMENT.toOpStatsData();
        assertNotEquals(0, createSegmentStats.getNumSuccessfulEvents());
        assertEquals(0, createSegmentStats.getNumFailedEvents());
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, connection);

        processor.createSegment(new WireCommands.CreateSegment(0, streamSegmentName,
                                                               WireCommands.CreateSegment.NO_SCALE, 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(0, streamSegmentName));

        processor.createTransaction(new WireCommands.CreateTransaction(1, streamSegmentName, txnid));
        assertTrue(append(StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), 1, store));
        processor.getTransactionInfo(new WireCommands.GetTransactionInfo(2, streamSegmentName, txnid));
        assertTrue(append(StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), 2, store));
        order.verify(connection).send(new WireCommands.TransactionCreated(1, streamSegmentName, txnid));
        order.verify(connection).send(Mockito.argThat(t -> {
            return t instanceof TransactionInfo && ((TransactionInfo) t).exists();
        }));
        processor.commitTransaction(new WireCommands.CommitTransaction(3, streamSegmentName, txnid));
        order.verify(connection).send(new WireCommands.TransactionCommitted(3, streamSegmentName, txnid));
        processor.getTransactionInfo(new WireCommands.GetTransactionInfo(4, streamSegmentName, txnid));
        order.verify(connection)
             .send(new WireCommands.NoSuchSegment(4, StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName,
                                                                                                     txnid)));

        txnid = UUID.randomUUID();
        processor.createTransaction(new WireCommands.CreateTransaction(1, streamSegmentName, txnid));
        assertTrue(append(StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), 1, store));
        order.verify(connection).send(new WireCommands.TransactionCreated(1, streamSegmentName, txnid));
        processor.getTransactionInfo(new WireCommands.GetTransactionInfo(2, streamSegmentName, txnid));
        order.verify(connection).send(Mockito.argThat(t -> {
            return t instanceof TransactionInfo && ((TransactionInfo) t).exists();
        }));
        processor.abortTransaction(new WireCommands.AbortTransaction(3, streamSegmentName, txnid));
        order.verify(connection).send(new WireCommands.TransactionAborted(3, streamSegmentName, txnid));
        processor.getTransactionInfo(new WireCommands.GetTransactionInfo(4, streamSegmentName, txnid));
        order.verify(connection)
             .send(new WireCommands.NoSuchSegment(4, StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName,
                                                                                                     txnid)));

        // Verify the case when the transaction segment is already sealed. This simulates the case when the process
        // crashed after sealing, but before issuing the merge.
        txnid = UUID.randomUUID();
        processor.createTransaction(new WireCommands.CreateTransaction(1, streamSegmentName, txnid));
        assertTrue(append(StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), 1, store));
        processor.getTransactionInfo(new WireCommands.GetTransactionInfo(2, streamSegmentName, txnid));
        assertTrue(append(StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid), 2, store));

        // Seal the transaction in the SegmentStore.
        String txnName = StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, txnid);
        store.sealStreamSegment(txnName, Duration.ZERO).join();

        processor.commitTransaction(new WireCommands.CommitTransaction(3, streamSegmentName, txnid));
        order.verify(connection).send(new WireCommands.TransactionCommitted(3, streamSegmentName, txnid));
        processor.getTransactionInfo(new WireCommands.GetTransactionInfo(4, streamSegmentName, txnid));
        order.verify(connection)
                .send(new WireCommands.NoSuchSegment(4, StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName,
                        txnid)));

        order.verifyNoMoreInteractions();
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, connection);

        // Execute and Verify createSegment/getStreamSegmentInfo calling stack is executed as design.
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, streamSegmentName));
        
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(2, streamSegmentName, attribute));
        order.verify(connection).send(new WireCommands.SegmentAttribute(2, WireCommands.NULL_ATTRIBUTE_VALUE));
        
        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(2, streamSegmentName, attribute, 1, WireCommands.NULL_ATTRIBUTE_VALUE));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(2, true));
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(3, streamSegmentName, attribute));
        order.verify(connection).send(new WireCommands.SegmentAttribute(3, 1));
        
        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(4, streamSegmentName, attribute, 5, WireCommands.NULL_ATTRIBUTE_VALUE));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(4, false));
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(5, streamSegmentName, attribute));
        order.verify(connection).send(new WireCommands.SegmentAttribute(5, 1));
        
        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(6, streamSegmentName, attribute, 10, 1));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(6, true));
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(7, streamSegmentName, attribute));
        order.verify(connection).send(new WireCommands.SegmentAttribute(7, 10));
        
        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(8, streamSegmentName, attribute, WireCommands.NULL_ATTRIBUTE_VALUE, 10));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(8, true));
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(9, streamSegmentName, attribute));
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, connection);

        // Create a segment and append 2 bytes.
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0));
        assertTrue(append(streamSegmentName, 1, store));
        assertTrue(append(streamSegmentName, 2, store));

        processor.sealSegment(new WireCommands.SealSegment(2, streamSegmentName));
        assertFalse(append(streamSegmentName, 2, store));

        // Truncate half.
        final long truncateOffset = store.getStreamSegmentInfo(streamSegmentName, false, PravegaRequestProcessor.TIMEOUT)
                .join().getLength() / 2;
        AssertExtensions.assertGreaterThan("Nothing to truncate.", 0, truncateOffset);
        processor.truncateSegment(new WireCommands.TruncateSegment(3, streamSegmentName, truncateOffset));
        assertEquals(truncateOffset, store.getStreamSegmentInfo(streamSegmentName, false, PravegaRequestProcessor.TIMEOUT)
                .join().getStartOffset());

        // Delete.
        processor.deleteSegment(new WireCommands.DeleteSegment(4, streamSegmentName));
        assertFalse(append(streamSegmentName, 4, store));

        // Verify connection response with same order.
        order.verify(connection).send(new WireCommands.SegmentCreated(1, streamSegmentName));
        order.verify(connection).send(new WireCommands.SegmentSealed(2, streamSegmentName));
        order.verify(connection).send(new WireCommands.SegmentTruncated(3, streamSegmentName));
        order.verify(connection).send(new WireCommands.SegmentDeleted(4, streamSegmentName));
    }

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

    private static ServiceBuilder newInlineExecutionInMemoryBuilder(ServiceBuilderConfig config) {
        return ServiceBuilder.newInMemoryBuilder(config, new InlineExecutor())
                             .withStreamSegmentStore(setup -> new SynchronousStreamSegmentStore(new StreamSegmentService(
                                     setup.getContainerRegistry(), setup.getSegmentToContainerMapper())));
    }
}
