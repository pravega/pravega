/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.service.host.handler;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.server.segmentstore.contracts.ReadResult;
import io.pravega.server.segmentstore.contracts.ReadResultEntry;
import io.pravega.server.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.server.segmentstore.contracts.ReadResultEntryType;
import io.pravega.server.segmentstore.contracts.StreamSegmentStore;
import io.pravega.server.segmentstore.service.mocks.SynchronousStreamSegmentStore;
import io.pravega.server.segmentstore.service.reading.ReadResultEntryBase;
import io.pravega.server.segmentstore.service.store.ServiceBuilder;
import io.pravega.server.segmentstore.service.store.ServiceBuilderConfig;
import io.pravega.server.segmentstore.service.store.ServiceConfig;
import io.pravega.server.segmentstore.service.store.StreamSegmentService;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsData;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.TestUtils;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
    public void testCreateSealDelete() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against.
        String streamSegmentName = "testCreateSealDelete";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, connection);

        // Execute create/seal/delete Segment command.
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0));
        assertTrue(append(streamSegmentName, 1, store));
        processor.sealSegment(new WireCommands.SealSegment(2, streamSegmentName));
        assertFalse(append(streamSegmentName, 2, store));
        processor.deleteSegment(new WireCommands.DeleteSegment(3, streamSegmentName));
        assertFalse(append(streamSegmentName, 3, store));

        // Verify connection response with same order.
        order.verify(connection).send(new WireCommands.SegmentCreated(1, streamSegmentName));
        order.verify(connection).send(new WireCommands.SegmentSealed(2, streamSegmentName));
        order.verify(connection).send(new WireCommands.SegmentDeleted(3, streamSegmentName));
    }

    private boolean append(String streamSegmentName, int number, StreamSegmentStore store) {
        return FutureHelpers.await(store.append(streamSegmentName,
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
