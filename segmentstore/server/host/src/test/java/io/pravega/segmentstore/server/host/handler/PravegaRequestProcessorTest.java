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
package io.pravega.segmentstore.server.host.handler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.pravega.auth.TokenExpiredException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.segmentstore.server.mocks.SynchronousStreamSegmentStore;
import io.pravega.segmentstore.server.reading.ReadResultEntryBase;
import io.pravega.segmentstore.server.reading.StreamSegmentReadResult;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.server.store.StreamSegmentService;
import io.pravega.segmentstore.server.tables.TableExtensionConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.shared.NameUtils;
import io.pravega.shared.metrics.MetricNotifier;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.TestUtils;
import java.nio.charset.Charset;
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
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.NameUtils.getIndexSegmentName;
import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@Slf4j
@RunWith(SerializedClassRunner.class)
public class PravegaRequestProcessorTest {

    private static final int MAX_KEY_LENGTH = 100;
    private static final int MAX_VALUE_LENGTH = 100;
    private final long requestId = 1L;

    @BeforeClass
    public static void setupClass() {
        MetricsProvider.initialize(MetricsConfig.builder().with(MetricsConfig.ENABLE_STATISTICS, true).build());
        MetricsProvider.getMetricsProvider().startWithoutExporting();
    }

    //region Stream Segments

    @Getter
    @Setter
    private static class TestReadResult extends StreamSegmentReadResult implements ReadResult {
        final List<ReadResultEntry> results;
        long currentOffset = 0;

        TestReadResult(long streamSegmentStartOffset, int maxResultLength, List<ReadResultEntry> results) {
            super(streamSegmentStartOffset, maxResultLength, (l, r, i) -> null, "");
            this.results = results;
        }

        @Override
        public synchronized boolean hasNext() {
            return !results.isEmpty();
        }

        @Override
        public synchronized ReadResultEntry next() {
            Assert.assertTrue("Expected copy-on-read enabled for all segment reads.", isCopyOnRead());
            ReadResultEntry result = results.remove(0);
            currentOffset = result.getStreamSegmentOffset();
            return result;
        }

        @Override
        public synchronized int getConsumedLength() {
            return (int) (currentOffset - getStreamSegmentStartOffset());
        }
    }

    private static class TestReadResultEntry extends ReadResultEntryBase {
        TestReadResultEntry(ReadResultEntryType type, long streamSegmentOffset, int requestedReadLength) {
            super(type, streamSegmentOffset, requestedReadLength);
        }

        @Override
        protected void complete(BufferView readResultEntryContents) {
            super.complete(readResultEntryContents);
        }

        @Override
        public void fail(Throwable exception) {
            super.fail(exception);
        }

        @Override
        public void requestContent(Duration timeout) {
            Preconditions.checkState(getType() != ReadResultEntryType.EndOfStreamSegment, "EndOfStreamSegmentReadResultEntry does not have any content.");
        }
    }

    @Test(timeout = 20000)
    public void testReadSegment() throws DurableDataLogException {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "scope/stream/testReadSegment";
        byte[] data = new byte[]{1, 2, 3, 4, 6, 7, 8, 9};
        int readLength = 1000;

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        TestReadResultEntry entry1 = new TestReadResultEntry(ReadResultEntryType.Cache, 0, readLength);
        entry1.complete(new ByteArraySegment(data));
        TestReadResultEntry entry2 = new TestReadResultEntry(ReadResultEntryType.Future, data.length, readLength);

        List<ReadResultEntry> results = new ArrayList<>();
        results.add(entry1);
        results.add(entry2);
        CompletableFuture<ReadResult> readResult = new CompletableFuture<>();
        readResult.complete(new TestReadResult(0, readLength, results));
        when(store.read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult);

        // Execute and Verify readSegment calling stack in connection and store is executed as design.
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength, "", requestId));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        verify(connection).send(new WireCommands.SegmentRead(streamSegmentName, 0, true, false, Unpooled.wrappedBuffer(data), requestId));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
        entry2.complete(new ByteArraySegment(data));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
        
        TestReadResultEntry entry3 = new TestReadResultEntry(ReadResultEntryType.Future, 2, readLength);

        List<ReadResultEntry> results2 = new ArrayList<>();
        results2.add(entry3);
        CompletableFuture<ReadResult> readResult2 = new CompletableFuture<>();
        readResult2.complete(new TestReadResult(2, readLength, results2));
        when(store.read(streamSegmentName, 2, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult2);

        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
        entry3.complete(new ByteArraySegment(data));
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 2, readLength, "", requestId));
        verify(store).read(streamSegmentName, 2, readLength, PravegaRequestProcessor.TIMEOUT);
        verify(connection).send(new WireCommands.SegmentRead(streamSegmentName, 2, true, false, Unpooled.wrappedBuffer(data), requestId));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 20000)
    public void testLocateOffsetThrowingSegmentTruncatedException() throws DurableDataLogException {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "scope/stream/testLocateOffset";
        String indexSegment = getIndexSegmentName(streamSegmentName);
        byte[] data = new byte[]{1, 2, 3, 4, 6, 7, 8, 9};
        int readLength = 24;

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        StreamSegmentInformation info = StreamSegmentInformation.builder()
                .name(indexSegment)
                .length(96)
                .startOffset(0)
                .build();
        when(store.getStreamSegmentInfo(indexSegment, PravegaRequestProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(info));

        TestReadResultEntry entry2 = new TestReadResultEntry(ReadResultEntryType.Truncated, 0, readLength);

        List<ReadResultEntry> results2 = new ArrayList<>();
        results2.add(entry2);
        CompletableFuture<ReadResult> readResult2 = new CompletableFuture<>();
        readResult2.complete(new TestReadResult(0, readLength, results2));
        when(store.read(eq(indexSegment), anyLong(), anyInt(), any())).thenReturn(readResult2);
        processor.locateOffset(new WireCommands.LocateOffset(requestId, streamSegmentName, 20L, ""));
        order.verify(connection).send(new WireCommands.SegmentTruncated(requestId, streamSegmentName));
    }

    @Test(timeout = 20000)
    public void testLocateOffsetThrowingIllegalStateException() throws DurableDataLogException {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "scope/stream/testLocateOffset";
        String indexSegment = getIndexSegmentName(streamSegmentName);
        byte[] data = new byte[]{1, 2, 3, 4, 6, 7, 8, 9};
        int readLength = 24;

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        TestReadResultEntry entry = new TestReadResultEntry(ReadResultEntryType.EndOfStreamSegment, data.length, readLength);

        List<ReadResultEntry> results = new ArrayList<>();
        results.add(entry);
        CompletableFuture<ReadResult> readResult = new CompletableFuture<>();
        readResult.complete(new TestReadResult(0, readLength, results));
        when(store.read(eq(indexSegment), anyLong(), anyInt(), any())).thenReturn(readResult);
        StreamSegmentInformation info = StreamSegmentInformation.builder()
                .name(indexSegment)
                .length(96)
                .startOffset(0)
                .build();
        when(store.getStreamSegmentInfo(indexSegment, PravegaRequestProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(info));

        processor.locateOffset(new WireCommands.LocateOffset(requestId, streamSegmentName, 20L, ""));
        verify(connection).close();

        TestReadResultEntry entry1 = new TestReadResultEntry(ReadResultEntryType.EndOfStreamSegment, 0, readLength);

        List<ReadResultEntry> results1 = new ArrayList<>();
        results1.add(entry1);
        CompletableFuture<ReadResult> readResult1 = new CompletableFuture<>();
        readResult1.complete(new TestReadResult(0, readLength, results1));
        when(store.read(indexSegment, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult1);
        processor.locateOffset(new WireCommands.LocateOffset(requestId, streamSegmentName, 20L, ""));
        verify(connection, times(2)).close();
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 20000)
    public void testLocateOffsetThrowingStreamSegmentNotExistException() throws DurableDataLogException {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "scope/stream/testLocateOffset";
        String indexSegment = getIndexSegmentName(streamSegmentName);

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        StreamSegmentInformation info = StreamSegmentInformation.builder()
                .name(streamSegmentName)
                .length(96)
                .startOffset(0)
                .build();
        CompletableFuture<SegmentProperties> future = new CompletableFuture<>();
        future.completeExceptionally(new StreamSegmentNotExistsException("Segment does not exits"));

        Mockito.when(store.getStreamSegmentInfo(anyString(), any()))
                .thenReturn(future)
                .thenReturn(CompletableFuture.completedFuture(info));

        processor.locateOffset(new WireCommands.LocateOffset(requestId, streamSegmentName, 20L, ""));

        verify(connection).send(new WireCommands.OffsetLocated(requestId, streamSegmentName, 96));

        CompletableFuture<SegmentProperties> future2 = new CompletableFuture<>();
        future2.completeExceptionally(new StreamSegmentNotExistsException("Segment does not exits"));

        Mockito.when(store.getStreamSegmentInfo(anyString(), any()))
                .thenReturn(future2);
        processor.locateOffset(new WireCommands.LocateOffset(requestId, streamSegmentName, 20L, ""));
        verify(store, times(2)).getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT);
        verify(store, times(2)).getStreamSegmentInfo(indexSegment, PravegaRequestProcessor.TIMEOUT);
        verify(connection).send(new WireCommands.NoSuchSegment(requestId, streamSegmentName, "", -1L));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 20000)
    public void locateOffsetTest() throws DurableDataLogException {
        String streamSegmentName = "scope/stream/testLocateOffset";
        String indexSegment = getIndexSegmentName(streamSegmentName);

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        StreamSegmentInformation info = StreamSegmentInformation.builder()
                                                                .name(streamSegmentName)
                                                                .length(24)
                                                                .startOffset(0)
                                                                .build();
        Mockito.when(store.getStreamSegmentInfo(anyString(), any()))
               .thenReturn(CompletableFuture.completedFuture(info));

        ReadResultEntry readResultEntry = mock(ReadResultEntry.class);
        doReturn(ReadResultEntryType.Cache).when(readResultEntry).getType();
        doNothing().when(readResultEntry).requestContent(any());

        ReadResult result = mock(ReadResult.class);
        doReturn(true).when(result).hasNext();
        doReturn(readResultEntry).when(result).next();

        doReturn(CompletableFuture.completedFuture(new IndexEntry(20, 1, 1233333).toBytes()))
                .when(readResultEntry).getContent();
        when(store.read(eq(indexSegment), anyLong(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(result));

        processor.locateOffset(new WireCommands.LocateOffset(requestId, streamSegmentName, 20L, ""));

        verify(connection).send(new WireCommands.OffsetLocated(requestId, streamSegmentName, 20L));

        verify(store).getStreamSegmentInfo(eq(indexSegment), any());
        verify(store).read(eq(indexSegment), anyLong(), anyInt(), any());
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 20000)
    public void testReadSegmentEmptySealed() throws DurableDataLogException {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "scope/stream/testReadSegment";
        int readLength = 1000;

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        TestReadResultEntry entry1 = new TestReadResultEntry(ReadResultEntryType.EndOfStreamSegment, 0, readLength);

        List<ReadResultEntry> results = new ArrayList<>();
        results.add(entry1);
        CompletableFuture<ReadResult> readResult = new CompletableFuture<>();
        readResult.complete(new TestReadResult(0, readLength, results));
        when(store.read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult);

        // Execute and Verify readSegment calling stack in connection and store is executed as design.
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength, "", requestId));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        verify(connection).send(new WireCommands.SegmentRead(streamSegmentName, 0, false, true, Unpooled.EMPTY_BUFFER, requestId));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 20000)
    public void testReadSegmentWithCancellationException() throws DurableDataLogException {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "scope/stream/testReadSegment";
        int readLength = 1000;

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        //Use low priority executor
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        CompletableFuture<ReadResult> readResult = new CompletableFuture<>();
        readResult.completeExceptionally(new CancellationException("cancel read"));
        // Simulate a CancellationException for a Read Segment.
        when(store.read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult);

        // Execute and Verify readSegment is calling stack in connection and store is executed as design.
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength, "", requestId));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        // Since the underlying store cancels the read request verify if an empty SegmentRead Wirecommand is sent as a response.
        verify(connection).send(new WireCommands.SegmentRead(streamSegmentName, 0, true, false, Unpooled.EMPTY_BUFFER, requestId));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }


    @Test(timeout = 20000)
    public void testReadSegmentTruncated() throws DurableDataLogException {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "scope/stream/testReadSegment";
        int readLength = 1000;

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();

        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

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
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength, "", requestId));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        verify(store).getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT);
        verify(connection).send(new WireCommands.SegmentIsTruncated(requestId, streamSegmentName, info.getStartOffset(), "", 0));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 20000)
    public void testReadFutureTruncated() throws DurableDataLogException {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "scope/stream/testReadSegment";
        int readLength = 1000;

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        TestReadResultEntry entry1 = new TestReadResultEntry(ReadResultEntryType.Future, 0, readLength);

        List<ReadResultEntry> results = new ArrayList<>();
        results.add(entry1);
        CompletableFuture<ReadResult> readResult = CompletableFuture.completedFuture(new TestReadResult(0, readLength, results));
        when(store.read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult);

        // Execute and Verify readSegment calling stack in connection and store is executed as design.
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength, "", requestId));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        
        entry1.fail(new StreamSegmentTruncatedException(100)); 
        verify(connection).send(new WireCommands.SegmentIsTruncated(requestId, streamSegmentName, 100, "", 0));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }    
    
    @Test(timeout = 20000)
    public void testReadFutureError() throws DurableDataLogException {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "scope/stream/testReadSegment";
        int readLength = 1000;

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        TestReadResultEntry entry1 = new TestReadResultEntry(ReadResultEntryType.Future, 0, readLength);

        List<ReadResultEntry> results = new ArrayList<>();
        results.add(entry1);
        CompletableFuture<ReadResult> readResult = CompletableFuture.completedFuture(new TestReadResult(0, readLength, results));
        when(store.read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult);

        // Execute and Verify readSegment calling stack in connection and store is executed as design.
        processor.readSegment(new WireCommands.ReadSegment(streamSegmentName, 0, readLength, "", requestId));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        
        entry1.fail(new RuntimeException());
        verify(connection, Mockito.atLeastOnce()).close();
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }    
    
    @Test(timeout = 20000)
    public void testCreateSegment() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        String streamSegmentName = "scope/stream/testCreateSegment";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(SegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), new TrackedConnection(connection),
                recorderMock, TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // Execute and Verify createSegment/getStreamSegmentInfo calling stack is executed as design.
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        verify(recorderMock).createSegment(eq(streamSegmentName), eq(WireCommands.CreateSegment.NO_SCALE), eq(0), any());
        assertTrue(append(streamSegmentName, 1, store));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(1, streamSegmentName, ""));
        assertTrue(append(streamSegmentName, 2, store));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, streamSegmentName));
        order.verify(connection).send(Mockito.any(WireCommands.StreamSegmentInfo.class));

        // Verify the correct type of Segment is created and that it has the correct roles.
        val si = store.getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT).join();
        val segmentType = SegmentType.fromAttributes(si.getAttributes());
        Assert.assertFalse(segmentType.isInternal() || segmentType.isCritical() || segmentType.isSystem() || segmentType.isTableSegment());

        // Verify the correct rollover size is passed down to the metadata store
        // Verify default value
        val attributes = si.getAttributes();
        Assert.assertEquals((long) attributes.get(Attributes.ROLLOVER_SIZE), 0L);
        // Verify custom value
        String streamSegmentName1 = "scope/stream/testCreateSegmentRolloverSizePositive";
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName1, WireCommands.CreateSegment.NO_SCALE, 0, "", 1024 * 1024L));
        val si1 = store.getStreamSegmentInfo(streamSegmentName1, PravegaRequestProcessor.TIMEOUT).join();
        val attributes1 = si1.getAttributes();
        Assert.assertEquals((long) attributes1.get(Attributes.ROLLOVER_SIZE), 1024 * 1024L);
        // Verify invalid negative value
        String streamSegmentName2 = "scope/stream/testCreateSegmentRolloverSizeNegative";
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName2, WireCommands.CreateSegment.NO_SCALE, 0, "", -1024L));
        val si2 = store.getStreamSegmentInfo(streamSegmentName2, PravegaRequestProcessor.TIMEOUT).join();
        val attributes2 = si2.getAttributes();
        Assert.assertEquals((long) attributes2.get(Attributes.ROLLOVER_SIZE), 0L); // fall back to default value

        // TestCreateSealDelete may executed before this test case,
        // so createSegmentStats may record 1 or 2 createSegment operation here.
    }

    @Test(timeout = 20000)
    public void testTransaction() throws Exception {
        String streamSegmentName = "scope/stream/testTxn";
        UUID txnid = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // create stream segment
        processor.createSegment(new WireCommands.CreateSegment(requestId, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, streamSegmentName));
        // create txn segment
        String transactionName = NameUtils.getTransactionNameFromId(streamSegmentName, txnid);
        processor.createSegment(new WireCommands.CreateSegment(requestId, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        // write to txn segment
        assertTrue(append(NameUtils.getTransactionNameFromId(streamSegmentName, txnid), 1, store));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transactionName, ""));
        assertTrue(append(NameUtils.getTransactionNameFromId(streamSegmentName, txnid), 2, store));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, transactionName));
        order.verify(connection).send(Mockito.argThat(t -> t instanceof WireCommands.StreamSegmentInfo && ((WireCommands.StreamSegmentInfo) t).exists()));

        processor.mergeSegmentsBatch(new WireCommands.MergeSegmentsBatch(requestId, streamSegmentName, ImmutableList.of(transactionName), ""));
        order.verify(connection).send(new WireCommands.SegmentsBatchMerged(requestId, streamSegmentName, ImmutableList.of(transactionName), ImmutableList.of(2L)));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transactionName, ""));
        order.verify(connection).send(new WireCommands.NoSuchSegment(requestId, NameUtils.getTransactionNameFromId(streamSegmentName, txnid), "", -1L));

        processor.mergeSegmentsBatch(new WireCommands.MergeSegmentsBatch(requestId, streamSegmentName, ImmutableList.of(transactionName), ""));
        order.verify(connection).send(new WireCommands.SegmentsBatchMerged(requestId, streamSegmentName, ImmutableList.of(transactionName), ImmutableList.of(2L)));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transactionName, ""));
        order.verify(connection).send(new WireCommands.NoSuchSegment(requestId, NameUtils.getTransactionNameFromId(streamSegmentName, txnid), "", -1L));

        //Segment Merge with multiple txns
        UUID txnid1 = UUID.randomUUID();
        UUID txnid2 = UUID.randomUUID();
        UUID txnid3 = UUID.randomUUID();

        String transaction1SegmentName = NameUtils.getTransactionNameFromId(streamSegmentName, txnid1);
        processor.createSegment(new WireCommands.CreateSegment(requestId, transaction1SegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, transaction1SegmentName));

        String transaction2SegmentName = NameUtils.getTransactionNameFromId(streamSegmentName, txnid2);
        processor.createSegment(new WireCommands.CreateSegment(requestId, transaction2SegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, transaction2SegmentName));

        String transaction3SegmentName = NameUtils.getTransactionNameFromId(streamSegmentName, txnid3);
        processor.createSegment(new WireCommands.CreateSegment(requestId, transaction3SegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, transaction3SegmentName));

        // write to txn segment(s)
        assertTrue(append(transaction1SegmentName, 1, store));
        assertTrue(append(transaction1SegmentName, 1, store));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transaction1SegmentName, ""));

        assertTrue(append(transaction2SegmentName, 2, store));
        assertTrue(append(transaction2SegmentName, 2, store));
        assertTrue(append(transaction2SegmentName, 2, store));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transaction2SegmentName, ""));

        assertTrue(append(transaction3SegmentName, 3, store));
        assertTrue(append(transaction3SegmentName, 3, store));
        assertTrue(append(transaction3SegmentName, 3, store));
        assertTrue(append(transaction3SegmentName, 3, store));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transaction3SegmentName, ""));
        order.verify(connection, times(3)).send(Mockito.argThat(t -> t instanceof WireCommands.StreamSegmentInfo && ((WireCommands.StreamSegmentInfo) t).exists()));

        List<String> txnSegmentNames = ImmutableList.of(transaction1SegmentName, transaction2SegmentName, transaction3SegmentName);
        processor.mergeSegmentsBatch(new WireCommands.MergeSegmentsBatch(requestId, streamSegmentName, txnSegmentNames, ""));
        order.verify(connection).send(new WireCommands.SegmentsBatchMerged(requestId, streamSegmentName, txnSegmentNames, ImmutableList.of(4L, 7L, 11L)));

        // verify that txn segments are merged and so do not exist any more
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transaction1SegmentName, ""));
        order.verify(connection).send(new WireCommands.NoSuchSegment(requestId, transaction1SegmentName, "", -1L));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transaction2SegmentName, ""));
        order.verify(connection).send(new WireCommands.NoSuchSegment(requestId, transaction2SegmentName, "", -1L));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transaction3SegmentName, ""));
        order.verify(connection).send(new WireCommands.NoSuchSegment(requestId, transaction3SegmentName, "", -1L));

        txnid = UUID.randomUUID();
        String transactionSegmentName = NameUtils.getTransactionNameFromId(streamSegmentName, txnid);

        processor.createSegment(new WireCommands.CreateSegment(requestId, transactionSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        assertTrue(append(NameUtils.getTransactionNameFromId(streamSegmentName, txnid), 1, store));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, transactionSegmentName));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transactionSegmentName, ""));
        order.verify(connection).send(Mockito.argThat(t -> t instanceof WireCommands.StreamSegmentInfo && ((WireCommands.StreamSegmentInfo) t).exists()));
        processor.deleteSegment(new WireCommands.DeleteSegment(requestId, transactionSegmentName, ""));
        order.verify(connection).send(new WireCommands.SegmentDeleted(requestId, transactionSegmentName));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transactionSegmentName, ""));
        order.verify(connection)
                .send(new WireCommands.NoSuchSegment(requestId, NameUtils.getTransactionNameFromId(streamSegmentName, txnid), "", -1L));

        // Verify the case when the transaction segment is already sealed. This simulates the case when the process
        // crashed after sealing, but before issuing the merge.
        txnid = UUID.randomUUID();
        transactionSegmentName = NameUtils.getTransactionNameFromId(streamSegmentName, txnid);

        processor.createSegment(new WireCommands.CreateSegment(requestId, transactionSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        assertTrue(append(NameUtils.getTransactionNameFromId(streamSegmentName, txnid), 1, store));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transactionSegmentName, ""));
        assertTrue(append(NameUtils.getTransactionNameFromId(streamSegmentName, txnid), 2, store));

        // Seal the transaction in the SegmentStore.
        String txnName = NameUtils.getTransactionNameFromId(streamSegmentName, txnid);
        store.sealStreamSegment(txnName, Duration.ZERO).join();

        processor.mergeSegmentsBatch(new WireCommands.MergeSegmentsBatch(requestId, streamSegmentName, ImmutableList.of(transactionSegmentName), ""));
        order.verify(connection).send(new WireCommands.SegmentsBatchMerged(requestId, streamSegmentName, ImmutableList.of(transactionSegmentName), ImmutableList.of(13L)));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transactionSegmentName, ""));
        order.verify(connection).send(new WireCommands.NoSuchSegment(requestId, NameUtils.getTransactionNameFromId(streamSegmentName, txnid), "", -1L));

        order.verifyNoMoreInteractions();
    }

    @Test(timeout = 20000)
    public void testIndexSegmentUpdateOnTxnMerge() throws Exception {
        String streamSegmentName = "scope/stream/testTxn";
        UUID txnid = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // create stream segment
        processor.createSegment(new WireCommands.CreateSegment(requestId, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, streamSegmentName));
        // create txn segment
        String transactionName = NameUtils.getTransactionNameFromId(streamSegmentName, txnid);
        processor.createSegment(new WireCommands.CreateSegment(requestId, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        // write to txn segment
        assertTrue(append(NameUtils.getTransactionNameFromId(streamSegmentName, txnid), 1, store));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transactionName, ""));
        assertTrue(append(NameUtils.getTransactionNameFromId(streamSegmentName, txnid), 2, store));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, transactionName));
        order.verify(connection).send(Mockito.argThat(t -> t instanceof WireCommands.StreamSegmentInfo && ((WireCommands.StreamSegmentInfo) t).exists()));
        doReturn(CompletableFuture.failedFuture(new StreamSegmentNotExistsException(getIndexSegmentName(streamSegmentName))))
            .when(store).getAttributes(eq(getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), any());
        processor.mergeSegmentsBatch(new WireCommands.MergeSegmentsBatch(requestId, streamSegmentName, ImmutableList.of(transactionName), ""));
        order.verify(connection).send(new WireCommands.SegmentsBatchMerged(requestId, streamSegmentName, ImmutableList.of(transactionName), ImmutableList.of(2L)));
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(requestId, transactionName, ""));
        order.verify(connection).send(new WireCommands.NoSuchSegment(requestId, NameUtils.getTransactionNameFromId(streamSegmentName, txnid), "", -1L));

        order.verifyNoMoreInteractions();
    }

    @Test(timeout = 20000)
    public void testIndexSegmentUpdateOnTransientSegmentMerge() throws Exception {
        String streamSegmentName = "scope/stream/testTxn";
        UUID writerId = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // create stream segment
        processor.createSegment(new WireCommands.CreateSegment(requestId, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, streamSegmentName));
        // create txn segment
        String transientName = NameUtils.getTransientNameFromId(streamSegmentName, writerId);
        processor.createSegment(new WireCommands.CreateSegment(requestId, transientName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, transientName));

        assertTrue(append(transientName, 1, store));

        processor.mergeSegments(new WireCommands.MergeSegments(requestId, streamSegmentName, transientName, ""));
        order.verify(connection).send(new WireCommands.SegmentsMerged(requestId, streamSegmentName, transientName, 1L));

        order.verifyNoMoreInteractions();
    }

    @Test(timeout = 10000)
    public void testIndexSegmentNotExistOnMerge() throws Exception {
        String streamSegmentName = "scope/stream/testTxn";
        UUID writerId = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        doReturn(CompletableFuture.completedFuture(null))
                .when(store).createStreamSegment(eq(NameUtils.getIndexSegmentName(streamSegmentName)),
                any(), any(), any());
        // create stream segment
        processor.createSegment(new WireCommands.CreateSegment(requestId, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, streamSegmentName));
        // create txn segment
        String transientName = NameUtils.getTransientNameFromId(streamSegmentName, writerId);
        processor.createSegment(new WireCommands.CreateSegment(requestId, transientName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, transientName));

        assertTrue(append(transientName, 1, store));

        processor.mergeSegments(new WireCommands.MergeSegments(requestId, streamSegmentName, transientName, ""));
        order.verify(connection).send(new WireCommands.SegmentsMerged(requestId, streamSegmentName, transientName, 1L));

        order.verifyNoMoreInteractions();
    }

    @Test(timeout = 10000)
    public void testIndexSegmentNotCreatedOnMerge() throws Exception {
        String streamSegmentName = "scope/stream/testTxn";
        UUID writerId = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        doReturn(CompletableFuture.completedFuture(null))
                .doReturn(CompletableFuture.failedFuture(new RuntimeException("Exception")))
                .when(store).createStreamSegment(eq(NameUtils.getIndexSegmentName(streamSegmentName)),
                        any(), any(), any());
        // create stream segment
        processor.createSegment(new WireCommands.CreateSegment(requestId, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, streamSegmentName));
        // create txn segment
        String transientName = NameUtils.getTransientNameFromId(streamSegmentName, writerId);
        processor.createSegment(new WireCommands.CreateSegment(requestId, transientName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, transientName));

        assertTrue(append(transientName, 1, store));

        processor.mergeSegments(new WireCommands.MergeSegments(requestId, streamSegmentName, transientName, ""));
        order.verify(connection).send(new WireCommands.SegmentsMerged(requestId, streamSegmentName, transientName, 1L));

        order.verifyNoMoreInteractions();
    }

    @Test(timeout = 20000)
    public void testMergedTransaction() throws Exception {
        String streamSegmentName = "scope/stream/testMergedTxn";
        UUID txnid = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);

        String transactionName = NameUtils.getTransactionNameFromId(streamSegmentName, txnid);
        doReturn(Futures.failedFuture(new StreamSegmentMergedException(transactionName))).when(store).sealStreamSegment(
                anyString(), any());
        doReturn(Futures.failedFuture(new StreamSegmentMergedException(transactionName))).when(store).mergeStreamSegment(
                anyString(), anyString(), any());

        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        processor.createSegment(new WireCommands.CreateSegment(requestId, streamSegmentName,
                WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, streamSegmentName));

        processor.createSegment(new WireCommands.CreateSegment(requestId, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, transactionName));
        processor.mergeSegmentsBatch(new WireCommands.MergeSegmentsBatch(requestId, streamSegmentName, ImmutableList.of(transactionName), ""));
        order.verify(connection).send(new WireCommands.SegmentsBatchMerged(requestId, streamSegmentName, ImmutableList.of(transactionName), List.of(0L)));
    }

    @Test(timeout = 20000)
    public void testTransactionTargetSegNotExists() throws Exception {
        String streamSegmentName = "scope/stream/testSegNotExistsTxn";
        UUID txnid = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        String transactionName = NameUtils.getTransactionNameFromId(streamSegmentName, txnid);

        doReturn(Futures.failedFuture(new StreamSegmentNotExistsException(streamSegmentName))).when(store).sealStreamSegment(
                anyString(), any());
        SegmentProperties segmentProperties = StreamSegmentInformation.builder().name(streamSegmentName).length(123).build();
        doReturn(CompletableFuture.completedFuture(segmentProperties)).when(store).getStreamSegmentInfo(anyString(), any());
        doReturn(Futures.failedFuture(new StreamSegmentNotExistsException(streamSegmentName)))
                .doReturn(Futures.failedFuture(new StreamSegmentMergedException(streamSegmentName)))
                .when(store).mergeStreamSegment(
                anyString(), anyString(), any(), any());
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        processor.createSegment(new WireCommands.CreateSegment(requestId, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, transactionName));
        processor.mergeSegmentsBatch(new WireCommands.MergeSegmentsBatch(requestId, streamSegmentName, ImmutableList.of(transactionName), ""));

        order.verify(connection).send(new WireCommands.NoSuchSegment(requestId, streamSegmentName, "", -1L));

        processor.mergeSegmentsBatch(new WireCommands.MergeSegmentsBatch(requestId, streamSegmentName, ImmutableList.of(transactionName), ""));
        order.verify(connection).send(new WireCommands.SegmentsBatchMerged(requestId, streamSegmentName, List.of(transactionName), List.of(123L)));
    }

    @Test(timeout = 20000)
    public void testMetricsOnSegmentMerge() throws Exception {
        String streamSegmentName = "scope/stream/txnSegment";
        UUID txnId = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        doReturn(Futures.failedFuture(new StreamSegmentMergedException(streamSegmentName))).when(store).sealStreamSegment(
                anyString(), any());

        //test txn segment merge
        CompletableFuture<MergeStreamSegmentResult> txnFuture = CompletableFuture.completedFuture(createMergeStreamSegmentResult(streamSegmentName, txnId));
        doReturn(txnFuture).when(store).mergeStreamSegment(anyString(), anyString(), any(), any());
        SegmentStatsRecorder recorderMock = mock(SegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), new TrackedConnection(connection),
                recorderMock, TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        processor.createSegment(new WireCommands.CreateSegment(0, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        String transactionName = NameUtils.getTransactionNameFromId(streamSegmentName, txnId);
        processor.createSegment(new WireCommands.CreateSegment(1, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        processor.mergeSegments(new WireCommands.MergeSegments(2, streamSegmentName, transactionName, ""));
        verify(recorderMock).merge(streamSegmentName, 100L, 10, streamSegmentName.hashCode());
    }

    private MergeStreamSegmentResult createMergeStreamSegmentResult(String streamSegmentName, UUID txnId) {
        Map<AttributeId, Long> attributes = new HashMap<>();
        attributes.put(Attributes.EVENT_COUNT, 10L);
        attributes.put(Attributes.CREATION_TIME, (long) streamSegmentName.hashCode());
        return new MergeStreamSegmentResult(100, 100, attributes);
    }

    @Test(timeout = 20000)
    public void testConditionalSegmentMergeReplaceIfEquals() throws Exception {
        String streamSegmentName = "scope/stream/txnSegment";
        UUID txnId = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        SegmentStatsRecorder recorderMock = mock(SegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), new TrackedConnection(connection),
                recorderMock, TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        processor.createSegment(new WireCommands.CreateSegment(0, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0L));
        order.verify(connection).send(new WireCommands.SegmentCreated(0, streamSegmentName));
        String transactionName = NameUtils.getTransactionNameFromId(streamSegmentName, txnId);
        processor.createSegment(new WireCommands.CreateSegment(1, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0L));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, transactionName));

        // Try to merge the transaction conditionally, when the attributes on the parent segment do not match.
        UUID randomAttribute1 = UUID.randomUUID();
        UUID randomAttribute2 = UUID.randomUUID();
        List<WireCommands.ConditionalAttributeUpdate> attributeUpdates = asList(
                new WireCommands.ConditionalAttributeUpdate(randomAttribute1, WireCommands.ConditionalAttributeUpdate.REPLACE_IF_EQUALS, 1, streamSegmentName.hashCode()),
                new WireCommands.ConditionalAttributeUpdate(randomAttribute2, WireCommands.ConditionalAttributeUpdate.REPLACE_IF_EQUALS, 2, streamSegmentName.hashCode())
        );

        // The first attempt should fail as the attribute update is not going to work.
        assertTrue(append(transactionName, 1, store));
        processor.mergeSegments(new WireCommands.MergeSegments(2, streamSegmentName, transactionName, "", attributeUpdates));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(2, false));

        // Now, set the right attributes in the parent segment.
        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(3, streamSegmentName, randomAttribute1,
                streamSegmentName.hashCode(), WireCommands.NULL_ATTRIBUTE_VALUE, ""));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(3, true));
        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(4, streamSegmentName, randomAttribute2,
                streamSegmentName.hashCode(), WireCommands.NULL_ATTRIBUTE_VALUE, ""));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(4, true));

        // Merge segments conditionally, now it should work.
        processor.mergeSegments(new WireCommands.MergeSegments(5, streamSegmentName, transactionName, "", attributeUpdates));
        order.verify(connection).send(new WireCommands.SegmentsMerged(5, streamSegmentName, transactionName, 1));

        // Check the value of attributes post merge.
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(6, streamSegmentName, randomAttribute1, ""));
        order.verify(connection).send(new WireCommands.SegmentAttribute(6, 1));
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(7, streamSegmentName, randomAttribute2, ""));
        order.verify(connection).send(new WireCommands.SegmentAttribute(7, 2));
    }

    @Test(timeout = 20000)
    public void testConditionalSegmentMergeReplace() throws Exception {
        String streamSegmentName = "scope/stream/txnSegment";
        UUID txnId = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        SegmentStatsRecorder recorderMock = mock(SegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, mock(TableStore.class), new TrackedConnection(connection),
                recorderMock, TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        processor.createSegment(new WireCommands.CreateSegment(0, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0L));
        order.verify(connection).send(new WireCommands.SegmentCreated(0, streamSegmentName));
        String transactionName = NameUtils.getTransactionNameFromId(streamSegmentName, txnId);
        processor.createSegment(new WireCommands.CreateSegment(1, transactionName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0L));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, transactionName));
        assertTrue(append(transactionName, 1, store));

        // Update to perform.
        UUID randomAttribute1 = UUID.randomUUID();
        UUID randomAttribute2 = UUID.randomUUID();
        List<WireCommands.ConditionalAttributeUpdate> attributeUpdates = asList(
                new WireCommands.ConditionalAttributeUpdate(randomAttribute1, WireCommands.ConditionalAttributeUpdate.REPLACE, 1, 0),
                new WireCommands.ConditionalAttributeUpdate(randomAttribute2, WireCommands.ConditionalAttributeUpdate.REPLACE, 2, 0)
        );

        // Set a attributes in the parent segment with a certain value.
        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(2, streamSegmentName, randomAttribute1,
                streamSegmentName.hashCode(), WireCommands.NULL_ATTRIBUTE_VALUE, ""));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(2, true));
        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(3, streamSegmentName, randomAttribute2,
                streamSegmentName.hashCode(), WireCommands.NULL_ATTRIBUTE_VALUE, ""));
        order.verify(connection).send(new WireCommands.SegmentAttributeUpdated(3, true));

        // Check the value of attributes post merge.
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(4, streamSegmentName, randomAttribute1, ""));
        order.verify(connection).send(new WireCommands.SegmentAttribute(4, streamSegmentName.hashCode()));
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(5, streamSegmentName, randomAttribute2, ""));
        order.verify(connection).send(new WireCommands.SegmentAttribute(5, streamSegmentName.hashCode()));

        // Merge segments replacing attributes, now it should work.
        processor.mergeSegments(new WireCommands.MergeSegments(6, streamSegmentName, transactionName, "", attributeUpdates));
        order.verify(connection).send(new WireCommands.SegmentsMerged(6, streamSegmentName, transactionName, 1));

        // Check the value of attributes post merge.
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(7, streamSegmentName, randomAttribute1, ""));
        order.verify(connection).send(new WireCommands.SegmentAttribute(7, 1));
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(8, streamSegmentName, randomAttribute2, ""));
        order.verify(connection).send(new WireCommands.SegmentAttribute(8, 2));
    }

    @Test(timeout = 20000)
    public void testSegmentAttribute() throws Exception {
        String streamSegmentName = "scope/stream/testSegmentAttribute";
        UUID attribute = UUID.randomUUID();
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // Execute and Verify createSegment/getStreamSegmentInfo calling stack is executed as design.
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
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
        String streamSegmentName = "scope/stream/testCreateSealDelete";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store,  mock(TableStore.class), connection,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // Create a segment and append 2 bytes.
        processor.createSegment(new WireCommands.CreateSegment(1, streamSegmentName, WireCommands.CreateSegment.NO_SCALE, 0, "", 0));
        assertTrue(append(streamSegmentName, 1, store));
        assertTrue(append(streamSegmentName, 2, store));

        processor.sealSegment(new WireCommands.SealSegment(requestId, streamSegmentName, ""));
        assertFalse(append(streamSegmentName, 2, store));

        // Truncate half.
        final long truncateOffset = store.getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT)
                .join().getLength() / 2;
        AssertExtensions.assertGreaterThan("Nothing to truncate.", 0, truncateOffset);
        processor.truncateSegment(new WireCommands.TruncateSegment(requestId, streamSegmentName, truncateOffset, ""));
        assertEquals(truncateOffset, store.getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT)
                .join().getStartOffset());

        // Truncate at the same offset - verify idempotence.
        processor.truncateSegment(new WireCommands.TruncateSegment(requestId, streamSegmentName, truncateOffset, ""));
        assertEquals(truncateOffset, store.getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT)
                .join().getStartOffset());

        // Truncate at a lower offset - verify failure.
        processor.truncateSegment(new WireCommands.TruncateSegment(requestId, streamSegmentName, truncateOffset - 1, ""));
        assertEquals(truncateOffset, store.getStreamSegmentInfo(streamSegmentName, PravegaRequestProcessor.TIMEOUT)
                .join().getStartOffset());

        // Delete.
        processor.deleteSegment(new WireCommands.DeleteSegment(requestId, streamSegmentName, ""));
        assertFalse(append(streamSegmentName, 4, store));

        // Verify connection response with same order.
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, streamSegmentName));
        order.verify(connection).send(new WireCommands.SegmentSealed(requestId, streamSegmentName));
        order.verify(connection, times(2)).send(new WireCommands.SegmentTruncated(requestId, streamSegmentName));
        order.verify(connection).send(new WireCommands.SegmentIsTruncated(requestId, streamSegmentName, truncateOffset, "", 0));
        order.verify(connection).send(new WireCommands.SegmentDeleted(requestId, streamSegmentName));
        order.verifyNoMoreInteractions();
    }
    //endregion

    //region Table Segments

    @Test(timeout = 20000)
    public void testCreateTableSegment() throws Exception {
        testCreateTableSegment(0);
        testCreateTableSegment(128);
    }

    private void testCreateTableSegment(int keyLength) throws Exception {
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, new TrackedConnection(connection),
                SegmentStatsRecorder.noOp(), recorderMock, new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        int requestId = 0;

        // GetInfo with non-existent Table Segment.
        processor.getTableSegmentInfo(new WireCommands.GetTableSegmentInfo(++requestId, tableSegmentName, ""));
        order.verify(connection).send(new WireCommands.NoSuchSegment(requestId, tableSegmentName, "", -1));

        // Execute and Verify createTableSegment calling stack is executed as design.
        processor.createTableSegment(new WireCommands.CreateTableSegment(++requestId, tableSegmentName, false, keyLength, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(requestId, tableSegmentName));

        processor.createTableSegment(new WireCommands.CreateTableSegment(++requestId, tableSegmentName, false, keyLength, "", 0));
        order.verify(connection).send(new WireCommands.SegmentAlreadyExists(requestId, tableSegmentName, ""));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());
        verifyNoMoreInteractions(recorderMock);

        // Verify the correct type of Segment is created and that it has the correct roles.
        val si = store.getStreamSegmentInfo(tableSegmentName, PravegaRequestProcessor.TIMEOUT).join();
        val segmentType = SegmentType.fromAttributes(si.getAttributes());
        Assert.assertFalse(segmentType.isInternal() || segmentType.isCritical() || segmentType.isSystem());
        Assert.assertTrue(segmentType.isTableSegment());
        Assert.assertEquals(keyLength > 0, segmentType.isFixedKeyLengthTableSegment());
        val kl = si.getAttributes().getOrDefault(Attributes.ATTRIBUTE_ID_LENGTH, 0L);
        Assert.assertEquals(keyLength, (long) kl);

        // Verify segment info can be retrieved.
        processor.getTableSegmentInfo(new WireCommands.GetTableSegmentInfo(++requestId, tableSegmentName, ""));
        order.verify(connection).send(new WireCommands.TableSegmentInfo(requestId, tableSegmentName, 0, 0, 0, keyLength));
        
        // Verify invoking GetTableSegmentInfo on a non-existing segment returns NoSuchSegment
        String nonExistingSegment = "nonExistingSegment";
        processor.getTableSegmentInfo(new WireCommands.GetTableSegmentInfo(++requestId, nonExistingSegment, ""));
        order.verify(connection).send(new WireCommands.NoSuchSegment(requestId, nonExistingSegment, "", -1));

        // Verify table segment has correct rollover size
        // Verify default value
        val attributes = si.getAttributes();
        val config = TableExtensionConfig.builder().build();
        Assert.assertEquals((long) attributes.get(Attributes.ROLLOVER_SIZE), config.getDefaultRolloverSize());

        // Verify custom value
        val tableSegmentName1 = "testCreateTableSegmentRolloverSizePositive";
        processor.createTableSegment(new WireCommands.CreateTableSegment(++requestId, tableSegmentName1, false, keyLength, "", 1024 * 1024L));
        val si1 = store.getStreamSegmentInfo(tableSegmentName1, PravegaRequestProcessor.TIMEOUT).join();
        val attributes1 = si1.getAttributes();
        Assert.assertEquals((long) attributes1.get(Attributes.ROLLOVER_SIZE), 1024 * 1024L);

        // Verify invalid value
        val tableSegmentName2 = "testCreateTableSegmentRolloverSizeNegative";
        processor.createTableSegment(new WireCommands.CreateTableSegment(++requestId, tableSegmentName2, false, keyLength, "", -1024L));
        val si2 = store.getStreamSegmentInfo(tableSegmentName2, PravegaRequestProcessor.TIMEOUT).join();
        val attributes2 = si2.getAttributes();
        Assert.assertEquals((long) attributes2.get(Attributes.ROLLOVER_SIZE), config.getDefaultRolloverSize()); // fall back to default value
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
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, new TrackedConnection(connection, tracker),
                SegmentStatsRecorder.noOp(), recorderMock, new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        //Generate keys
        ArrayList<ArrayView> keys = generateKeys(3, rnd);

        // Execute and Verify createSegment calling stack is executed as design.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, false, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());

        // Test with unversioned data.
        TableEntry e1 = TableEntry.unversioned(keys.get(0), generateValue(rnd));
        WireCommands.TableEntries cmd = getTableEntries(singletonList(e1));
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(2, tableSegmentName, "", cmd, WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        order.verify(connection).send(new WireCommands.TableEntriesUpdated(2, singletonList(0L)));
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(1), eq(false), any());
        verifyConnectionTracker(e1, connection, tracker);

        // Test with versioned data.
        e1 = TableEntry.versioned(keys.get(0), generateValue(rnd), 0L);
        cmd = getTableEntries(singletonList(e1));
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(3, tableSegmentName, "", cmd, WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        ArgumentCaptor<WireCommand> wireCommandsCaptor = ArgumentCaptor.forClass(WireCommand.class);
        order.verify(connection).send(wireCommandsCaptor.capture());
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(1), eq(true), any());
        verifyConnectionTracker(e1, connection, tracker);

        List<Long> keyVersions = ((WireCommands.TableEntriesUpdated) wireCommandsCaptor.getAllValues().get(0)).getUpdatedVersions();
        assertTrue(keyVersions.size() == 1);

        // Test with key not present. The table store throws KeyNotExistsException.
        TableEntry e2 = TableEntry.versioned(keys.get(1), generateValue(rnd), 0L);
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(4, tableSegmentName, "", getTableEntries(singletonList(e2)), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        order.verify(connection).send(new WireCommands.TableKeyDoesNotExist(4, tableSegmentName, ""));
        verifyNoMoreInteractions(recorderMock);
        verifyConnectionTracker(e2, connection, tracker);

        // Test with invalid key version. The table store throws BadKeyVersionException.
        TableEntry e3 = TableEntry.versioned(keys.get(0), generateValue(rnd), 10L);
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(5, tableSegmentName, "", getTableEntries(singletonList(e3)), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        order.verify(connection).send(new WireCommands.TableKeyBadVersion(5, tableSegmentName, ""));
        verifyNoMoreInteractions(recorderMock);
        verifyConnectionTracker(e3, connection, tracker);

        // Test with valid tableSegmentOffset.
        long tableSegmentOffset = store.getStreamSegmentInfo(tableSegmentName, Duration.ofMinutes(1)).get().getLength();
        TableEntry e4 = TableEntry.versioned(keys.get(0), generateValue(rnd), keyVersions.get(0));
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(6, tableSegmentName, "", getTableEntries(singletonList(e4)), tableSegmentOffset));
        order.verify(connection).send(wireCommandsCaptor.capture());
        verify(recorderMock, times(2)).updateEntries(eq(tableSegmentName), eq(1), eq(true), any());
        verifyConnectionTracker(e4, connection, tracker);

        keyVersions = ((WireCommands.TableEntriesUpdated) wireCommandsCaptor.getAllValues().get(1)).getUpdatedVersions();
        assertTrue(keyVersions.size() == 1);
        // Test with invalid tableSegmentOffset.
        TableEntry e5 = TableEntry.versioned(keys.get(0), generateValue(rnd), keyVersions.get(0));
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(7, tableSegmentName, "", getTableEntries(singletonList(e5)), tableSegmentOffset - 1));
        long length = store.getStreamSegmentInfo(tableSegmentName, Duration.ofMinutes(1)).get().getLength();
        order.verify(connection).send(new WireCommands.SegmentIsTruncated(7, tableSegmentName, length, "", tableSegmentOffset - 1));
        verify(recorderMock, times(2)).updateEntries(eq(tableSegmentName), eq(1), eq(true), any());
        verifyConnectionTracker(e5, connection, tracker);
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
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, new TrackedConnection(connection, tracker),
                SegmentStatsRecorder.noOp(), recorderMock, new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // Generate keys.
        ArrayList<ArrayView> keys = generateKeys(3, rnd);

        // Create a table segment and add data.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, false, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        TableEntry e1 = TableEntry.unversioned(keys.get(0), generateValue(rnd));
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(2, tableSegmentName, "", getTableEntries(singletonList(e1)), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        order.verify(connection).send(new WireCommands.TableEntriesUpdated(2, singletonList(0L)));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(1), eq(false), any());
        verifyConnectionTracker(e1, connection, tracker);

        // Remove a Table Key
        WireCommands.TableKey key = new WireCommands.TableKey(toByteBuf(e1.getKey().getKey()), 0L);
        processor.removeTableKeys(new WireCommands.RemoveTableKeys(3, tableSegmentName, "", singletonList(key), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        order.verify(connection).send(new WireCommands.TableKeysRemoved(3, tableSegmentName));
        verify(recorderMock).removeKeys(eq(tableSegmentName), eq(1), eq(true), any());
        verifyConnectionTracker(key, connection, tracker);

        // Test with non-existent key.
        key = new WireCommands.TableKey(toByteBuf(e1.getKey().getKey()), 0L);
        processor.removeTableKeys(new WireCommands.RemoveTableKeys(4, tableSegmentName, "", singletonList(key), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        order.verify(connection).send(new WireCommands.TableKeyBadVersion(4, tableSegmentName, ""));
        verifyConnectionTracker(key, connection, tracker);

        long segmentLength = store.getStreamSegmentInfo(tableSegmentName, Duration.ofMinutes(1)).get().getLength();
        // Test when providing a matching tableSegmentOffset.
        TableEntry e2 = TableEntry.unversioned(keys.get(1), generateValue(rnd));
        key = new WireCommands.TableKey(toByteBuf(e2.getKey().getKey()), e2.getKey().getVersion());
        processor.removeTableKeys(new WireCommands.RemoveTableKeys(5, tableSegmentName, "", singletonList(key), segmentLength));
        order.verify(connection).send(new WireCommands.TableKeysRemoved(5, tableSegmentName));
        verify(recorderMock).removeKeys(eq(tableSegmentName), eq(1), eq(true), any());
        verifyConnectionTracker(key, connection, tracker);

        //// Test when providing a mismatching tableSegmentOffset.
        long badOffset = segmentLength - 1;
        TableEntry e3 = TableEntry.unversioned(keys.get(2), generateValue(rnd));
        key = new WireCommands.TableKey(toByteBuf(e3.getKey().getKey()), e3.getKey().getVersion());
        processor.removeTableKeys(new WireCommands.RemoveTableKeys(6, tableSegmentName, "", singletonList(key), badOffset));
        segmentLength = store.getStreamSegmentInfo(tableSegmentName, Duration.ofMinutes(1)).get().getLength();
        // BadOffsetException should be thrown, prompting a SegmentIsTruncated response.
        order.verify(connection).send(new WireCommands.SegmentIsTruncated(6, tableSegmentName, segmentLength, "", badOffset));
        verify(recorderMock).removeKeys(eq(tableSegmentName), eq(1), eq(true), any());
        verifyConnectionTracker(key, connection, tracker);
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, new TrackedConnection(connection),
                SegmentStatsRecorder.noOp(), recorderMock, new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // Create a table segment.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, false, 0, "", 0));
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, new TrackedConnection(connection),
                SegmentStatsRecorder.noOp(), recorderMock, new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // Generate keys.
        ArrayList<ArrayView> keys = generateKeys(2, rnd);

        // Create a table segment and add data.
        processor.createTableSegment(new WireCommands.CreateTableSegment(3, tableSegmentName, false, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(3, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());

        TableEntry e1 = TableEntry.unversioned(keys.get(0), generateValue(rnd));
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(4, tableSegmentName, "", getTableEntries(singletonList(e1)), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, new TrackedConnection(connection),
                SegmentStatsRecorder.noOp(), recorderMock, new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // Generate keys.
        ArrayList<ArrayView> keys = generateKeys(2, rnd);

        // Create a table segment and add data.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, false, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        recorderMockOrder.verify(recorderMock).createTableSegment(eq(tableSegmentName), any());
        TableEntry entry = TableEntry.unversioned(keys.get(0), generateValue(rnd));

        // Read value of a non-existent key.
        WireCommands.TableKey key = new WireCommands.TableKey(toByteBuf(entry.getKey().getKey()), TableKey.NO_VERSION);
        processor.readTable(new WireCommands.ReadTable(2, tableSegmentName, "", singletonList(key)));

        // expected result is Key (with key with version as NOT_EXISTS) and an empty TableValue.)
        WireCommands.TableKey keyResponse = new WireCommands.TableKey(toByteBuf(entry.getKey().getKey()),
                WireCommands.TableKey.NOT_EXISTS);
        order.verify(connection).send(new WireCommands.TableRead(2, tableSegmentName,
                                                                 new WireCommands.TableEntries(
                                                                         singletonList(new AbstractMap.SimpleImmutableEntry<>(keyResponse, WireCommands.TableValue.EMPTY)))));
        recorderMockOrder.verify(recorderMock).getKeys(eq(tableSegmentName), eq(1), any());

        // Update a value to a key.
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(3, tableSegmentName, "", getTableEntries(singletonList(entry)), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        order.verify(connection).send(new WireCommands.TableEntriesUpdated(3, singletonList(0L)));
        recorderMockOrder.verify(recorderMock).updateEntries(eq(tableSegmentName), eq(1), eq(false), any());

        // Read the value of the key.
        key = new WireCommands.TableKey(toByteBuf(entry.getKey().getKey()), 0L);
        TableEntry expectedEntry = TableEntry.versioned(entry.getKey().getKey(), entry.getValue(), 0L);
        processor.readTable(new WireCommands.ReadTable(4, tableSegmentName, "", singletonList(key)));
        order.verify(connection).send(new WireCommands.TableRead(4, tableSegmentName,
                                                                 getTableEntries(singletonList(expectedEntry))));
        recorderMockOrder.verify(recorderMock).getKeys(eq(tableSegmentName), eq(1), any());
    }

    @Test(timeout = 10000)
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, new TrackedConnection(connection),
                SegmentStatsRecorder.noOp(), recorderMock, new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // Generate keys.
        ArrayList<ArrayView> keys = generateKeys(3, rnd);
        TableEntry e1 = TableEntry.unversioned(keys.get(0), generateValue(rnd));
        TableEntry e2 = TableEntry.unversioned(keys.get(1), generateValue(rnd));
        TableEntry e3 = TableEntry.unversioned(keys.get(2), generateValue(rnd));

        // Create a table segment and add data.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, false, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(2, tableSegmentName, "", getTableEntries(asList(e1, e2, e3)), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(3), eq(false), any());

        // 1. Now read the table keys where suggestedKeyCount is equal to number of entries in the Table Store.
        WireCommands.TableIteratorArgs args = new WireCommands.TableIteratorArgs(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
        processor.readTableKeys(new WireCommands.ReadTableKeys(3, tableSegmentName, "", 3, args));

        // Capture the WireCommands sent.
        ArgumentCaptor<WireCommand> wireCommandsCaptor = ArgumentCaptor.forClass(WireCommand.class);
        order.verify(connection, times(2)).send(wireCommandsCaptor.capture());
        verify(recorderMock).iterateKeys(eq(tableSegmentName), eq(3), any());

        // Verify the WireCommands.
        List<Long> keyVersions = ((WireCommands.TableEntriesUpdated) wireCommandsCaptor.getAllValues().get(0)).getUpdatedVersions();
        WireCommands.TableKeysRead getTableKeysReadResponse = (WireCommands.TableKeysRead) wireCommandsCaptor.getAllValues().get(1);
        assertTrue(getTableKeysReadResponse.getKeys().stream().map(WireCommands.TableKey::getKeyVersion).collect(Collectors.toList()).containsAll(keyVersions));

        // 2. Now read the table keys where suggestedKeyCount is less than the number of keys in the Table Store.
        processor.readTableKeys(new WireCommands.ReadTableKeys(3, tableSegmentName, "", 1, args));

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
        args = new WireCommands.TableIteratorArgs(state, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);

        // 3. Now read the remaining table keys by providing a higher suggestedKeyCount and the state to the iterator.
        processor.readTableKeys(new WireCommands.ReadTableKeys(3, tableSegmentName, "", 3, args));
        // Capture the WireCommands sent.
        tableKeysCaptor = ArgumentCaptor.forClass(WireCommands.TableKeysRead.class);
        order.verify(connection, times(1)).send(tableKeysCaptor.capture());
        verify(recorderMock).iterateKeys(eq(tableSegmentName), eq(1), any());

        // Verify the WireCommands.
        getTableKeysReadResponse =  tableKeysCaptor.getAllValues().get(0);
        assertEquals(2, getTableKeysReadResponse.getKeys().size());
        assertTrue(keyVersions.containsAll(getTableKeysReadResponse.getKeys().stream().map(WireCommands.TableKey::getKeyVersion).collect(Collectors.toList())));
    }

    @Test(timeout = 10000)
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
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, new TrackedConnection(connection),
                SegmentStatsRecorder.noOp(), recorderMock, new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // Generate keys.
        ArrayList<ArrayView> keys = generateKeys(3, rnd);
        ArrayView testValue = generateValue(rnd);
        TableEntry e1 = TableEntry.unversioned(keys.get(0), testValue);
        TableEntry e2 = TableEntry.unversioned(keys.get(1), testValue);
        TableEntry e3 = TableEntry.unversioned(keys.get(2), testValue);

        // Create a table segment and add data.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, false, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(2, tableSegmentName, "", getTableEntries(asList(e1, e2, e3)), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(3), eq(false), any());

        // 1. Now read the table entries where suggestedEntryCount is equal to number of entries in the Table Store.
        WireCommands.TableIteratorArgs args = new WireCommands.TableIteratorArgs(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
        processor.readTableEntries(new WireCommands.ReadTableEntries(3, tableSegmentName, "", 3, args));

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
            return testValue.equals(new ByteArraySegment(bytes));
        }));

        // 2. Now read the table keys where suggestedEntryCount is less than the number of entries in the Table Store.
        processor.readTableEntries(new WireCommands.ReadTableEntries(3, tableSegmentName, "", 1, args));

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
        args = new WireCommands.TableIteratorArgs(state, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);

        // 3. Now read the remaining table entries by providing a higher suggestedKeyCount and the state to the iterator.
        processor.readTableEntries(new WireCommands.ReadTableEntries(3, tableSegmentName, "", 3, args));
        // Capture the WireCommands sent.
        tableEntriesCaptor = ArgumentCaptor.forClass(WireCommands.TableEntriesRead.class);
        order.verify(connection, times(1)).send(tableEntriesCaptor.capture());
        verify(recorderMock).iterateEntries(eq(tableSegmentName), eq(1), any());

        // Verify the WireCommands.
        getTableEntriesIteratorsResp =  tableEntriesCaptor.getAllValues().get(0);
        assertEquals(2, getTableEntriesIteratorsResp.getEntries().getEntries().size());
        assertTrue(keyVersions.containsAll(getTableEntriesIteratorsResp.getEntries().getEntries().stream().map(e -> e.getKey().getKeyVersion()).collect(Collectors.toList())));
    }

    @Test(timeout = 10000)
    public void testReadTableEntriesDeltaEmpty() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        String tableSegmentName = "testReadTableEntriesDelta";

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, new TrackedConnection(connection), SegmentStatsRecorder.noOp(),
                recorderMock, new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, false, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());

        processor.readTableEntriesDelta(new WireCommands.ReadTableEntriesDelta(1, tableSegmentName, "", 0, 3));
        ArgumentCaptor<WireCommand> wireCommandsCaptor = ArgumentCaptor.forClass(WireCommand.class);
        verify(recorderMock).iterateEntries(eq(tableSegmentName), eq(0), any());
    }

    @Test(timeout = 10000)
    public void testReadTableEntriesDeltaOutOfBounds() throws  Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        val rnd = new Random(0);
        String tableSegmentName = "testReadTableEntriesDelta";

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, new TrackedConnection(connection), SegmentStatsRecorder.noOp(),
                recorderMock, new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, false, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());

        // Generate keys.
        ArrayList<ArrayView> keys = generateKeys(3, rnd);
        ArrayView testValue = generateValue(rnd);
        TableEntry e1 = TableEntry.unversioned(keys.get(0), testValue);
        TableEntry e2 = TableEntry.unversioned(keys.get(1), testValue);
        TableEntry e3 = TableEntry.unversioned(keys.get(2), testValue);

        processor.updateTableEntries(new WireCommands.UpdateTableEntries(2, tableSegmentName, "",
                getTableEntries(asList(e1, e2, e3)), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(3), eq(false), any());
        long length = store.getStreamSegmentInfo(tableSegmentName, Duration.ofMinutes(1)).get().getLength();
        processor.readTableEntriesDelta(new WireCommands.ReadTableEntriesDelta(1, tableSegmentName, "", length + 1, 3));
        order.verify(connection).send(new WireCommands.ErrorMessage(1,
                tableSegmentName,
                "fromPosition (652) can not exceed the length (651) of the TableSegment.",
                WireCommands.ErrorMessage.ErrorCode.valueOf(IllegalArgumentException.class)));
        verify(recorderMock, times(0)).iterateEntries(eq(tableSegmentName), eq(3), any());
    }

    @Test(timeout = 10000)
    public void testReadTableEntriesDelta() throws Exception {
        // Set up PravegaRequestProcessor instance to execute requests against
        val rnd = new Random(0);
        String tableSegmentName = "testReadTableEntriesDelta";

        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        val recorderMock = mock(TableSegmentStatsRecorder.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, tableStore, new TrackedConnection(connection),
                SegmentStatsRecorder.noOp(), recorderMock, new PassingTokenVerifier(), false,
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));

        // Generate keys.
        ArrayList<ArrayView> keys = generateKeys(3, rnd);
        ArrayView testValue = generateValue(rnd);
        TableEntry e1 = TableEntry.unversioned(keys.get(0), testValue);
        TableEntry e2 = TableEntry.unversioned(keys.get(1), testValue);
        TableEntry e3 = TableEntry.unversioned(keys.get(2), testValue);

        // Create a table segment and add data.
        processor.createTableSegment(new WireCommands.CreateTableSegment(1, tableSegmentName, false, 0, "", 0));
        order.verify(connection).send(new WireCommands.SegmentCreated(1, tableSegmentName));
        verify(recorderMock).createTableSegment(eq(tableSegmentName), any());
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(2, tableSegmentName, "",
                getTableEntries(asList(e1, e2, e3)), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(3), eq(false), any());

        // 1. Now read the table entries where suggestedEntryCount is equal to number of entries in the Table Store.
        processor.readTableEntriesDelta(new WireCommands.ReadTableEntriesDelta(3, tableSegmentName, "", 0, 3));

        // Capture the WireCommands sent.
        ArgumentCaptor<WireCommand> wireCommandsCaptor = ArgumentCaptor.forClass(WireCommand.class);
        order.verify(connection, times(2)).send(wireCommandsCaptor.capture());
        verify(recorderMock).iterateEntries(eq(tableSegmentName), eq(3), any());

        // Verify the WireCommands.
        List<Long> keyVersions = ((WireCommands.TableEntriesUpdated) wireCommandsCaptor.getAllValues().get(0)).getUpdatedVersions();
        WireCommands.TableEntriesDeltaRead getTableEntriesIteratorsResp =
                (WireCommands.TableEntriesDeltaRead) wireCommandsCaptor.getAllValues().get(1);
        assertTrue(getTableEntriesIteratorsResp.getEntries().getEntries().stream().map(e -> e.getKey().getKeyVersion()).collect(Collectors.toList()).containsAll(keyVersions));
        // Verify if the value is correct.
        assertTrue(getTableEntriesIteratorsResp.getEntries().getEntries().stream().allMatch(e -> {
            ByteBuf buf = e.getValue().getData();
            byte[] bytes = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), bytes);
            return testValue.equals(new ByteArraySegment(bytes));
        }));

        // 2. Now read the table entries where suggestedEntryCount is less than the number of entries in the Table Store.
        processor.readTableEntriesDelta(new WireCommands.ReadTableEntriesDelta(3, tableSegmentName, "", 0L, 1));

        // Capture the WireCommands sent.
        ArgumentCaptor<WireCommands.TableEntriesDeltaRead> tableEntriesCaptor =
                ArgumentCaptor.forClass(WireCommands.TableEntriesDeltaRead.class);
        order.verify(connection, times(1)).send(tableEntriesCaptor.capture());

        // Verify the WireCommands.
        getTableEntriesIteratorsResp = tableEntriesCaptor.getAllValues().get(0);
        assertEquals(1, getTableEntriesIteratorsResp.getEntries().getEntries().size());
        assertTrue(keyVersions.contains(getTableEntriesIteratorsResp.getEntries().getEntries().get(0).getKey().getKeyVersion()));

        assertFalse(getTableEntriesIteratorsResp.isShouldClear());
        assertFalse(getTableEntriesIteratorsResp.isReachedEnd());
        // Get the last position.
        long lastPosition = getTableEntriesIteratorsResp.getLastPosition();

        // 3. Now read the remaining table entries by providing a higher suggestedKeyCount and the state to the iterator.
        processor.readTableEntriesDelta(new WireCommands.ReadTableEntriesDelta(3, tableSegmentName, "",  lastPosition, 3));
        // Capture the WireCommands sent.
        order.verify(connection, times(1)).send(tableEntriesCaptor.capture());
        verify(recorderMock).iterateEntries(eq(tableSegmentName), eq(1), any());

        // Verify the WireCommands.
        getTableEntriesIteratorsResp =  tableEntriesCaptor.getAllValues().get(1);
        // We read through all the entries, so this should report as having reached the end.
        assertTrue(getTableEntriesIteratorsResp.isReachedEnd());
        assertEquals(2, getTableEntriesIteratorsResp.getEntries().getEntries().size());
        assertTrue(keyVersions.containsAll(getTableEntriesIteratorsResp.getEntries().getEntries().stream().map(e -> e.getKey().getKeyVersion()).collect(Collectors.toList())));

        // 4. Update some TableEntry.
        TableEntry e4 = TableEntry.versioned(keys.get(0), generateValue(rnd), keyVersions.get(0));
        processor.updateTableEntries(new WireCommands.UpdateTableEntries(4, tableSegmentName, "",
                getTableEntries(asList(e4)), WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        verify(recorderMock).updateEntries(eq(tableSegmentName), eq(1), eq(true), any());

        // 5. Remove some TableEntry.
        TableEntry e5 = TableEntry.versioned(keys.get(1), generateValue(rnd), keyVersions.get(1));
        WireCommands.TableKey key = new WireCommands.TableKey(toByteBuf(e5.getKey().getKey()), e5.getKey().getVersion());
        processor.removeTableKeys(new WireCommands.RemoveTableKeys(5, tableSegmentName, "", singletonList(key),
                WireCommands.NULL_TABLE_SEGMENT_OFFSET));
        order.verify(connection).send(new WireCommands.TableKeysRemoved(5, tableSegmentName));
        verify(recorderMock).removeKeys(eq(tableSegmentName), eq(1), eq(true), any());

        //// Now very the delta iteration returns a list with an updated key (4.) and a removed key (5.).
        processor.readTableEntriesDelta(new WireCommands.ReadTableEntriesDelta(6, tableSegmentName, "",  0L, 4));
        //verify(recorderMock).iterateEntries(eq(tableSegmentName), eq(3), any());
        // Capture the WireCommands sent.
        order.verify(connection).send(tableEntriesCaptor.capture());
        // Verify the WireCommands.
        getTableEntriesIteratorsResp =  tableEntriesCaptor.getAllValues().get(2);
        val results = getTableEntriesIteratorsResp.getEntries().getEntries()
                .stream()
                .map(e -> TableEntry.versioned(
                        new ByteBufWrapper(e.getKey().getData()),
                        new ByteBufWrapper(e.getValue().getData()),
                        e.getKey().getKeyVersion()))
                .collect(Collectors.toList());

        assertEquals("Expecting 2 entries left in the TableSegment", 2, results.size());
        // Does not container entry removed.
        assertFalse(results.contains(e5));
    }

    @Test(timeout = 10000)
    public void testCreateSealTruncateDeleteIndexSegment() throws Exception {
        String segment = "testCreateSealTruncateDeleteIndexSegment/testStream/0";
        String indexSegment = getIndexSegmentName(segment);
        ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());

        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        EmbeddedChannel channel = createChannel(store);
        WireCommands.SegmentCreated created = (WireCommands.SegmentCreated) sendRequest(channel, new WireCommands.CreateSegment(1, segment, WireCommands.CreateSegment.NO_SCALE, 0, "", 1024L));
        assertEquals(segment, created.getSegment());

        UUID uuid = UUID.randomUUID();
        WireCommands.AppendSetup setup = (WireCommands.AppendSetup) sendRequest(channel, new WireCommands.SetupAppend(2, uuid, segment, ""));

        assertEquals(segment, setup.getSegment());
        assertEquals(uuid, setup.getWriterId());

        data.retain();

        //Append 40 bytes of data
        sendRequest(channel, new Append(segment, uuid, 1, new WireCommands.Event(data), 1L));
        sendRequest(channel, new Append(segment, uuid, 2, new WireCommands.Event(data), 1L));

        //Total length
        assertEquals(store.getStreamSegmentInfo(segment, PravegaRequestProcessor.TIMEOUT).join().getLength(), 40);

        //Validating the data append data
        WireCommands.SegmentRead result = (WireCommands.SegmentRead) sendRequest(channel, new WireCommands.ReadSegment(segment, 0, 20, "", 1L));
        ByteBuf resultAfterOneAppend = result.getData();
        assertEquals("Hello world", resultAfterOneAppend.toString(Charset.defaultCharset()).trim());

        // Truncate one event
        final long truncateOffset = 20;

        //Before truncation validation
        assertEquals(0, store.getStreamSegmentInfo(segment, PravegaRequestProcessor.TIMEOUT).join().getStartOffset());
        assertEquals(0, store.getStreamSegmentInfo(indexSegment, PravegaRequestProcessor.TIMEOUT).join().getStartOffset());

        //After truncation validation
        sendRequest(channel, new WireCommands.TruncateSegment(requestId, segment, truncateOffset, ""));

        assertEquals(truncateOffset, store.getStreamSegmentInfo(segment, PravegaRequestProcessor.TIMEOUT).join().getStartOffset());
        assertEquals(0, store.getStreamSegmentInfo(indexSegment, PravegaRequestProcessor.TIMEOUT).join().getStartOffset());

        // Truncate at the same offset - verify idempotence.
        sendRequest(channel, new WireCommands.TruncateSegment(requestId, segment, truncateOffset, ""));
        assertEquals(truncateOffset, store.getStreamSegmentInfo(segment, PravegaRequestProcessor.TIMEOUT).join().getStartOffset());
        assertEquals(0, store.getStreamSegmentInfo(indexSegment, PravegaRequestProcessor.TIMEOUT).join().getStartOffset());

        //Deleting the main segment and validating that index segment has also deleted
        sendRequest(channel, new WireCommands.DeleteSegment(1, segment, ""));
        assertThrows(StreamSegmentNotExistsException.class, () -> store.getStreamSegmentInfo(segment, PravegaRequestProcessor.TIMEOUT).join());
        assertThrows(StreamSegmentNotExistsException.class, () -> store.getStreamSegmentInfo(indexSegment, PravegaRequestProcessor.TIMEOUT).join());
    }

    @Test(timeout = 20000)
    public void testTruncateIndexSegmentWithNegativeOffset() throws Exception {
        String segment = "testTruncateIndexSegmentWithNegativeOffset/testStream/0";
        String indexSegment = getIndexSegmentName(segment);
        ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());

        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        EmbeddedChannel channel = createChannel(store);
        WireCommands.SegmentCreated created = (WireCommands.SegmentCreated) sendRequest(channel, new WireCommands.CreateSegment(1, segment, WireCommands.CreateSegment.NO_SCALE, 0, "", 1024L));
        assertEquals(segment, created.getSegment());

        UUID uuid = UUID.randomUUID();
        WireCommands.AppendSetup setup = (WireCommands.AppendSetup) sendRequest(channel, new WireCommands.SetupAppend(2, uuid, segment, ""));
        assertEquals(segment, setup.getSegment());
        assertEquals(indexSegment, store.getStreamSegmentInfo(indexSegment, PravegaRequestProcessor.TIMEOUT).join().getName());
        data.retain();

        //Append 40 bytes of data
        sendRequest(channel, new Append(segment, uuid, 1, new WireCommands.Event(data), 1L));
        sendRequest(channel, new Append(segment, uuid, 2, new WireCommands.Event(data), 1L));

        final long truncateOffset = -5000;
        AssertExtensions.assertLessThan("Negative offset", 0, truncateOffset);

        IllegalStateException exception = Assert.assertThrows(IllegalStateException.class, () ->
                sendRequest(channel, new WireCommands.TruncateSegment(requestId, segment, truncateOffset, "")));
        assertTrue(exception.getMessage().contains("No reply to request"));
    }

    @Test(timeout = 10000)
    public void testTruncateIndexSegmentWithGreaterThenEndOffset() throws Exception {
        String segment = "testTruncateIndexSegmentWithGreaterThenEndOffset/testStream/0";
        String indexSegment = getIndexSegmentName(segment);
        ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());

        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        EmbeddedChannel channel = createChannel(store);
        WireCommands.SegmentCreated created = (WireCommands.SegmentCreated) sendRequest(channel, new WireCommands.CreateSegment(1, segment, WireCommands.CreateSegment.NO_SCALE, 0, "", 1024L));
        assertEquals(segment, created.getSegment());

        UUID uuid = UUID.randomUUID();
        WireCommands.AppendSetup setup = (WireCommands.AppendSetup) sendRequest(channel, new WireCommands.SetupAppend(2, uuid, segment, ""));
        assertEquals(segment, setup.getSegment());
        assertEquals(indexSegment, store.getStreamSegmentInfo(indexSegment, PravegaRequestProcessor.TIMEOUT).join().getName());
        data.retain();

        //Append 40 bytes of data
        sendRequest(channel, new Append(segment, uuid, 1, new WireCommands.Event(data), 1L));
        sendRequest(channel, new Append(segment, uuid, 2, new WireCommands.Event(data), 1L));

        final long truncateOffset = 100;
        AssertExtensions.assertGreaterThan("Larger than endOffset", store.getStreamSegmentInfo(segment, PravegaRequestProcessor.TIMEOUT).join().getLength(), truncateOffset);
        Reply reply = sendRequest(channel, new WireCommands.TruncateSegment(requestId, segment, truncateOffset, ""));

        //since the requested offset is larger than the last valid offset, hence BadOffSetException is thrown, which is handled as a wirecommand response 'SegmentIsTruncated
        assertTrue(reply instanceof WireCommands.SegmentIsTruncated);

        assertEquals(0, store.getStreamSegmentInfo(segment, PravegaRequestProcessor.TIMEOUT).join().getStartOffset());
        assertEquals(0, store.getStreamSegmentInfo(indexSegment, PravegaRequestProcessor.TIMEOUT).join().getStartOffset());
    }

    @Test(timeout = 10000)
    public void testTruncateIndexSegmentThrowStreamSegmentNotExistsException() throws Exception {
        String segmentName = "truncateSegment/stream/0";
        String indexSegmentName = getIndexSegmentName(segmentName);
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        @Cleanup
        EmbeddedChannel channel = createChannel(store);

        Duration timeout = Duration.ofMinutes(1);
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();

        SegmentProperties segmentProperties = StreamSegmentInformation.builder()
                                                                      .name(indexSegmentName)
                                                                      .length(0)
                                                                      .startOffset(0)
                                                                      .attributes(Map.of(EVENT_COUNT, 30L))
                                                                      .build();

        doReturn(CompletableFuture.failedFuture(new StreamSegmentNotExistsException("Segment does not exits")))
                .when(store).getStreamSegmentInfo(indexSegmentName, timeout);
        doReturn(CompletableFuture.completedFuture(null)).when(store).truncateStreamSegment(anyString(), anyLong(), any());

        Reply reply = sendRequest(channel, new WireCommands.TruncateSegment(requestId, segmentName, 30L, ""));
        assertTrue(reply instanceof WireCommands.SegmentTruncated);

        doReturn(CompletableFuture.completedFuture(segmentProperties)).when(store).getStreamSegmentInfo(any(), any());
        reply = sendRequest(channel, new WireCommands.TruncateSegment(requestId, segmentName, 30L, ""));
        assertTrue(reply instanceof WireCommands.SegmentTruncated);

        doReturn(CompletableFuture.failedFuture(new TokenExpiredException("Token expired"))).when(store).getStreamSegmentInfo(any(), any());
        reply = sendRequest(channel, new WireCommands.TruncateSegment(requestId, segmentName, 30L, ""));
        assertTrue(reply instanceof WireCommands.AuthTokenCheckFailed);
    }

    private ArrayView generateData(int length, Random rnd) {
        byte[] keyData = new byte[length];
        rnd.nextBytes(keyData);
        return new ByteArraySegment(keyData);
    }

    private WireCommands.TableEntries getTableEntries(List<TableEntry> updateData) {

        List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> entries = updateData.stream().map(te -> {
            if (te == null) {
                return new AbstractMap.SimpleImmutableEntry<>(WireCommands.TableKey.EMPTY, WireCommands.TableValue.EMPTY);
            } else {
                val tableKey = new WireCommands.TableKey(toByteBuf(te.getKey().getKey()), te.getKey().getVersion());
                val tableValue = new WireCommands.TableValue(wrappedBuffer(te.getValue().getCopy()));
                return new AbstractMap.SimpleImmutableEntry<>(tableKey, tableValue);
            }
        }).collect(toList());

        return new WireCommands.TableEntries(entries);
    }

    private ArrayView generateValue(Random rnd) {
        return generateData(MAX_VALUE_LENGTH, rnd);
    }

    private ArrayList<ArrayView> generateKeys(int keyCount, Random rnd) {
        val result = new ArrayList<ArrayView>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            result.add(generateData(MAX_KEY_LENGTH, rnd));
        }

        return result;
    }

    private void verifyConnectionTracker(WireCommands.TableKey k, ServerConnection connection, ConnectionTracker tracker) {
        verifyConnectionTracker(k.getData().readableBytes(), connection, tracker);
    }

    private void verifyConnectionTracker(TableEntry e, ServerConnection connection, ConnectionTracker tracker) {
        verifyConnectionTracker(e.getKey().getKey().getLength() + e.getValue().getLength(), connection, tracker);
    }

    private void verifyConnectionTracker(int len, ServerConnection connection, ConnectionTracker tracker) {
        verify(tracker).updateOutstandingBytes(connection, len, len);
        verify(tracker).updateOutstandingBytes(connection, -len, 0);
        clearInvocations(tracker);
    }

    private static Reply sendRequest(EmbeddedChannel channel, Request request) throws Exception {
        channel.writeInbound(request);
        log.info("Request {} sent to Segment store", request);
        Object encodedReply = channel.readOutbound();
        for (int i = 0; encodedReply == null && i < 500; i++) {
            channel.runPendingTasks();
            Thread.sleep(10);
            encodedReply = channel.readOutbound();
        }
        if (encodedReply == null) {
            log.error("Error while try waiting for a response from Segment Store");
            throw new IllegalStateException("No reply to request: " + request);
        }
        WireCommand decoded = CommandDecoder.parseCommand((ByteBuf) encodedReply);
        ((ByteBuf) encodedReply).release();
        assertNotNull(decoded);
        return (Reply) decoded;
    }

    static EmbeddedChannel createChannel(StreamSegmentStore store) {
        ServerConnectionInboundHandler lsh = new ServerConnectionInboundHandler();
        EmbeddedChannel channel = new EmbeddedChannel(new ExceptionLoggingHandler(""),
                new CommandEncoder(null, MetricNotifier.NO_OP_METRIC_NOTIFIER),
                new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
                new CommandDecoder(),
                new AppendDecoder(),
                lsh);
        @Cleanup
        InlineExecutor indexExecutor = new InlineExecutor();
        lsh.setRequestProcessor(AppendProcessor.defaultBuilder(new IndexAppendProcessor(indexExecutor, store))
                .store(store)
                .connection(new TrackedConnection(lsh))
                .nextRequestProcessor(new PravegaRequestProcessor(store, mock(TableStore.class), lsh,
                        new IndexAppendProcessor(indexExecutor, store)))
                .build());
        return channel;
    }

    //endregion

    //region Other Helpers

    private boolean append(String streamSegmentName, int number, StreamSegmentStore store) {
        return Futures.await(store.append(streamSegmentName,
                new ByteBufWrapper(Unpooled.wrappedBuffer(new byte[]{(byte) number})),
                null,
                PravegaRequestProcessor.TIMEOUT));
    }

    static ServiceBuilderConfig getBuilderConfig() {
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

    static ServiceBuilder newInlineExecutionInMemoryBuilder(ServiceBuilderConfig config) {
        return ServiceBuilder.newInMemoryBuilder(config, (size, name, threadPriority) -> new InlineExecutor())
                .withStreamSegmentStore(setup -> new SynchronousStreamSegmentStore(new StreamSegmentService(
                        setup.getContainerRegistry(), setup.getSegmentToContainerMapper())));
    }

    private ByteBuf toByteBuf(BufferView bufferView) {
        val iterators = Iterators.transform(bufferView.iterateBuffers(), Unpooled::wrappedBuffer);
        return Unpooled.wrappedUnmodifiableBuffer(Iterators.toArray(iterators, ByteBuf.class));
    }

    //endregion
}
