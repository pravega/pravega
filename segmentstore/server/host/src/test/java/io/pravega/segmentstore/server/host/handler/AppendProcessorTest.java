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

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.pravega.auth.TokenExpiredException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.TokenVerifierImpl;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.shared.NameUtils;
import io.pravega.shared.metrics.MetricNotifier;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.CreateTransientSegment;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.InvalidEventNumber;
import io.pravega.shared.protocol.netty.WireCommands.OperationUnsupported;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.security.token.JsonWebToken;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.IntentionalException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static io.pravega.test.common.AssertExtensions.assertEventuallyEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AppendProcessorTest {
    private static final long TIMEOUT_SEC = 8;
    private static final ScheduledExecutorService INLINE_EXECUTOR = new InlineExecutor();

    private final long requestId = 1234L;
    @Rule
    public final Timeout globalTimeout = Timeout.seconds(TIMEOUT_SEC);
    
    
    @AfterClass
    public static void tearDown() {
        INLINE_EXECUTOR.shutdown();
    }

    @Test
    public void testKeepAlive() {
        ServerConnection connection = mock(ServerConnection.class);
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                .store(store)
                .connection(new TrackedConnection(connection, mock(ConnectionTracker.class)))
                .statsRecorder(Mockito.mock(SegmentStatsRecorder.class))
                .build();

        WireCommands.KeepAlive keepAliveCommand = new WireCommands.KeepAlive();
        processor.keepAlive(keepAliveCommand);
        verify(connection).send(new WireCommands.KeepAlive());
    }

    @Test
    public void testAppend() throws InterruptedException {
        String streamSegmentName = "scope/stream/0.#epoch.0";
        String streamIndexSegmentName = "scope/stream/0.#epoch.0#index";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                                                   .store(store)
                                                   .connection(new TrackedConnection(connection, tracker))
                                                   .statsRecorder(mockedRecorder)
                                                   .build();

        setupGetAttributes(streamSegmentName, clientId, store);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), CompletableFuture.completedFuture((long) data.length));

        SetupAppend setupAppendCommand = new SetupAppend(1, clientId, streamSegmentName, "");
        processor.setupAppend(setupAppendCommand);
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(tracker).updateOutstandingBytes(connection, data.length, data.length);
        verify(connection).send(new DataAppended(requestId, clientId, data.length, 0L, data.length));
        verify(tracker).updateOutstandingBytes(connection, -data.length, 0);
        indexAppendProcessor.runRemainingAndClose();
        verify(store, atLeast(1)).getStreamSegmentInfo(anyString(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).getAttributes(eq(streamIndexSegmentName), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, atLeast(1)).append(eq(streamIndexSegmentName), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
        verify(mockedRecorder).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
        assertTrue(processor.isSetupAppendCompleted(setupAppendCommand.getSegment(), setupAppendCommand.getWriterId()));
    }

    @Test
    public void testAppendIndex() throws InterruptedException {
        String streamSegmentName = "scope/stream/0.#epoch.0";
        String streamIndexSegmentName = "scope/stream/0.#epoch.0#index";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        IndexEntry indexEntry1 = new IndexEntry(data.length, 1, 0);
        byte[] iaDataBytes = indexEntry1.toBytes().getCopy();
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                                                   .store(store)
                                                   .connection(new TrackedConnection(connection, tracker))
                                                   .statsRecorder(mockedRecorder)
                                                   .build();
        
        setupGetAttributes(streamSegmentName, clientId, store);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);

        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), CompletableFuture.completedFuture((long) data.length));
        val acIndex = interceptAppend(store, streamIndexSegmentName, updateIndexEventNumber(1), CompletableFuture.completedFuture((long) iaDataBytes.length));

        SetupAppend setupAppendCommand = new SetupAppend(1, clientId, streamSegmentName, "");
        processor.setupAppend(setupAppendCommand);
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(tracker).updateOutstandingBytes(connection, data.length, data.length);
        verify(connection).send(new DataAppended(requestId, clientId, data.length, 0L, data.length));
        verify(tracker).updateOutstandingBytes(connection, -data.length, 0);
        indexAppendProcessor.runRemainingAndClose();
        // Assert IndexSegment data
        IndexEntry indexEntry = IndexEntry.fromBytes(BufferView.wrap(acIndex.appendedData.get()));
        assertEquals(indexEntry1.getOffset(), indexEntry.getOffset());
        assertEquals(indexEntry1.getEventCount(), indexEntry.getEventCount());
        verify(store, times(1)).getStreamSegmentInfo(anyString(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(2)).append(anyString(), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
        verify(mockedRecorder).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
        assertTrue(processor.isSetupAppendCompleted(setupAppendCommand.getSegment(), setupAppendCommand.getWriterId()));
    }

    private AttributeUpdateCollection updateIndexEventNumber(long eventNum) {
        return AttributeUpdateCollection.from(
                new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.ReplaceIfGreater, eventNum));
    }

    private void setMockForIndexSegmentAppends(String streamSegmentName, StreamSegmentStore store, long eventCount, long eventLength) {
        CompletableFuture<SegmentProperties> info = CompletableFuture.completedFuture(
            StreamSegmentInformation.builder()
                    .name(streamSegmentName)
                    .length(eventLength)
                    .attributes(ImmutableMap.<AttributeId, Long>builder()
                            .put(Attributes.SCALE_POLICY_TYPE, 0L)
                            .put(EVENT_COUNT, eventCount)
                            .put(Attributes.SCALE_POLICY_RATE, 10L).build())
                    .build());
        when(store.getStreamSegmentInfo(eq(streamSegmentName), any())).thenReturn(info);
        CompletableFuture<Map<AttributeId, Long>> attributes = CompletableFuture.completedFuture(ImmutableMap.<AttributeId, Long>builder()
                                .put(Attributes.EXPECTED_INDEX_SEGMENT_EVENT_SIZE, (long) NameUtils.INDEX_APPEND_EVENT_SIZE).build());
        when(store.getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), eq(List.of(Attributes.EXPECTED_INDEX_SEGMENT_EVENT_SIZE)), anyBoolean(), any())).thenReturn(attributes);
    }

    @Test
    public void testCreateTransientSegment() {

        String parentSegmentName = "scope/stream/parentSegment";
        UUID writerId = UUID.randomUUID();
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                .store(store)
                .connection(new TrackedConnection(connection, tracker))
                .statsRecorder(mockedRecorder)
                .build();

        interceptCreateTransient(store, parentSegmentName);
        CreateTransientSegment createTransientSegment = new CreateTransientSegment(1, writerId, parentSegmentName, "");
        processor.createTransientSegment(createTransientSegment);

        verify(connection).send(new WireCommands.SegmentCreated(1, any()));

        verifyNoMoreInteractions(connection);
    }

    // Verifies that the AppendProcessor performs a clean-up call on each Transient Segment it is tracking.
    @Test
    public void testTransientSegmentCleanupOnClose() {

        String parentSegmentName = "scope/stream/parentSegment";
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                .store(store)
                .connection(new TrackedConnection(connection, tracker))
                .statsRecorder(mockedRecorder)
                .build();
        interceptCreateTransient(store, parentSegmentName);

        int transientSegments = 10;
        for (int i = 1; i <= transientSegments; i++) {
            UUID writerId = new UUID(0, i);
            CreateTransientSegment createTransientSegment = new CreateTransientSegment(i, writerId, parentSegmentName, "");
            processor.createTransientSegment(createTransientSegment);
        }
        verify(connection, times(transientSegments)).send(any(WireCommands.SegmentCreated.class));
        processor.close();
        // For each Transient Segment we created, make sure that the StreamSegmentStore calls deleteStreamSegment on them.
        verify(store, times(transientSegments)).deleteStreamSegment(any(), any());
        // Make sure the connection has also been properly closed.
        verify(connection).close();
    }

    @Test
    public void testSetupAppendClosesConnectionIfTokenHasExpired() {
        String streamSegmentName = "scope/stream/0.#epoch.0";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                .store(store)
                .connection(new TrackedConnection(connection, tracker))
                .statsRecorder(mockedRecorder)
                .tokenVerifier(new TokenVerifierImpl("secret"))
                .build();

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), CompletableFuture.completedFuture((long) data.length));

        Date expiryDate = Date.from(Instant.now().minusSeconds(100));
        JsonWebToken token = new JsonWebToken("subject", "audience", "secret".getBytes(), expiryDate, null);

        SetupAppend setupAppend = new SetupAppend(1, clientId, streamSegmentName, token.toCompactString());
        processor.setupAppend(setupAppend);
        verify(connection).close();
    }

    @Test
    public void testSetupTokenExpiryTaskClosesConnectionIfTokenHasExpired() {
        // Arrange
        String streamSegmentName = "scope/stream/0.#epoch.0";
        UUID clientId = UUID.randomUUID();

        StreamSegmentStore mockStore = mock(StreamSegmentStore.class);
        ServerConnection mockConnection = mock(ServerConnection.class);

        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(mockStore);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                .store(mockStore)
                .connection(new TrackedConnection(mockConnection))
                .tokenExpiryHandlerExecutor(INLINE_EXECUTOR)
                .build();

        // Spy the actual Append Processor, so that we can have some of the methods return stubbed values.
        AppendProcessor mockProcessor = spy(processor);
        doReturn(true).when(mockProcessor).isSetupAppendCompleted(streamSegmentName, clientId);

        JsonWebToken token = new JsonWebToken("subject", "audience", "secret".getBytes(),
                Date.from(Instant.now().minusSeconds(5)), null);
        SetupAppend setupAppend = new SetupAppend(1, clientId, streamSegmentName, token.toCompactString());

        // Act
        mockProcessor.setupTokenExpiryTask(setupAppend, token).join();

        // Assert
        verify(mockConnection).close();
    }

    @Test
    public void testSetupTokenExpiryWhenConnectionSendThrowsException() {
        // Arrange
        String streamSegmentName = "scope/stream/0.#epoch.0";
        UUID clientId = UUID.randomUUID();

        StreamSegmentStore mockStore = mock(StreamSegmentStore.class);
        ServerConnection mockConnection = mock(ServerConnection.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(mockStore);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                .store(mockStore)
                .connection(new TrackedConnection(mockConnection))
                .tokenExpiryHandlerExecutor(INLINE_EXECUTOR)
                .build();

        // Spy the actual Append Processor, so that we can have some of the methods return stubbed values.
        AppendProcessor mockProcessor = spy(processor);
        doReturn(true).when(mockProcessor).isSetupAppendCompleted(streamSegmentName, clientId);
        doThrow(new RuntimeException()).when(mockConnection).send(any());

        Date expiryDate = Date.from(Instant.now().plusMillis(300));
        JsonWebToken token = new JsonWebToken("subject", "audience", "secret".getBytes(), expiryDate, null);

        SetupAppend setupAppend = new SetupAppend(1, clientId, streamSegmentName, token.toCompactString());

        // Act
        mockProcessor.setupTokenExpiryTask(setupAppend, token).join();
    }

    @Test
    public void testTransactionAppend() {
        String streamSegmentName = "scope/stream/transactionSegment#transaction.01234567890123456789012345678901";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                                                   .store(store)
                                                   .connection(new TrackedConnection(connection, tracker))
                                                   .statsRecorder(mockedRecorder)
                                                   .build();

        setupGetAttributes(streamSegmentName, clientId, store);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), CompletableFuture.completedFuture(21L));
        processor.setupAppend(new SetupAppend(requestId, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac, data);
        verify(connection).send(new AppendSetup(requestId, streamSegmentName, clientId, 0));
        verify(tracker).updateOutstandingBytes(connection, data.length, data.length);
        verify(connection).send(new DataAppended(requestId, clientId, data.length, 0L, 21L));
        verify(tracker).updateOutstandingBytes(connection, -data.length, 0);
        verify(store, times(0)).getStreamSegmentInfo(anyString(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).append(anyString(), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);

        verify(mockedRecorder).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test
    public void testSwitchingSegment() {
        String streamSegmentName1 = "scope/stream/testAppendSegment1";
        String streamSegmentName2 = "scope/stream/testAppendSegment2";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        InOrder verifier = Mockito.inOrder(store);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(connection, tracker)).build();

        setupGetAttributes(streamSegmentName1, clientId, store);

        setMockForIndexSegmentAppends(streamSegmentName1, store, 1L, data.length);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName1, ""));
        verifier.verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));

        val ac1 = interceptAppend(store, streamSegmentName1, updateEventNumber(clientId, 10), CompletableFuture.completedFuture(1L));
        processor.append(new Append(streamSegmentName1, clientId, 10, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(verifier, ac1, data);

        setupGetAttributes(streamSegmentName2, clientId, store);
        setMockForIndexSegmentAppends(streamSegmentName2, store, 1L, data.length);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName2, ""));
        verifier.verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));

        val ac2 = interceptAppend(store, streamSegmentName2, updateEventNumber(clientId, 2000), CompletableFuture.completedFuture(2L));
        processor.append(new Append(streamSegmentName2, clientId, 2000, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(verifier, ac2, data);

        val ac3 = interceptAppend(store, streamSegmentName1, updateEventNumber(clientId, 20, 10, 1), CompletableFuture.completedFuture(3L));
        processor.append(new Append(streamSegmentName1, clientId, 20, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(verifier, ac3, data);
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName1)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName2)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, atLeast(2)).append(anyString(), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testConditionalAppendSuccess() throws InterruptedException {
        String streamSegmentName = "scope/stream/testConditionalAppendSuccess";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                                                   .store(store)
                                                   .connection(new TrackedConnection(connection, tracker))
                                                   .statsRecorder(mockedRecorder)
                                                   .build();

        setupGetAttributes(streamSegmentName, clientId, store);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        val ac1 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 1), CompletableFuture.completedFuture((long) data.length));
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data), null, requestId));

        val ac2 = interceptAppend(store, streamSegmentName, data.length, updateEventNumber(clientId, 2, 1, 1), CompletableFuture.completedFuture(Long.valueOf(2 * data.length)));
        processor.append(new Append(streamSegmentName, clientId, 2, 1, Unpooled.wrappedBuffer(data), (long) data.length, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac1, data);
        verifyStoreAppend(ac2, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(tracker, times(2)).updateOutstandingBytes(connection, data.length, data.length);
        verify(connection).send(new DataAppended(requestId, clientId, 1, 0, data.length));
        verify(connection).send(new DataAppended(requestId, clientId, 2, 1, 2 * data.length));
        verify(tracker, times(2)).updateOutstandingBytes(connection, -data.length, 0);

        indexAppendProcessor.runRemainingAndClose();
        verify(store, atLeast(1)).getStreamSegmentInfo(anyString(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, atLeast(1)).append(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(store);
        verify(mockedRecorder, times(2)).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test
    public void testConditionalAppendFailure() throws InterruptedException {
        String streamSegmentName = "scope/stream/testConditionalAppendFailure";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                                                   .store(store)
                                                   .connection(new TrackedConnection(connection, tracker))
                                                   .statsRecorder(mockedRecorder)
                                                   .build();

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac1 = interceptAppend(store, streamSegmentName, 0, updateEventNumber(clientId, 1), CompletableFuture.completedFuture((long) data.length));
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data), 0L, requestId));

        val ac2 = interceptAppend(store, streamSegmentName, 0, updateEventNumber(clientId, 2, 1, 1),
                Futures.failedFuture(new BadOffsetException(streamSegmentName, data.length, 0)));

        processor.append(new Append(streamSegmentName, clientId, 2, 1, Unpooled.wrappedBuffer(data), 0L, requestId));
        val ac3 = interceptAppend(store, streamSegmentName, 0, updateEventNumber(clientId, 3, 1, 1),
                                  Futures.failedFuture(new BadAttributeUpdateException(streamSegmentName, new AttributeUpdate(AttributeId.fromUUID(clientId), null, 0), true, "test")));

        processor.append(new Append(streamSegmentName, clientId, 3, 1, Unpooled.wrappedBuffer(data), 0L, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac1, data);
        verifyStoreAppend(ac2, data);
        verifyStoreAppend(ac3, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(tracker, times(3)).updateOutstandingBytes(connection, data.length, data.length);
        verify(connection).send(new DataAppended(requestId, clientId, 1, 0, data.length));
        verify(connection).send(new ConditionalCheckFailed(clientId, 2, requestId));
        verify(connection).send(new InvalidEventNumber(clientId, requestId, "test", 3));
        verify(connection).close();
        verify(tracker, times(3)).updateOutstandingBytes(connection, -data.length, 0);
        indexAppendProcessor.runRemainingAndClose();
        verify(store, atLeast(1)).getStreamSegmentInfo(anyString(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, atLeast(1)).append(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
        verify(mockedRecorder).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test
    public void testMultipleSetupAppendDueToConnectionFailure() throws InterruptedException {
        String streamSegmentName = "scope/stream/testMultipleSetupAppend";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3, 4, 6, 7, 8, 9};
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                .store(store)
                .connection(new TrackedConnection(connection, tracker))
                .statsRecorder(mockedRecorder)
                .build();
        // Setup mock to enusure both the SetupAppends return the newer values.
        val clientIdAttribute = AttributeId.fromUUID(clientId);
        when(store.getAttributes(streamSegmentName, Collections.singleton(clientIdAttribute), true, AppendProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(clientIdAttribute, 0L)))
                .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(clientIdAttribute, 1L)));

        val ac1 = interceptAppend(store, streamSegmentName, 0, updateEventNumber(clientId, 1), CompletableFuture.completedFuture((long) data.length));
        // First conditional Append.
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        setMockForIndexSegmentAppends(streamSegmentName, store, 1, data.length);
        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data), 0L, 1L));

        //Simulate a connection drop on the client side and the client resends a SetupAppend followed by conditional Append.
        val ac2 = interceptAppend(store, streamSegmentName, data.length, updateEventNumber(clientId, 2, 1, 1),
                CompletableFuture.completedFuture((long) data.length * 2));
        processor.setupAppend(new SetupAppend(2, clientId, streamSegmentName, ""));
        setMockForIndexSegmentAppends(streamSegmentName, store, 2, data.length);
        processor.append(new Append(streamSegmentName, clientId, 2, 1, Unpooled.wrappedBuffer(data), (long) data.length, 2));

        verify(store, times(2)).getAttributes(anyString(), eq(Collections.singleton(clientIdAttribute)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac1, data);
        verifyStoreAppend(ac2, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(tracker, times(2)).updateOutstandingBytes(connection, data.length, data.length);
        verify(connection).send(new DataAppended(1, clientId, 1, 0, data.length));
        // verify that the second SetupAppend is responded by the AppendProcessor.
        verify(connection).send(new AppendSetup(2, streamSegmentName, clientId, 1));
        verify(connection).send(new DataAppended(2, clientId, 2, 1, data.length * 2));
        indexAppendProcessor.runRemainingAndClose();
        verifyNoMoreInteractions(connection);
        verify(store, atLeast(1)).getStreamSegmentInfo(anyString(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(2)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, atLeast(1)).append(anyString(), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(store);
        verify(mockedRecorder, times(2)).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test
    public void testConditionalAppendFailureOnUnconditionalAppend() throws InterruptedException {
        String streamSegmentName = "scope/stream/testConditionalAppendFailureOnUnconditionalAppend";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                                                   .store(store)
                                                   .connection(new TrackedConnection(connection, tracker))
                                                   .statsRecorder(mockedRecorder)
                                                   .build();

        setupGetAttributes(streamSegmentName, clientId, store);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        val ac1 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 1), CompletableFuture.completedFuture((long) data.length));

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data), null, requestId));

        val ac2 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 1, 1, 1),
                Futures.failedFuture(new BadOffsetException(streamSegmentName, data.length, 0)));

        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac1, data);
        verifyStoreAppend(ac2, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(tracker, times(2)).updateOutstandingBytes(connection, data.length, data.length);
        verify(connection).send(new DataAppended(requestId, clientId, 1, 0, data.length));
        verify(connection).close();
        indexAppendProcessor.runRemainingAndClose();
        verify(tracker, times(2)).updateOutstandingBytes(connection, -data.length, 0);
        verify(store, atLeast(1)).getStreamSegmentInfo(anyString(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, atLeast(1)).append(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testInvalidOffset() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(connection, tracker)).build();

        setupGetAttributes(streamSegmentName, clientId, 100, store);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
            fail();
        } catch (IllegalStateException e) {
            //expected
        }
        verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 100));
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testSetupSkipped() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(connection, tracker)).build();
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
            fail();
        } catch (IllegalStateException e) {
            //expected
        }
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    private IndexAppendProcessor getIndexAppendProcessor(StreamSegmentStore store) {
        return new IndexAppendProcessor(INLINE_EXECUTOR, store, Duration.ofSeconds(TIMEOUT_SEC));
    }

    @Test
    public void testSwitchingStream() {
        String segment1 = "scope/stream/segment1";
        String segment2 = "scope/stream/segment2";
        UUID clientId1 = UUID.randomUUID();
        UUID clientId2 = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(connection, tracker)).build();

        setupGetAttributes(segment1, clientId1, store);
        val ac1 = interceptAppend(store, segment1, updateEventNumber(clientId1, data.length), CompletableFuture.completedFuture((long) data.length));

        setupGetAttributes(segment2, clientId2, store);
        val ac2 = interceptAppend(store, segment2, updateEventNumber(clientId2, data.length), CompletableFuture.completedFuture((long) data.length));

        setMockForIndexSegmentAppends(segment1, store, 1L, data.length);
        processor.setupAppend(new SetupAppend(1, clientId1, segment1, ""));
        processor.append(new Append(segment1, clientId1, data.length, 1, Unpooled.wrappedBuffer(data), null, 2));
        setMockForIndexSegmentAppends(segment2, store, 1L, data.length);
        processor.setupAppend(new SetupAppend(3, clientId2, segment2, ""));
        processor.append(new Append(segment2, clientId2, data.length, 1, Unpooled.wrappedBuffer(data), null, 4));

        verify(store).getAttributes(eq(segment1), eq(Collections.singleton(AttributeId.fromUUID(clientId1))), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac1, data);

        verify(store).getAttributes(eq(segment2), eq(Collections.singleton(AttributeId.fromUUID(clientId2))), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac2, data);
        verify(tracker, times(2)).updateOutstandingBytes(connection, data.length, data.length);
        verify(connection).send(new AppendSetup(1, segment1, clientId1, 0));
        verify(connection).send(new DataAppended(2, clientId1, data.length, 0, data.length));
        verify(connection).send(new AppendSetup(3, segment2, clientId2, 0));
        verify(connection).send(new DataAppended(4, clientId2, data.length, 0, data.length));
        verify(tracker, times(2)).updateOutstandingBytes(connection, -data.length, 0);
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(segment1)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(segment2)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, atLeast(2)).append(anyString(), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testAppendFails() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = mock(SegmentStatsRecorder.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                                                   .store(store)
                                                   .connection(new TrackedConnection(connection, tracker))
                                                   .statsRecorder(mockedRecorder)
                                                   .build();

        setupGetAttributes(streamSegmentName, clientId, store);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), Futures.failedFuture(new IntentionalException()));

        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length * 2, 1, Unpooled.wrappedBuffer(data), null, requestId));
            fail();
        } catch (IllegalStateException e) {
            // Expected
        }
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(tracker).updateOutstandingBytes(connection, data.length, data.length);
        verify(connection).close();
        verify(tracker).updateOutstandingBytes(connection, -data.length, 0);

        verify(store, atMost(2)).append(any(), any(), any(), any());
        verifyNoMoreInteractions(connection);

        verify(mockedRecorder, never()).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test(timeout = 5000)
    public void testAppendFailChannelClose() throws Exception {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3, 4, 6, 7, 8, 9};

        // Setup mocks.
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        setupGetAttributes(streamSegmentName, clientId, store);

        interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), Futures.failedFuture(new IntentionalException()));

        @Cleanup
        EmbeddedChannel channel = createChannel(store, indexAppendProcessor);

        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        // Send a setup append WireCommand.
        Reply reply = sendRequest(channel, new SetupAppend(requestId, clientId, streamSegmentName, ""));
        assertEquals(new AppendSetup(requestId, streamSegmentName, clientId, 0), reply);

        // Send an append which will cause a RuntimeException to be thrown by the store.
        reply = sendRequest(channel, new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null,
                                                requestId));
        assertNull("No WireCommand reply is expected", reply);
        // Verify that the channel is closed by the AppendProcessor.
        assertEventuallyEquals(false, channel::isOpen, 3000);
    }

    @Test(timeout = 5000)
    public void testBadAttributeException() throws Exception {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3, 4, 6, 7, 8, 9};

        // Setup mocks.
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        setupGetAttributes(streamSegmentName, clientId, store);
        val ex = new BadAttributeUpdateException(streamSegmentName,
                new AttributeUpdate(AttributeId.randomUUID(), AttributeUpdateType.ReplaceIfEquals, 100, 101),
                false, "error");
        interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), Futures.failedFuture(ex));

        @Cleanup
        EmbeddedChannel channel = createChannel(store, indexAppendProcessor);

        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        // Send a setup append WireCommand.
        Reply reply = sendRequest(channel, new SetupAppend(requestId, clientId, streamSegmentName, ""));
        assertEquals(new AppendSetup(requestId, streamSegmentName, clientId, 0), reply);

        // Send an append which will cause a RuntimeException to be thrown by the store.
        reply = sendRequest(channel, new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null,
                                                requestId));
        // validate InvalidEventNumber Wirecommand is sent before closing the Channel.
        assertNotNull("Invalid Event WireCommand is expected", reply);
        assertEquals(WireCommandType.INVALID_EVENT_NUMBER.getCode(), ((WireCommand) reply).getType().getCode());
        assertEquals(requestId, ((InvalidEventNumber) reply).getRequestId());

        // Verify that the channel is closed by the AppendProcessor.
        assertEventuallyEquals(false, channel::isOpen, 3000);
    }

    @Test
    public void testEventNumbers() throws InterruptedException {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(connection, tracker)).build();

        when(store.getAttributes(streamSegmentName, Collections.singleton(AttributeId.fromUUID(clientId)), true, AppendProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(Collections.emptyMap()));
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        verify(store).getAttributes(streamSegmentName, Collections.singleton(AttributeId.fromUUID(clientId)), true, AppendProcessor.TIMEOUT);

        int eventCount = 100;
        val ac1 = interceptAppend(store, streamSegmentName,
                updateEventNumber(clientId, 100, Attributes.NULL_ATTRIBUTE_VALUE, eventCount), CompletableFuture.completedFuture((long) data.length));
        processor.append(new Append(streamSegmentName, clientId, 100, eventCount, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(ac1, data);

        val ac2 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 200, 100, eventCount),
                CompletableFuture.completedFuture(null));
        processor.append(new Append(streamSegmentName, clientId, 200, eventCount, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(ac2, data);
        indexAppendProcessor.runRemainingAndClose();
        verify(store, atLeast(1)).getStreamSegmentInfo(anyString(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, atLeast(1)).append(anyString(), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(store);
    }

    /**
     * Verifies that appends are "pipelined" into the underlying store and that acks are sent as appropriate via the
     * connection when they complete.
     * @throws InterruptedException 
     */
    @Test(timeout = 15 * 1000)
    public void testAppendPipelining() throws InterruptedException {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data1 = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        byte[] data2 = new byte[]{1, 2, 3, 4, 5};
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(connection, tracker)).build();

        setupGetAttributes(streamSegmentName, clientId, store);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1, data1.length);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));

        // Initiate two appends in short sequence, and simulate the Store blocking on both of them.
        val store1 = new CompletableFuture<Long>();
        val ac1 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 1, 0, 1), store1);
        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data1), null, requestId));
        val store2 = new CompletableFuture<Long>();
        val ac2 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 2, 1, 1), store2);
        processor.append(new Append(streamSegmentName, clientId, 2, 1, Unpooled.wrappedBuffer(data2), null, requestId));
        verifyStoreAppend(ac1, data1);
        verifyStoreAppend(ac2, data2);
        verify(tracker).updateOutstandingBytes(connection, data1.length, data1.length);
        verify(tracker).updateOutstandingBytes(connection, data2.length, data1.length + data2.length);

        // Complete the second one (this simulates acks arriving out of order from the store).
        store2.complete(100L);

        // Verify an ack is sent for both appends (because of pipelining guarantees), but only the second append's length
        // is subtracted from the outstanding bytes.
        verify(connection).send(new DataAppended(requestId, clientId, 2, 0L, 100L));
        verify(tracker).updateOutstandingBytes(connection, -data2.length, data1.length);
        verifyNoMoreInteractions(connection);

        // Complete the first one, and verify that no additional acks are sent via the connection, but the first append's
        // length is subtracted from the outstanding bytes.
        store1.complete(50L);
        indexAppendProcessor.runRemainingAndClose();
        verify(tracker).updateOutstandingBytes(connection, -data1.length, 0);
        verify(store, atLeast(1)).getStreamSegmentInfo(anyString(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, atLeast(1)).append(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 15 * 1000)
    public void testAppendPipeliningWithSeal() throws InterruptedException {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data1 = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        byte[] data2 = new byte[]{1, 2, 3, 4, 5};
        byte[] data3 = new byte[]{1, 2, 3};
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(connection, tracker)).build();
        InOrder connectionVerifier = Mockito.inOrder(connection);

        setupGetAttributes(streamSegmentName, clientId, store);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1, data1.length);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));
        connectionVerifier.verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));

        // Initiate one append.
        val store1 = new CompletableFuture<Long>();
        val ac1 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 1, 0, 1), store1);
        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data1), null, requestId));
        verifyStoreAppend(ac1, data1);
        verify(tracker).updateOutstandingBytes(connection, data1.length, data1.length);

        // Second append will fail with StreamSegmentSealedException (this simulates having one append, immediately followed
        // by a Seal, then another append).
        val ac2 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 2, 1, 1), Futures.failedFuture(new StreamSegmentSealedException(streamSegmentName)));
        processor.append(new Append(streamSegmentName, clientId, 2, 1, Unpooled.wrappedBuffer(data2), null, requestId));
        verifyStoreAppend(ac2, data2);
        verify(tracker).updateOutstandingBytes(connection, data2.length, data1.length + data2.length);
        verify(tracker).updateOutstandingBytes(connection, -data2.length, data1.length);

        // Third append should fail just like the second one. However this will have a higher event number than that one
        // and we use it to verify it won't prevent sending the SegmentIsSealed message or send it multiple times.
        val ac3 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 3, 2, 1), Futures.failedFuture(new StreamSegmentSealedException(streamSegmentName)));
        processor.append(new Append(streamSegmentName, clientId, 3, 1, Unpooled.wrappedBuffer(data3), null, requestId));
        verifyStoreAppend(ac3, data3);
        verify(tracker).updateOutstandingBytes(connection, data3.length, data1.length + data3.length);

        // Complete the first one and verify it is acked properly.
        store1.complete(100L);
        connectionVerifier.verify(connection).send(new DataAppended(requestId, clientId, 1, 0L, 100L));

        // Verify that a SegmentIsSealed message is sent AFTER the ack from the first one (for the second append).
        connectionVerifier.verify(connection).send(new WireCommands.SegmentIsSealed(requestId, streamSegmentName, "", 2L));

        // Verify that a second SegmentIsSealed is sent (for the third append).
        connectionVerifier.verify(connection).send(new WireCommands.SegmentIsSealed(requestId, streamSegmentName, "", 3L));
        verify(tracker).updateOutstandingBytes(connection, -data3.length, data1.length);
        verify(tracker).updateOutstandingBytes(connection, -data1.length, 0);

        interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 4, 3, 1), Futures.failedFuture(new IntentionalException(streamSegmentName)));
        AssertExtensions.assertThrows(
                "append() accepted a new request after sending a SegmentIsSealed message.",
                () -> processor.append(new Append(streamSegmentName, clientId, 4, 1, Unpooled.wrappedBuffer(data1), null, requestId)),
                ex -> ex instanceof IllegalStateException);
        indexAppendProcessor.runRemainingAndClose();
        verify(store, atLeast(1)).getStreamSegmentInfo(anyString(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, atLeast(1)).append(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(),  any(), eq(AppendProcessor.TIMEOUT));
        // Verify no more messages are sent over the connection.
        connectionVerifier.verifyNoMoreInteractions();
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testEventNumbersOldClient() throws InterruptedException {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(connection, tracker)).build();

        when(store.getAttributes(streamSegmentName, Collections.singleton(AttributeId.fromUUID(clientId)), true, AppendProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(AttributeId.fromUUID(clientId), 100L)));
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        verify(store).getAttributes(streamSegmentName, Collections.singleton(AttributeId.fromUUID(clientId)), true, AppendProcessor.TIMEOUT);

        int eventCount = 10;
        val ac1 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 200, 100, eventCount),
                CompletableFuture.completedFuture((long) data.length));
        processor.append(new Append(streamSegmentName, clientId, 200, eventCount, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(ac1, data);

        val ac2 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 300, 200, eventCount),
                CompletableFuture.completedFuture((long) (2 * data.length)));
        processor.append(new Append(streamSegmentName, clientId, 300, eventCount, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(ac2, data);
        indexAppendProcessor.runRemainingAndClose();
        verify(store, atLeast(1)).getStreamSegmentInfo(anyString(), eq(AppendProcessor.TIMEOUT));
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verify(store, atLeast(1)).append(anyString(), any(),  any(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testUnsupportedOperation() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(getIndexAppendProcessor(store)).store(store).connection(new TrackedConnection(connection, tracker)).build();

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), Futures.failedFuture(new UnsupportedOperationException()));

        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac, data);

        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(tracker).updateOutstandingBytes(connection, data.length, data.length);
        verify(connection).send(new OperationUnsupported(requestId, "appending data", ""));
        verify(tracker).updateOutstandingBytes(connection, -data.length, 0);
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testCancellationException() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(connection, tracker)).build();

        setupGetAttributes(streamSegmentName, clientId, store);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1L, data.length);
        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), Futures.failedFuture(new CancellationException()));
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(AttributeId.fromUUID(clientId))), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac, data);

        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(tracker).updateOutstandingBytes(connection, data.length, data.length);
        verify(connection).close();
        verify(tracker).updateOutstandingBytes(connection, -data.length, 0);
        verify(store, times(1)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    /**
     * Simulates multiple connections being set up and all sending appends (conditional or unconditional). Some may be
     * failed by the store. Verifies that {@link ConnectionTracker#getTotalOutstanding()} does not drift with time,
     * regardless of append outcome.
     */
    @Test
    public void testOutstandingByteTracking() throws Exception {
        final int connectionCount = 5;
        final int writersCount = 5;
        final int segmentCount = 5;

        val tracker = new ConnectionTracker();
        val context = mock(ChannelHandlerContext.class);
        val channel = mock(Channel.class);
        val channelConfig = mock(ChannelConfig.class);
        when(context.channel()).thenReturn(channel);
        when(channel.config()).thenReturn(channelConfig);
        val eventLoop = mock(EventLoop.class);
        when(channel.eventLoop()).thenReturn(eventLoop);
        when(eventLoop.inEventLoop()).thenReturn(false);
        val channelFuture = mock(ChannelFuture.class);
        when(channel.writeAndFlush(any())).thenReturn(channelFuture);

        val segments = IntStream.range(0, segmentCount).mapToObj(Integer::toString).collect(Collectors.toList());
        val writers = IntStream.range(0, writersCount).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toList());

        val store = mock(StreamSegmentStore.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        val processors = new ArrayList<AppendProcessor>();
        for (int i = 0; i < connectionCount; i++) {
            val h = new ServerConnectionInboundHandler();
            h.channelRegistered(context);
            val p = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(h, tracker)).build();
            processors.add(p);
        }

        // Setup appends.
        for (int connectionId = 0; connectionId < processors.size(); connectionId++) {
            for (val s : segments) {
                for (val w : writers) {
                    when(store.getAttributes(s, Collections.singleton(AttributeId.fromUUID(w)), true, AppendProcessor.TIMEOUT))
                            .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(AttributeId.fromUUID(w), 0L)));
                    setMockForIndexSegmentAppends(s, store, 1, 1);
                    processors.get(connectionId).setupAppend(new WireCommands.SetupAppend(0, w, s, null));
                }
            }
        }

        // Divide the segments into conditional and unconditional.
        val conditionalSegments = segments.subList(0, segments.size() / 2);
        val unconditionalSegments = segments.subList(conditionalSegments.size(), segments.size() - 1);

        // Send a few appends to each connection from each writer.
        val appendData = Unpooled.wrappedBuffer(new byte[1]);
        when(store.append(any(), any(), any(), any()))
                .thenReturn(delayedResponse(0L));
        for (val s : unconditionalSegments) {
            for (val p : processors) {
                for (val w : writers) {
                    setMockForIndexSegmentAppends(s, store, 1, 1);
                    p.append(new Append(s, w, 1, new WireCommands.Event(appendData.retain()), 0));
                }
            }
        }

        // Send a few conditional appends to each connection from each writer. Fail some along the way.
        int appendOffset = 0;
        for (val s : conditionalSegments) {
            for (val p : processors) {
                for (val w : writers) {
                    boolean fail = appendOffset % 3 == 0;
                    if (fail) {
                        when(store.append(any(), any(long.class), any(), any(), any()))
                                .thenReturn(delayedFailure(new BadOffsetException(s, appendOffset, appendOffset)));
                    } else {
                        when(store.append(any(), any(long.class), any(), any(), any()))
                                .thenReturn(delayedResponse(0L));
                    }
                    setMockForIndexSegmentAppends(s, store, 1, 1);
                    p.append(new Append(s, w, 1, new WireCommands.Event(appendData.retain()), appendOffset, 0));
                    appendOffset++;
                }
            }
        }

        // Fail (attributes) all connections.
        when(store.append(any(), any(), any(), any()))
                .thenReturn(delayedFailure(new BadAttributeUpdateException("s", null, false, "intentional")));
        for (val s : conditionalSegments) {
            for (val p : processors) {
                for (val w : writers) {
                    setMockForIndexSegmentAppends(s, store, 1, 1);
                    p.append(new Append(s, w, 1, new WireCommands.Event(appendData.retain()), 0));
                }
            }
        }

        // Verify that there is no drift in the ConnectionTracker#getTotalOutstanding value. Due to the async nature
        // of the calls, this value may not immediately be updated.
        AssertExtensions.assertEventuallyEquals(0L, tracker::getTotalOutstanding, 10000);
    }

    @Test(timeout = 15 * 1000)
    public void testAppendAfterSealReturns() throws Exception {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(connection, tracker)).build();
        InOrder connectionVerifier = Mockito.inOrder(connection);
        setupGetAttributes(streamSegmentName, clientId, store);
        setMockForIndexSegmentAppends(streamSegmentName, store, 1, 1);
        processor.setupAppend(new SetupAppend(requestId, clientId, streamSegmentName, ""));
        verify(store).getAttributes(eq(streamSegmentName), any(), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(connection).send(new WireCommands.AppendSetup(requestId, streamSegmentName, clientId, 0));
        verify(store).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 15 * 1000)
    public void testAppendAfterSealThrows() throws Exception {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        @Cleanup
        IndexAppendProcessor indexAppendProcessor = getIndexAppendProcessor(store);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(indexAppendProcessor).store(store).connection(new TrackedConnection(connection, tracker)).build();
        InOrder connectionVerifier = Mockito.inOrder(connection);
        @SuppressWarnings("unchecked")
        Map<AttributeId, Long> attributes = mock(Map.class);
        when(store.getAttributes(streamSegmentName, Collections.singleton(AttributeId.fromUUID(clientId)), true, AppendProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(attributes));
        setMockForIndexSegmentAppends(streamSegmentName, store, 1, 1);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        verify(store).getAttributes(eq(streamSegmentName), any(), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(connection).close();
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 10000)
    public void testCreateSegmentIfNotExistOnSetupAppend() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        ConnectionTracker tracker = mock(ConnectionTracker.class);
        @Cleanup
        AppendProcessor processor = AppendProcessor.defaultBuilder(getIndexAppendProcessor(store)).store(store).connection(new TrackedConnection(connection, tracker)).build();
        InOrder connectionVerifier = Mockito.inOrder(connection);
        setupGetAttributes(streamSegmentName, clientId, store);
        CompletableFuture<SegmentProperties> toBeReturned = CompletableFuture.failedFuture(new StreamSegmentNotExistsException("Stream not exist"));
        CompletableFuture<SegmentProperties> authFailed = CompletableFuture.failedFuture(new TokenExpiredException("Token expired"));
        CompletableFuture<Void> createIndexFuture = CompletableFuture.completedFuture(null);
        doReturn(toBeReturned).doReturn(authFailed).when(store).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), eq(AppendProcessor.TIMEOUT));
        doReturn(createIndexFuture).when(store).createStreamSegment(anyString(), any(), any(), eq(AppendProcessor.TIMEOUT));
        processor.setupAppend(new SetupAppend(requestId, clientId, streamSegmentName, ""));
        processor.setupAppend(new SetupAppend(requestId, clientId, streamSegmentName, ""));
        verify(connection, times(2)).send(any());
        verify(store, times(2)).getAttributes(eq(streamSegmentName), any(), anyBoolean(), any());
        verify(store, times(2)).getAttributes(eq(NameUtils.getIndexSegmentName(streamSegmentName)), any(), anyBoolean(), any());
        verify(store, times(1)).createStreamSegment(anyString(), any(), any(), any());
        verify(connection).close();
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    private <T> CompletableFuture<T> delayedResponse(T value) {
        return Futures.delayedFuture(Duration.ofMillis(1), INLINE_EXECUTOR).thenApply(v -> value);
    }

    private <V, T extends Throwable> CompletableFuture<V> delayedFailure(T ex) {
        return Futures.delayedFuture(Duration.ofMillis(1), INLINE_EXECUTOR)
                      .thenCompose(v -> Futures.failedFuture(ex));
    }

    private AttributeUpdateCollection updateEventNumber(UUID clientId, long eventNum) {
        return updateEventNumber(clientId, eventNum, 0, 1);
    }

    private AttributeUpdateCollection updateEventNumber(UUID clientId, long eventNum, long previousValue, long eventCount) {
        return AttributeUpdateCollection.from(
                new AttributeUpdate(AttributeId.fromUUID(clientId), AttributeUpdateType.ReplaceIfEquals, eventNum, previousValue),
                new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, eventCount));
    }

    private void setupGetAttributes(String streamSegmentName, UUID clientId, StreamSegmentStore store) {
        setupGetAttributes(streamSegmentName, clientId, 0, store);
    }

    private void setupGetAttributes(String streamSegmentName, UUID clientId, long eventNumber, StreamSegmentStore store) {
        when(store.getAttributes(streamSegmentName, Collections.singleton(AttributeId.fromUUID(clientId)), true, AppendProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(AttributeId.fromUUID(clientId), eventNumber)));
    }

    private EmbeddedChannel createChannel(StreamSegmentStore store, IndexAppendProcessor indexAppendProcessor) {
        ServerConnectionInboundHandler lsh = new ServerConnectionInboundHandler();
        EmbeddedChannel channel = new EmbeddedChannel(new ExceptionLoggingHandler(""),
                new CommandEncoder(null, MetricNotifier.NO_OP_METRIC_NOTIFIER),
                new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
                new CommandDecoder(),
                new AppendDecoder(),
                lsh);
        lsh.setRequestProcessor(AppendProcessor.defaultBuilder(indexAppendProcessor)
                                               .store(store)
                                               .connection(new TrackedConnection(lsh))
                                               .nextRequestProcessor(new PravegaRequestProcessor(store, mock(TableStore.class), lsh, indexAppendProcessor))
                                               .build());
        return channel;
    }

    private Reply sendRequest(EmbeddedChannel channel, Request request) throws Exception {
        channel.writeInbound(request);
        Object encodedReply = channel.readOutbound();
        for (int i = 0; encodedReply == null && i < 50; i++) {
            channel.runPendingTasks();
            Thread.sleep(10);
            encodedReply = channel.readOutbound();
        }
        if (encodedReply == null) {
            return null;
        }
        WireCommand decoded = CommandDecoder.parseCommand((ByteBuf) encodedReply);
        ((ByteBuf) encodedReply).release();
        assertNotNull(decoded);
        return (Reply) decoded;
    }

    private void interceptCreateTransient(StreamSegmentStore store, String streamSegmentName) {
        when(store.createStreamSegment(contains(streamSegmentName), eq(SegmentType.TRANSIENT_SEGMENT), any(), eq(AppendProcessor.TIMEOUT)))
                .thenAnswer(invocation ->  CompletableFuture.completedFuture(null));
    }

    private AppendContext interceptAppend(StreamSegmentStore store, String streamSegmentName, AttributeUpdateCollection attributeUpdates,
                                          CompletableFuture<Long> response) {
        if (!NameUtils.isIndexSegment(streamSegmentName)) {
            this.interceptAppend(store, NameUtils.getIndexSegmentName(streamSegmentName), response);
        }
        val result = new AppendContext(store, streamSegmentName, attributeUpdates);
        when(store.append(eq(streamSegmentName), any(), eq(attributeUpdates), eq(AppendProcessor.TIMEOUT)))
                .thenAnswer(invocation -> {
                    result.appendedData.set(((ByteBufWrapper) invocation.getArgument(1)).getCopy());
                    return response;
                });
        return result;
    }

    private void interceptAppend(StreamSegmentStore store, String streamSegmentName,
                                          CompletableFuture<Long> response) {
        when(store.append(eq(streamSegmentName), any(), any(), eq(AppendProcessor.TIMEOUT)))
                .thenAnswer(invocation -> {
                    //result.appendedData.set(((ByteBufWrapper) invocation.getArgument(1)).getCopy());
                    return response;
                });
    }

    private AppendContext interceptAppend(StreamSegmentStore store, String streamSegmentName, long offset, AttributeUpdateCollection attributeUpdates,
                                          CompletableFuture<Long> response) {
        if (!NameUtils.isIndexSegment(streamSegmentName)) {
            this.interceptAppend(store, NameUtils.getIndexSegmentName(streamSegmentName), response);
        }
        val result = new AppendContext(store, streamSegmentName, offset, attributeUpdates);
        when(store.append(eq(streamSegmentName), eq(offset), any(), eq(attributeUpdates), eq(AppendProcessor.TIMEOUT)))
                .thenAnswer(invocation -> {
                    result.appendedData.set(((ByteBufWrapper) invocation.getArgument(2)).getCopy());
                    return response;
                });
        return result;
    }

    private void verifyStoreAppend(AppendContext appendContext, byte[] expectedData) {
        verifyStoreAppend(Mockito::verify, appendContext, expectedData);
    }

    private void verifyStoreAppend(InOrder verifier, AppendContext appendContext, byte[] expectedData) {
        verifyStoreAppend(verifier::verify, appendContext, expectedData);
    }

    private void verifyStoreAppend(Function<StreamSegmentStore, StreamSegmentStore> verifier, AppendContext appendContext, byte[] expectedData) {
        if (appendContext.offset == null) {
            verifier.apply(appendContext.store).append(eq(appendContext.segmentName),
                    any(),
                    eq(appendContext.attributeUpdates),
                    eq(AppendProcessor.TIMEOUT));
        } else {
            verifier.apply(appendContext.store).append(eq(appendContext.segmentName),
                    eq(appendContext.offset),
                    any(),
                    eq(appendContext.attributeUpdates),
                    eq(AppendProcessor.TIMEOUT));
        }
        assertArrayEquals(expectedData, appendContext.appendedData.get());
    }

    @RequiredArgsConstructor
    private static class AppendContext {
        final StreamSegmentStore store;
        final String segmentName;
        final Long offset;
        final AttributeUpdateCollection attributeUpdates;
        final AtomicReference<byte[]> appendedData = new AtomicReference<>();

        AppendContext(StreamSegmentStore store, String segmentName, AttributeUpdateCollection attributeUpdates) {
            this(store, segmentName, null, attributeUpdates);
        }
    }
   
}
