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
package io.pravega.client.admin.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerFailureException;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReaderGroupNotFoundException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionInfo;
import io.pravega.client.stream.impl.EventPointerImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.impl.TransactionInfoImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import lombok.Cleanup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.reset;

public class StreamManagerImplTest {

    private static final int SERVICE_PORT = 12345;
    private final String defaultScope = "foo";
    private StreamManager streamManager;
    private Controller controller = null;
    private MockConnectionFactoryImpl connectionFactory;

    @Before
    public void setUp() {
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        connectionFactory = new MockConnectionFactoryImpl();
        this.controller = new MockController(uri.getEndpoint(), uri.getPort(), connectionFactory, true);
        this.streamManager = new StreamManagerImpl(controller, connectionFactory);
    }

    @After
    public void tearDown() {
        this.streamManager.close();
        this.controller.close();
        this.connectionFactory.close();
    }

    @Test 
    public void testConnectionPoolConfig() {
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:" + TestUtils.getAvailableListenPort())).build();
        @Cleanup 
        StreamManagerImpl streamManager = new StreamManagerImpl(clientConfig);
        ConnectionPoolImpl connectionPool = (ConnectionPoolImpl) streamManager.getConnectionPool();
    
        Assert.assertEquals(clientConfig, connectionPool.getClientConfig());
    }

    @Test
    public void testCreateAndDeleteScope() throws DeleteScopeFailedException {
        // Create and delete immediately
        Assert.assertTrue(streamManager.createScope(defaultScope));
        Assert.assertTrue(streamManager.deleteScope(defaultScope));

        // Create twice
        Assert.assertTrue(streamManager.createScope(defaultScope));
        Assert.assertFalse(streamManager.createScope(defaultScope));
        Assert.assertTrue(streamManager.deleteScope(defaultScope));

        Assert.assertTrue(streamManager.createScope(defaultScope));
        Assert.assertTrue(streamManager.deleteScopeRecursive(defaultScope));

        // Try to create invalid scope name.
        AssertExtensions.assertThrows(Exception.class, () -> streamManager.createScope("_system"));

        // This call should actually fail
        Assert.assertFalse(streamManager.deleteScope(defaultScope));
    }

    @Test
    public  void testFetchEvent() throws EndOfSegmentException, SegmentTruncatedException {
        Segment segment = new Segment("scope", "stream", 0L);
        StringBuilder sb = new StringBuilder();
        sb.append(segment.getScopedName());
        sb.append(':');
        sb.append(123L);
        sb.append('-');
        sb.append(27);

        //Setting Up the Mocks
        EventSegmentReader reader = mock(EventSegmentReader.class);
        SegmentInputStreamFactory segmentInputStreamFactory = mock(SegmentInputStreamFactory.class);
        StreamManagerImpl streamManagerLocal = new StreamManagerImpl(controller, connectionFactory, segmentInputStreamFactory);
        when(segmentInputStreamFactory.createEventReaderForSegment(any(Segment.class), anyLong(), anyInt())).thenReturn(reader);
        when(reader.read()).thenReturn(ByteBuffer.wrap("Test-Fetch-Event".getBytes(StandardCharsets.UTF_8)));

        EventPointer pointer = EventPointerImpl.fromString(sb.toString());
        assertNotNull(pointer);
        UTF8StringSerializer serializer = new UTF8StringSerializer();
        CompletableFuture<String> future = streamManagerLocal.fetchEvent(pointer, serializer);
       assertEquals("Test-Fetch-Event", future.join());
    }

    @Test
    public  void testFetchEventThrowingNoSuchSegmentException() throws EndOfSegmentException, SegmentTruncatedException, InterruptedException {
        Segment segment = new Segment("scope", "stream", 0L);
        StringBuilder sb = new StringBuilder();
        sb.append(segment.getScopedName());
        sb.append(':');
        sb.append(123L);
        sb.append('-');
        sb.append(27);

        EventSegmentReader reader = mock(EventSegmentReader.class);
        SegmentInputStreamFactory segmentInputStreamFactory = mock(SegmentInputStreamFactory.class);
        StreamManagerImpl streamManagerLocal = new StreamManagerImpl(controller, connectionFactory, segmentInputStreamFactory);
        when(segmentInputStreamFactory.createEventReaderForSegment(any(Segment.class), anyLong(), anyInt())).thenReturn(reader);

        when(reader.read()).thenThrow(new NoSuchSegmentException("Event no longer exists."));

        EventPointer pointer = EventPointerImpl.fromString(sb.toString());
        assertNotNull(pointer);
        UTF8StringSerializer serializer = new UTF8StringSerializer();
        CompletableFuture<String> future = streamManagerLocal.fetchEvent(pointer, serializer);
        AssertExtensions.assertFutureThrows("Should throw Exception", future, throwable -> throwable instanceof NoSuchEventException);

    }

    @Test
    public  void testFetchEventThrowingEndOfSegmentException() throws EndOfSegmentException, SegmentTruncatedException, InterruptedException {
        Segment segment = new Segment("scope", "stream", 0L);
        StringBuilder sb = new StringBuilder();
        sb.append(segment.getScopedName());
        sb.append(':');
        sb.append(123L);
        sb.append('-');
        sb.append(27);

        EventSegmentReader reader = mock(EventSegmentReader.class);
        SegmentInputStreamFactory segmentInputStreamFactory = mock(SegmentInputStreamFactory.class);
        StreamManagerImpl streamManagerLocal = new StreamManagerImpl(controller, connectionFactory, segmentInputStreamFactory);
        when(segmentInputStreamFactory.createEventReaderForSegment(any(Segment.class), anyLong(), anyInt())).thenReturn(reader);

        when(reader.read()).thenThrow(new EndOfSegmentException());

        EventPointer pointer = EventPointerImpl.fromString(sb.toString());
        assertNotNull(pointer);
        UTF8StringSerializer serializer = new UTF8StringSerializer();
        CompletableFuture<String> future = streamManagerLocal.fetchEvent(pointer, serializer);
        AssertExtensions.assertFutureThrows("Should throw Exception", future, throwable -> throwable instanceof NoSuchEventException);

    }

    @Test(timeout = 15000)
    public void testStreamInfo() throws Exception {
        final String streamName = "stream";
        final Stream stream = new StreamImpl(defaultScope, streamName);

        // Setup Mocks
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateSegment request = (WireCommands.CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateSegment.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = (WireCommands.GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                                                             false, false, 0, 0, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.GetStreamSegmentInfo.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(),
                                                           connectionFactory, true);
        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        @Cleanup
        final StreamManager streamManager = new StreamManagerImpl(mockController, pool);

        streamManager.createScope(defaultScope);
        streamManager.createStream(defaultScope, streamName, StreamConfiguration.builder()
                                                                                .scalingPolicy(ScalingPolicy.fixed(3))
                                                                                .build());
        // fetch StreamInfo.
        StreamInfo info = streamManager.fetchStreamInfo(defaultScope, streamName).join();

        //validate results.
        assertEquals(defaultScope, info.getScope());
        assertEquals(streamName, info.getStreamName());
        assertNotNull(info.getTailStreamCut());
        assertEquals(stream, info.getTailStreamCut().asImpl().getStream());
        assertEquals(3, info.getTailStreamCut().asImpl().getPositions().size());
        assertNotNull(info.getHeadStreamCut());
        assertEquals(stream, info.getHeadStreamCut().asImpl().getStream());
        assertEquals(3, info.getHeadStreamCut().asImpl().getPositions().size());
        assertFalse(info.isSealed());
    }

    @Test(timeout = 10000)
    public void testSealedStream() throws ConnectionFailedException {
        final String streamName = "stream";
        final Stream stream = new StreamImpl(defaultScope, streamName);

        // Setup Mocks
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateSegment request = (WireCommands.CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateSegment.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = (WireCommands.GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                         false, false, 0, 0, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.GetStreamSegmentInfo.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = spy(new MockController(location.getEndpoint(), location.getPort(),
                connectionFactory, true));

        doReturn(CompletableFuture.completedFuture(true)).when(mockController).sealStream(defaultScope, streamName);
        StreamSegments empty = new StreamSegments(new TreeMap<>());
        doReturn(CompletableFuture.completedFuture(empty) ).when(mockController).getCurrentSegments(defaultScope, streamName);

        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        // Create a StreamManager
        @Cleanup
        final StreamManager streamManager = new StreamManagerImpl(mockController, pool);

        // Create a scope and stream and seal it.
        streamManager.createScope(defaultScope);
        streamManager.createStream(defaultScope, streamName, StreamConfiguration.builder()
                                                                                .scalingPolicy(ScalingPolicy.fixed(3))
                                                                                .build());
        streamManager.sealStream(defaultScope, streamName);

        //Fetch StreamInfo
        StreamInfo info = streamManager.fetchStreamInfo(defaultScope, streamName).join();

        //validate results.
        assertEquals(defaultScope, info.getScope());
        assertEquals(streamName, info.getStreamName());
        assertNotNull(info.getTailStreamCut());
        assertEquals(stream, info.getTailStreamCut().asImpl().getStream());
        assertEquals(0, info.getTailStreamCut().asImpl().getPositions().size());
        assertNotNull(info.getHeadStreamCut());
        assertEquals(stream, info.getHeadStreamCut().asImpl().getStream());
        assertEquals(3, info.getHeadStreamCut().asImpl().getPositions().size());
        assertTrue(info.isSealed());
    }

    @Test(timeout = 10000)
    public void testListScopes() throws ConnectionFailedException {
        // Setup Mocks
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateSegment request = (WireCommands.CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateSegment.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = (WireCommands.GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                         false, false, 0, 0, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.GetStreamSegmentInfo.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(),
                connectionFactory, true);
        
        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        @Cleanup
        final StreamManager streamManager = new StreamManagerImpl(mockController, pool);

        String scope = "scope";
        String scope1 = "scope1";
        String scope2 = "scope2";
        String scope3 = "scope3";
        String stream1 = "stream1";
        streamManager.createScope(scope);
        streamManager.createScope(scope1);
        streamManager.createScope(scope2);
        streamManager.createScope(scope3);

        streamManager.createStream(scope, stream1, StreamConfiguration.builder()
                                                                      .scalingPolicy(ScalingPolicy.fixed(3))
                                                                      .build());
        ArrayList<String> result = Lists.newArrayList(streamManager.listScopes());
        assertEquals(result.size(), 4);
        assertTrue(streamManager.checkScopeExists(scope));
        assertFalse(streamManager.checkScopeExists("nonExistent"));
        assertTrue(streamManager.checkStreamExists(scope, stream1));
        assertFalse(streamManager.checkStreamExists(scope, "nonExistent"));
        assertFalse(streamManager.checkStreamExists("nonExistent", "nonExistent"));
    }

    @Test(timeout = 10000)
    public void testListStreamInScope() throws ConnectionFailedException {
        // Setup Mocks
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateSegment request = (WireCommands.CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateSegment.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = (WireCommands.GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                         false, false, 0, 0, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.GetStreamSegmentInfo.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(),
                connectionFactory, true);
        
        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        @Cleanup
        final StreamManager streamManager = new StreamManagerImpl(mockController, pool);

        String scope = "scope";
        String stream1 = "stream1";
        String stream2 = "stream2";
        String stream3 = "stream3";
        streamManager.createScope(scope);
        
        streamManager.createStream(scope, stream1, StreamConfiguration.builder()
                                                                        .scalingPolicy(ScalingPolicy.fixed(3))
                                                                        .tag("t1")
                                                                        .build());
        streamManager.createStream(scope, stream2, StreamConfiguration.builder()
                                                                        .scalingPolicy(ScalingPolicy.fixed(3))
                                                                        .build());
        streamManager.createStream(scope, stream3, StreamConfiguration.builder()
                                                                        .scalingPolicy(ScalingPolicy.fixed(3))
                                                                        .build());
        Iterator<Stream> m = streamManager.listStreams(scope);
        Set<Stream> streams = new HashSet<>();
        assertTrue(m.hasNext());
        streams.add(m.next());
        assertTrue(m.hasNext());
        streams.add(m.next());
        assertTrue(m.hasNext());
        streams.add(m.next());
        assertFalse(m.hasNext());

        assertEquals(3, streams.size());
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream1)));
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream2)));
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream3)));
        assertEquals(Collections.singleton("t1"), streamManager.getStreamTags(scope, stream1));
        assertEquals(Collections.singletonList(Stream.of(scope, stream1)), newArrayList(streamManager.listStreams(scope, "t1")));

        streamManager.updateStream(scope, stream1, StreamConfiguration.builder()
                                                                     .scalingPolicy(ScalingPolicy.fixed(3))
                                                                     .build());
        assertEquals(Collections.emptySet(), streamManager.getStreamTags(scope, stream1));

    }
    
    @Test(timeout = 10000)
    public void testForceDeleteScope() throws ConnectionFailedException, DeleteScopeFailedException {
        // Setup Mocks
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateSegment request = (WireCommands.CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateSegment.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = (WireCommands.GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                         false, false, 0, 0, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.GetStreamSegmentInfo.class));
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.DeleteSegment request = (WireCommands.DeleteSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.SegmentDeleted(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.DeleteSegment.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = spy(new MockController(location.getEndpoint(), location.getPort(),
                connectionFactory, true));

        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        @Cleanup
        final StreamManager streamManager = new StreamManagerImpl(mockController, pool);

        String scope = "scope";
        String stream1 = "stream1";
        String stream2 = "stream2";
        String stream3 = "stream3";
        streamManager.createScope(scope);
        
        streamManager.createStream(scope, stream1, StreamConfiguration.builder()
                                                                        .scalingPolicy(ScalingPolicy.fixed(3))
                                                                        .build());
        streamManager.createStream(scope, stream2, StreamConfiguration.builder()
                                                                        .scalingPolicy(ScalingPolicy.fixed(3))
                                                                        .build());
        streamManager.createStream(scope, stream3, StreamConfiguration.builder()
                                                                        .scalingPolicy(ScalingPolicy.fixed(3))
                                                                        .build());
        Set<Stream> streams = Sets.newHashSet(streamManager.listStreams(scope));
        
        assertEquals(3, streams.size());
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream1)));
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream2)));
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream3)));

        // mock controller client to throw exceptions when attempting to seal and delete for stream 1. 
        doAnswer(x -> Futures.failedFuture(new ControllerFailureException("Unable to seal stream"))).when(mockController).sealStream(scope, stream1);
        doAnswer(x -> Futures.failedFuture(new IllegalArgumentException("Stream not sealed"))).when(mockController).deleteStream(scope, stream1);

        AssertExtensions.assertThrows("Should have thrown exception", () -> streamManager.deleteScope(scope, true), 
                e -> Exceptions.unwrap(e) instanceof DeleteScopeFailedException);

        // reset mock controller
        reset(mockController);
        
        // throw invalid stream for stream 2. Delete should happen despite invalid stream exception.
        doAnswer(x -> Futures.failedFuture(new InvalidStreamException("Stream does not exist"))).when(mockController).sealStream(scope, stream2);

        assertTrue(streamManager.deleteScope(scope, true));
    }

    @Test(timeout = 10000)
    public void testForceDeleteScopeWithKeyValueTables() throws ConnectionFailedException, DeleteScopeFailedException {
        // Setup Mocks
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateSegment request = (WireCommands.CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateSegment.class));
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateTableSegment request = (WireCommands.CreateTableSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateTableSegment.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateTableSegment request = invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateTableSegment.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = (WireCommands.GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                false, false, 0, 0, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.GetStreamSegmentInfo.class));
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.DeleteSegment request = (WireCommands.DeleteSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.SegmentDeleted(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.DeleteSegment.class));
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.DeleteTableSegment request = invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.SegmentDeleted(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.DeleteTableSegment.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = spy(new MockController(location.getEndpoint(), location.getPort(),
                connectionFactory, true));

        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        @Cleanup
        final StreamManager streamManager = new StreamManagerImpl(mockController, pool);
        @Cleanup
        final KeyValueTableManager keyValueTableManager = new KeyValueTableManagerImpl(mockController, connectionFactory);

        String scope = "scope";
        String kvt1 = "kvt1";
        String kvt2 = "kvt2";
        streamManager.createScope(scope);

        KeyValueTableConfiguration kvtConfig = KeyValueTableConfiguration.builder().partitionCount(1).primaryKeyLength(1).secondaryKeyLength(1).build();
        keyValueTableManager.createKeyValueTable(scope, kvt1, kvtConfig);
        keyValueTableManager.createKeyValueTable(scope, kvt2, kvtConfig);
        Set<KeyValueTableInfo> keyValueTables = Sets.newHashSet(keyValueTableManager.listKeyValueTables(scope));

        assertEquals(2, keyValueTables.size());
        assertTrue(keyValueTables.stream().anyMatch(x -> x.getKeyValueTableName().equals(kvt1)));
        assertTrue(keyValueTables.stream().anyMatch(x -> x.getKeyValueTableName().equals(kvt2)));

        // mock controller client to throw exceptions when attempting to delete key value table 1.
        doAnswer(x -> Futures.failedFuture(new ControllerFailureException("Unable to delete key value table")))
                .when(mockController).deleteKeyValueTable(scope, kvt1);

        AssertExtensions.assertThrows("Should have thrown exception", () -> streamManager.deleteScope(scope, true),
                e -> Exceptions.unwrap(e) instanceof DeleteScopeFailedException);

        // reset mock controller
        reset(mockController);

        assertTrue(streamManager.deleteScope(scope, true));
    }

    @Test
    public void testForceDeleteScopeWithReaderGroups() throws ConnectionFailedException, DeleteScopeFailedException {
        // Setup Mocks
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateSegment request = (WireCommands.CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateSegment.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = (WireCommands.GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                false, false, 0, 0, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.GetStreamSegmentInfo.class));
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.DeleteSegment request = (WireCommands.DeleteSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.SegmentDeleted(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.DeleteSegment.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = spy(new MockController(location.getEndpoint(), location.getPort(),
                connectionFactory, true));

        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        @Cleanup
        final StreamManager streamManager = new StreamManagerImpl(mockController, pool);

        String scope = "scope";
        String stream1 = "stream1";
        String stream2 = "stream2";
        String readerGroup1 = "readerGroup1";
        String readerGroup2 = "readerGroup2";
        ReaderGroupConfig config1 = ReaderGroupConfig.builder()
                .stream(NameUtils.getScopedStreamName(scope, stream1))
                .build();
        ReaderGroupConfig config2 = ReaderGroupConfig.builder()
                .stream(NameUtils.getScopedStreamName(scope, stream2))
                .build();
        streamManager.createScope(scope);

        streamManager.createStream(scope, stream1, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build());
        streamManager.createStream(scope, stream2, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build());
        Set<Stream> streams = Sets.newHashSet(streamManager.listStreams(scope));

        assertEquals(2, streams.size());
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream1)));
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream2)));

        mockController.createReaderGroup(scope, readerGroup1, config1);
        mockController.createReaderGroup(scope, readerGroup2, config2);

        // mock controller client to throw exceptions when attempting to get config for reader-group.
        doAnswer(x -> Futures.failedFuture(new ControllerFailureException("Unable to access reader-group config")))
                .when(mockController).getReaderGroupConfig(scope, readerGroup1);

        doAnswer(x -> new AsyncIterator<Stream>() {
            final Iterator<Stream> iterator = new ArrayList<Stream>(Arrays.asList(new StreamImpl(scope, stream1), new StreamImpl(scope, stream2),
                    new StreamImpl(scope, NameUtils.getStreamForReaderGroup(readerGroup1)),
                    new StreamImpl(scope, NameUtils.getStreamForReaderGroup(readerGroup2)))).iterator();

            @Override
            public CompletableFuture<Stream> getNext() {
                Stream next;
                if (!iterator.hasNext()) {
                    next = null;
                } else {
                    next = iterator.next();
                }
                return CompletableFuture.completedFuture(next);
            }
        }).when(mockController).listStreams(scope);
        AssertExtensions.assertThrows("Should have thrown exception", () -> streamManager.deleteScope(scope, true),
                e -> Exceptions.unwrap(e) instanceof DeleteScopeFailedException);

        // reset mock controller
        reset(mockController);

        streamManager.createStream(scope, stream1, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build());
        streamManager.createStream(scope, stream2, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build());
        streams = Sets.newHashSet(streamManager.listStreams(scope));

        assertEquals(2, streams.size());
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream1)));
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream2)));

        mockController.createReaderGroup(scope, readerGroup1, config1);
        mockController.createReaderGroup(scope, readerGroup2, config2);

        // mock controller client to throw exceptions when attempting to delete the reader-group.
        doAnswer(x -> Futures.failedFuture(new ControllerFailureException("Unable to delete reader-group")))
                .when(mockController).deleteReaderGroup(scope, readerGroup1, config1.getReaderGroupId());

        doAnswer(x -> new AsyncIterator<Stream>() {
            final Iterator<Stream> iterator = new ArrayList<Stream>(Arrays.asList(new StreamImpl(scope, stream1), new StreamImpl(scope, stream2),
                    new StreamImpl(scope, NameUtils.getStreamForReaderGroup(readerGroup1)),
                    new StreamImpl(scope, NameUtils.getStreamForReaderGroup(readerGroup2)))).iterator();

            @Override
            public CompletableFuture<Stream> getNext() {
                Stream next;
                if (!iterator.hasNext()) {
                    next = null;
                } else {
                    next = iterator.next();
                }
                return CompletableFuture.completedFuture(next);
            }
        }).when(mockController).listStreams(scope);
        AssertExtensions.assertThrows("Should have thrown exception", () -> streamManager.deleteScope(scope, true),
                e -> Exceptions.unwrap(e) instanceof DeleteScopeFailedException);

        // reset mock controller
        reset(mockController);

        streamManager.createStream(scope, stream1, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build());
        streamManager.createStream(scope, stream2, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build());
        streams = Sets.newHashSet(streamManager.listStreams(scope));

        assertEquals(2, streams.size());
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream1)));
        assertTrue(streams.stream().anyMatch(x -> x.getStreamName().equals(stream2)));

        mockController.createReaderGroup(scope, readerGroup1, config1);
        mockController.createReaderGroup(scope, readerGroup2, config2);

        // mock controller client to throw ReaderGroupNotFoundException when attempting to get the config of reader-group.
        doAnswer(x -> Futures.failedFuture(new ReaderGroupNotFoundException("Reader-group does not exist")))
                .when(mockController).getReaderGroupConfig(scope, readerGroup1);

        doAnswer(x -> new AsyncIterator<Stream>() {
            final Iterator<Stream> iterator = new ArrayList<Stream>(Arrays.asList(new StreamImpl(scope, stream1), new StreamImpl(scope, stream2),
                    new StreamImpl(scope, NameUtils.getStreamForReaderGroup(readerGroup1)),
                    new StreamImpl(scope, NameUtils.getStreamForReaderGroup(readerGroup2)))).iterator();

            @Override
            public CompletableFuture<Stream> getNext() {
                Stream next;
                if (!iterator.hasNext()) {
                    next = null;
                } else {
                    next = iterator.next();
                }
                return CompletableFuture.completedFuture(next);
            }
        }).when(mockController).listStreams(scope);
        assertTrue(streamManager.deleteScope(scope, true));
    }

    @Test
    public void testListCompletedTransactions() throws ConnectionFailedException {
        // Setup Mocks
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer((Answer<Void>) invocation -> {
            WireCommands.CreateSegment request = invocation.getArgument(0);
            connectionFactory.getProcessor(location)
                    .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
            return null;
        }).when(connection).send(Mockito.any(WireCommands.CreateSegment.class));

        Mockito.doAnswer((Answer<Void>) invocation -> {
            WireCommands.GetStreamSegmentInfo request = invocation.getArgument(0);
            connectionFactory.getProcessor(location)
                    .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                            false, false, 0, 0, 0));
            return null;
        }).when(connection).send(Mockito.any(WireCommands.GetStreamSegmentInfo.class));
        connectionFactory.provideConnection(location, connection);

        String scope = "scope";
        String stream = "stream";
        Stream stream1 = new StreamImpl(scope, stream);

        Controller mockController = mock(Controller.class);
        List<TransactionInfo> transactionInfoList = new ArrayList<>();
        transactionInfoList.add(new TransactionInfoImpl(stream1, UUID.randomUUID(), Transaction.Status.ABORTED));
        transactionInfoList.add(new TransactionInfoImpl(stream1, UUID.randomUUID(), Transaction.Status.COMMITTED));

        doAnswer(x -> CompletableFuture.completedFuture(transactionInfoList)).when(mockController).listCompletedTransactions(new StreamImpl(scope, stream));

        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        @Cleanup final StreamManager streamManagerImpl = new StreamManagerImpl(mockController, pool);

        List<TransactionInfo> listTransactions = streamManagerImpl.listCompletedTransactions(new StreamImpl(scope, stream));
        assertEquals(2, listTransactions.size());
        AssertExtensions.assertThrows(Exception.class, () -> streamManager.listCompletedTransactions(new StreamImpl(scope, stream)));
    }

    @Test
    public void testGetDistanceBetweenTwoStreamCuts() {
        String scope = "scope";
        String stream = "stream";
        Stream stream1 = new StreamImpl(scope, stream);
        final StreamCut startStreamCut = getStreamCut("scope", stream, 10L, 1, 2);
        final StreamCut endStreamCut = getStreamCut("scope", stream, 30L, 1, 2);

        Controller mockController = mock(Controller.class);
        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        @Cleanup final StreamManager streamManagerImpl = new StreamManagerImpl(mockController, pool);

        ImmutableSet<Segment> r = ImmutableSet.<Segment>builder()
                .addAll(startStreamCut.asImpl().getPositions().keySet())
                .addAll(endStreamCut.asImpl().getPositions().keySet()).build();

        when(mockController.getSegments(startStreamCut,
                endStreamCut)).thenReturn(CompletableFuture.completedFuture(new StreamSegmentSuccessors(r, "")));
        CompletableFuture<Long> cf = streamManagerImpl.getDistanceBetweenTwoStreamCuts(stream1, startStreamCut,
                endStreamCut);
        Long distance = cf.join();
        assertEquals(Long.valueOf(40), distance);
    }

    @Test
    public void testGetDistanceBetweenTwoSCWithEndSCUnbounded() throws ConnectionFailedException {
        final String streamName = "stream";
        final Stream stream = new StreamImpl(defaultScope, streamName);

        // Setup Mocks
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateSegment request = (WireCommands.CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateSegment.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = (WireCommands.GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                false, false, 0, 20, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.GetStreamSegmentInfo.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(),
                connectionFactory, true);
        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        @Cleanup
        final StreamManager streamManager = new StreamManagerImpl(mockController, pool);

        streamManager.createScope(defaultScope);
        streamManager.createStream(defaultScope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        final StreamCut startStreamCut = getStreamCut(defaultScope, streamName, 10L, 0, 1);
        final StreamCut endStreamCut = StreamCut.UNBOUNDED;

        CompletableFuture<Long> cf = streamManager.getDistanceBetweenTwoStreamCuts(stream, startStreamCut,
                endStreamCut);
        Long distance = cf.join();
        assertEquals(Long.valueOf(20), distance);
    }

    @Test
    public void testGetDistanceBetweenTwoSCWithStartSCUnbounded() throws ConnectionFailedException {
        final String streamName = "stream";
        final Stream stream = new StreamImpl(defaultScope, streamName);

        // Setup Mocks
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateSegment request = (WireCommands.CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateSegment.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = (WireCommands.GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                false, false, 0, 30, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.GetStreamSegmentInfo.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(),
                connectionFactory, true);
        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        @Cleanup
        final StreamManager streamManager = new StreamManagerImpl(mockController, pool);

        streamManager.createScope(defaultScope);
        streamManager.createStream(defaultScope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        final StreamCut startStreamCut = StreamCut.UNBOUNDED;
        final StreamCut endStreamCut = getStreamCut(defaultScope, streamName, 10L, 0, 1);
        CompletableFuture<Long> cf = streamManager.getDistanceBetweenTwoStreamCuts(stream, startStreamCut,
                endStreamCut);
        Long distance = cf.join();
        assertEquals(Long.valueOf(20), distance);
    }

    @Test
    public void testGetDistanceBetweenTwoSCWhenBothUnbounded() throws ConnectionFailedException {
        final String streamName = "stream";
        final Stream stream = new StreamImpl(defaultScope, streamName);

        // Setup Mocks
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateSegment request = (WireCommands.CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.CreateSegment.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = (WireCommands.GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                        .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                false, false, 0, 10, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(WireCommands.GetStreamSegmentInfo.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(),
                connectionFactory, true);
        ConnectionPoolImpl pool = new ConnectionPoolImpl(ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        @Cleanup
        final StreamManager streamManager = new StreamManagerImpl(mockController, pool);

        streamManager.createScope(defaultScope);
        streamManager.createStream(defaultScope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        final StreamCut startStreamCut = StreamCut.UNBOUNDED;
        final StreamCut endStreamCut = StreamCut.UNBOUNDED;
        CompletableFuture<Long> cf = streamManager.getDistanceBetweenTwoStreamCuts(stream, startStreamCut,
                endStreamCut);
        Long distance = cf.join();
        assertEquals(Long.valueOf(10), distance);
    }

    private StreamCut getStreamCut(String scope, String streamName, long offset, int... segmentNumbers) {
        ImmutableMap.Builder<Segment, Long> builder = ImmutableMap.<Segment, Long>builder();
        Arrays.stream(segmentNumbers).forEach(seg -> {
            builder.put(new Segment(scope, streamName, seg), offset);
        });

        return new StreamCutImpl(Stream.of(scope, streamName), builder.build());
    }
}
