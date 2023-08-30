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
package io.pravega.client.state.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactory;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactoryImpl;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInfo;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.segment.impl.ServerTimeoutException;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.common.util.ByteBufferUtils;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.Mockito;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RevisionedStreamClientTest {
    private static final int SERVICE_PORT = 12345;
    private final StreamConfiguration config = StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build();
    
    @Test
    public void testWriteWhileReading() {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        createScopeAndStream(scope, stream, controller);

        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);
        
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, new JavaSerializer<>(), config);
        
        Revision initialRevision = client.fetchLatestRevision();
        client.writeUnconditionally("a");
        client.writeUnconditionally("b");
        client.writeUnconditionally("c");
        Iterator<Entry<Revision, String>> iter = client.readFrom(initialRevision);
        assertTrue(iter.hasNext());
        assertEquals("a", iter.next().getValue());
        
        client.writeUnconditionally("d");
        
        assertTrue(iter.hasNext());
        assertEquals("b", iter.next().getValue());
        assertTrue(iter.hasNext());
        
        Entry<Revision, String> entry = iter.next();
        assertEquals("c", entry.getValue());
        assertFalse(iter.hasNext());
        
        iter = client.readFrom(entry.getKey());
        assertTrue(iter.hasNext());
        assertEquals("d", iter.next().getValue());
        assertFalse(iter.hasNext());
    }

    @Test(timeout = 30000L)
    public void testReadRange() throws Exception {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        createScopeAndStream(scope, stream, controller);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);

        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, new JavaSerializer<>(), config);

        Revision r0 = client.fetchOldestRevision();
        client.writeUnconditionally("a");
        Revision ra = client.fetchLatestRevision();
        client.writeUnconditionally("b");
        Revision rb = client.fetchLatestRevision();
        client.writeUnconditionally("c");
        Iterator<Entry<Revision, String>> iterA = client.readRange(r0, rb);
        assertTrue(iterA.hasNext());
        assertEquals("a", iterA.next().getValue());
        assertEquals("b", iterA.next().getValue());
        assertThrows(NoSuchElementException.class, () -> iterA.next().getValue());
        // Will return an empty Entry for the same revision
        Iterator<Entry<Revision, String>> iterB = client.readRange(r0, r0);
        assertFalse(iterB.hasNext());
        // Checking the condition when endRevision is passed at the place of the startRevision
        assertThrows(IllegalStateException.class, () -> client.readRange(rb, r0));
        // Validating the case when stream already truncated and start revision didn't exist.
        Revision rc = client.fetchLatestRevision();
        client.truncateToRevision(rb);
        assertEquals(rb, client.fetchOldestRevision());
        assertThrows(TruncatedDataException.class, () -> client.readRange(ra, rc));
    }

    @Test(timeout = 60000L)
    public void testReadRangeThrowingTimoutException() throws Exception {
        String scope = "scope";
        String stream = "stream";
        // Setup Environment
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        createScopeAndStream(scope, stream, controller);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();

        // Setup mock
        SegmentOutputStreamFactory outFactory = mock(SegmentOutputStreamFactory.class);
        SegmentOutputStream out = mock(SegmentOutputStream.class);
        Segment segment = new Segment(scope, stream, 0);
        when(outFactory.createOutputStreamForSegment(eq(segment), any(), any(), any(DelegationTokenProvider.class)))
                .thenReturn(out);

        SegmentInputStreamFactory inFactory = mock(SegmentInputStreamFactory.class);
        EventSegmentReader in = mock(EventSegmentReader.class);
        when(inFactory.createEventReaderForSegment(eq(segment), anyInt())).thenReturn(in);
        when(in.read(anyLong())).thenReturn(null).thenReturn(serializer.serialize("testData"));

        SegmentMetadataClientFactory metaFactory = mock(SegmentMetadataClientFactory.class);
        SegmentMetadataClient metaClient = mock(SegmentMetadataClient.class);
        when(metaFactory.createSegmentMetadataClient(any(Segment.class), any(DelegationTokenProvider.class))).thenReturn(metaClient);

        CompletableFuture<SegmentInfo> neverCompleteFuture = new CompletableFuture<>();

        when(metaClient.getSegmentInfo()).thenReturn(neverCompleteFuture);

        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory,
                inFactory, streamFactory, streamFactory, metaFactory);

        @Cleanup
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, serializer,
                SynchronizerConfig.builder().build());
        // creating some dummy revision
        Revision r0 = new RevisionImpl(segment, 0L, 0);
        Revision ra = new RevisionImpl(segment, 0L, 0);

        assertThrows(ServerTimeoutException.class, () -> client.readRange(r0, ra));
    }

    @Test(timeout = 60000L)
    public void testReadFromThrowingTimoutException() throws Exception {
        String scope = "scope";
        String stream = "stream";
        // Setup Environment
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        createScopeAndStream(scope, stream, controller);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();

        // Setup mock
        SegmentOutputStreamFactory outFactory = mock(SegmentOutputStreamFactory.class);
        SegmentOutputStream out = mock(SegmentOutputStream.class);
        Segment segment = new Segment(scope, stream, 0);
        when(outFactory.createOutputStreamForSegment(eq(segment), any(), any(), any(DelegationTokenProvider.class)))
                .thenReturn(out);

        SegmentInputStreamFactory inFactory = mock(SegmentInputStreamFactory.class);
        EventSegmentReader in = mock(EventSegmentReader.class);
        when(inFactory.createEventReaderForSegment(eq(segment), anyInt())).thenReturn(in);
        when(in.read(anyLong())).thenReturn(null).thenReturn(serializer.serialize("testData"));

        SegmentMetadataClientFactory metaFactory = mock(SegmentMetadataClientFactory.class);
        SegmentMetadataClient metaClient = mock(SegmentMetadataClient.class);
        when(metaFactory.createSegmentMetadataClient(any(Segment.class), any(DelegationTokenProvider.class))).thenReturn(metaClient);

        CompletableFuture<SegmentInfo> neverCompleteFuture = new CompletableFuture<>();

        when(metaClient.getSegmentInfo()).thenReturn(neverCompleteFuture);

        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory,
                inFactory, streamFactory, streamFactory, metaFactory);

        @Cleanup
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, serializer,
                SynchronizerConfig.builder().build());
        // creating some dummy revision
        Revision r0 = new RevisionImpl(segment, 0L, 0);

        assertThrows(ServerTimeoutException.class, () -> client.readFrom(r0));
    }

    @Test
    public void testConditionalWrite() {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        createScopeAndStream(scope, stream, controller);

        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);
               
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, new JavaSerializer<>(), config);
        
        client.writeUnconditionally("a");
        Revision revision = client.fetchLatestRevision();
        Revision newRevision = client.writeConditionally(revision, "b");
        assertNotNull(newRevision);
        assertTrue(newRevision.compareTo(revision) > 0);
        assertEquals(newRevision, client.fetchLatestRevision());
        
        Revision failed = client.writeConditionally(revision, "fail");
        assertNull(failed);
        assertEquals(newRevision, client.fetchLatestRevision());
        
        Iterator<Entry<Revision, String>> iter = client.readFrom(revision);
        assertTrue(iter.hasNext());
        Entry<Revision, String> entry = iter.next();
        assertEquals(newRevision, entry.getKey());
        assertEquals("b", entry.getValue());
        assertFalse(iter.hasNext());
    }
    
    @Test
    public void testMark() {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        createScopeAndStream(scope, stream, controller);

        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);
            
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, new JavaSerializer<>(), config);
        
        client.writeUnconditionally("a");
        Revision ra = client.fetchLatestRevision();
        client.writeUnconditionally("b");
        Revision rb = client.fetchLatestRevision();
        client.writeUnconditionally("c");
        Revision rc = client.fetchLatestRevision();
        assertTrue(client.compareAndSetMark(null, ra));
        assertEquals(ra, client.getMark());
        assertTrue(client.compareAndSetMark(ra, rb));
        assertEquals(rb, client.getMark());
        assertFalse(client.compareAndSetMark(ra, rc));
        assertEquals(rb, client.getMark());
        assertTrue(client.compareAndSetMark(rb, rc));
        assertEquals(rc, client.getMark());
        assertTrue(client.compareAndSetMark(rc, ra));
        assertEquals(ra, client.getMark());
        assertTrue(client.compareAndSetMark(ra, null));
        assertEquals(null, client.getMark());
    }
    
    @Test
    public void testSegmentTruncation() {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        createScopeAndStream(scope, stream, controller);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);
        
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, new JavaSerializer<>(), config);
        
        Revision r0 = client.fetchLatestRevision();
        client.writeUnconditionally("a");
        Revision ra = client.fetchLatestRevision();
        client.writeUnconditionally("b");
        Revision rb = client.fetchLatestRevision();
        client.writeUnconditionally("c");
        Revision rc = client.fetchLatestRevision();
        assertEquals(r0, client.fetchOldestRevision());
        client.truncateToRevision(r0);
        assertEquals(r0, client.fetchOldestRevision());
        client.truncateToRevision(ra);
        assertEquals(ra, client.fetchOldestRevision());
        client.truncateToRevision(r0);
        assertEquals(ra, client.fetchOldestRevision());
        assertThrows(TruncatedDataException.class, () -> client.readFrom(r0));
        Iterator<Entry<Revision, String>> iterA = client.readFrom(ra);
        assertTrue(iterA.hasNext());
        Iterator<Entry<Revision, String>> iterB = client.readFrom(ra);
        assertTrue(iterB.hasNext());
        assertEquals("b", iterA.next().getValue());
        assertEquals("b", iterB.next().getValue());
        client.truncateToRevision(rb);
        assertTrue(iterA.hasNext());
        assertEquals("c", iterA.next().getValue());
        client.truncateToRevision(rc);
        assertFalse(iterA.hasNext());
        assertTrue(iterB.hasNext());
        assertThrows(TruncatedDataException.class, () -> iterB.next());
        
    }

    @Test
    public void testCreateRevisionedStreamClientError() {
        String scope = "scope";
        String stream = "stream";
        JavaSerializer<Serializable> serializer = new JavaSerializer<>();
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        Controller controller = Mockito.mock(Controller.class);

        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);
        
        SynchronizerConfig config = SynchronizerConfig.builder().build();

        // Simulate sealed stream.
        CompletableFuture<StreamSegments> result = new CompletableFuture<>();
        result.complete(new StreamSegments(new TreeMap<>()));
        when(controller.getCurrentSegments(scope, stream)).thenReturn(result);

        assertThrows(InvalidStreamException.class, () -> clientFactory.createRevisionedStreamClient(stream, serializer, config));

        // Simulate invalid stream.
        result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException());
        when(controller.getCurrentSegments(scope, stream)).thenReturn(result);

        assertThrows(InvalidStreamException.class, () -> clientFactory.createRevisionedStreamClient(stream, serializer, config));

        // Simulate null result from Controller.
        result = new CompletableFuture<>();
        result.complete(null);
        when(controller.getCurrentSegments(scope, stream)).thenReturn(result);

        assertThrows(InvalidStreamException.class, () -> clientFactory.createRevisionedStreamClient(stream, serializer, config));

    }

    @Test
    public void testSegmentSealedFromSegmentOutputStreamError() {
        String scope = "scope";
        String stream = "stream";
        // Setup Environment
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        createScopeAndStream(scope, stream, controller);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();

        // Setup mock
        SegmentOutputStreamFactory outFactory = mock(SegmentOutputStreamFactory.class);
        SegmentOutputStream out = mock(SegmentOutputStream.class);
        when(outFactory.createOutputStreamForSegment(eq(new Segment(scope, stream, 0)), any(), any(), any(DelegationTokenProvider.class)))
                .thenReturn(out);
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory,
                                                                        streamFactory, outFactory, streamFactory, streamFactory);

        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        PendingEvent event1 = PendingEvent.withoutHeader("key", ByteBufferUtils.EMPTY, writeFuture);
        PendingEvent event2 = PendingEvent.withoutHeader("key", ByteBufferUtils.EMPTY, null);
        // Two events are returned when the callback invokes getUnackedEventsOnSeal
        when(out.getUnackedEventsOnSeal()).thenReturn(Arrays.asList(event1, event2));

        @Cleanup
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, new JavaSerializer<>(),
                                                                                           SynchronizerConfig.builder().build());
        // simulate invocation of handleSegmentSealed by Segment writer.
        ((RevisionedStreamClientImpl) client).handleSegmentSealed();

        // Verify SegmentOutputStream#getUnackedEventsOnSeal is invoked.
        verify(out, times(1)).getUnackedEventsOnSeal();
        assertTrue(writeFuture.isCompletedExceptionally());
        assertThrows(SegmentSealedException.class, writeFuture::get);
    }

    @Test
    public void testTimeoutWithStreamIterator() throws Exception {
        String scope = "scope";
        String stream = "stream";
        // Setup Environment
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        createScopeAndStream(scope, stream, controller);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();

        // Setup mock
        SegmentOutputStreamFactory outFactory = mock(SegmentOutputStreamFactory.class);
        SegmentOutputStream out = mock(SegmentOutputStream.class);
        Segment segment = new Segment(scope, stream, 0);
        when(outFactory.createOutputStreamForSegment(eq(segment), any(), any(), any(DelegationTokenProvider.class)))
                .thenReturn(out);

        SegmentInputStreamFactory inFactory = mock(SegmentInputStreamFactory.class);
        EventSegmentReader in = mock(EventSegmentReader.class);
        when(inFactory.createEventReaderForSegment(eq(segment), anyInt())).thenReturn(in);
        when(in.read(anyLong())).thenReturn(null).thenReturn(serializer.serialize("testData"));

        SegmentMetadataClientFactory metaFactory = mock(SegmentMetadataClientFactory.class);
        SegmentMetadataClient metaClient = mock(SegmentMetadataClient.class);
        when(metaFactory.createSegmentMetadataClient(any(Segment.class), any(DelegationTokenProvider.class))).thenReturn(metaClient);
        when(metaClient.getSegmentInfo()).thenReturn(CompletableFuture.completedFuture(new SegmentInfo(segment, 0, 30, false, System.currentTimeMillis())));

        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory,
                inFactory, streamFactory, streamFactory, metaFactory);

        @Cleanup
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, serializer,
                SynchronizerConfig.builder().build());
        Iterator<Entry<Revision, String>> iterator = client.readFrom(new RevisionImpl(segment, 15, 1));

        assertTrue("True is expected since offset is less than end offset", iterator.hasNext());
        assertNotNull("Verify the entry is not null", iterator.next());
        verify(in, times(2)).read(anyLong());
    }

    @Test
    public void testRetryOnTimeout() throws ConnectionFailedException {
        String scope = "scope";
        String stream = "stream";
        Segment segment = new Segment(scope, stream, 0L);
        // Setup Environment
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);

        // Setup Mocks
        JavaSerializer<String> serializer = new JavaSerializer<>();

        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint
                .getPort(), connectionFactory, false);

        // Setup client connection.
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);

        // Create Scope and Stream.
        createScopeAndStream(scope, stream, controller);

        // Create mock ClientFactory.
        SegmentInputStreamFactory segInputFactory = new SegmentInputStreamFactoryImpl(controller, connectionFactory);
        SegmentOutputStreamFactory segOutputFactory = mock(SegmentOutputStreamFactory.class);
        ConditionalOutputStreamFactory condOutputFactory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        SegmentMetadataClientFactory segMetaFactory = mock(SegmentMetadataClientFactory.class);
        SegmentMetadataClient segMetaClient = mock(SegmentMetadataClient.class);
        when(segMetaFactory.createSegmentMetadataClient(eq(segment), any(DelegationTokenProvider.class)))
                .thenReturn(segMetaClient);
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, segInputFactory,
                segOutputFactory, condOutputFactory, segMetaFactory);

        RevisionedStreamClientImpl<String> client = spy((RevisionedStreamClientImpl<String>) clientFactory
                .createRevisionedStreamClient(stream, serializer,
                        SynchronizerConfig.builder().build()));
        // Override the readTimeout value for RevisionedClient to 1 second.
        doReturn(1000L).when(client).getReadTimeout();

        // Setup the SegmentMetadataClient mock
        doReturn(CompletableFuture.completedFuture(new SegmentInfo(segment, 0L, 30L, false, 1L)))
                .when(segMetaClient).getSegmentInfo();

        // Get the iterator from Revisioned Stream Client.
        Iterator<Entry<Revision, String>> iterator = client.readFrom(new RevisionImpl(segment, 15, 1));
        // since we are trying to read @ offset 15 and the writeOffset is 30L a true is returned for hasNext().
        assertTrue(iterator.hasNext());

        // Setup mock to validate a retry.
        doNothing().doAnswer(i -> {
            WireCommands.ReadSegment request = i.getArgument(0);
            ReplyProcessor rp = connectionFactory.getProcessor(endpoint);
            WireCommands.Event event = new WireCommands.Event(Unpooled.wrappedBuffer(serializer.serialize("A")));
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            event.writeFields(new DataOutputStream(bout));
            ByteBuf eventData = Unpooled.wrappedBuffer(bout.toByteArray());
            // Invoke Reply processor to simulate a successful read.
            rp.process(new WireCommands.SegmentRead(request.getSegment(), 15L, true, true, eventData, request
                    .getRequestId()));
            return null;
        }).when(c).send(any(WireCommands.ReadSegment.class));
        Entry<Revision, String> r = iterator.next();
        assertEquals("A", r.getValue());
        // Verify retries have been performed.
        verify(c, times(3)).send(any(WireCommands.ReadSegment.class));
    }

    private void createScopeAndStream(String scope, String stream, MockController controller) {
        controller.createScope(scope).join();
        controller.createStream(scope, stream, config).join();
    }
}
