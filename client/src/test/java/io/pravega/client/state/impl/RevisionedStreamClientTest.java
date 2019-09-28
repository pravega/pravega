/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.state.impl;

import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.common.util.ByteBufferUtils;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
        result.complete(new StreamSegments(new TreeMap<>(), ""));
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

    private void createScopeAndStream(String scope, String stream, MockController controller) {
        controller.createScope(scope).join();
        controller.createStream(scope, stream, config).join();
    }
}
