/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.state.impl;

import io.pravega.ClientFactory;
import io.pravega.common.netty.PravegaNodeUri;
import io.pravega.state.Revision;
import io.pravega.state.RevisionedStreamClient;
import io.pravega.state.SynchronizerConfig;
import io.pravega.stream.impl.ClientFactoryImpl;
import io.pravega.stream.impl.JavaSerializer;
import io.pravega.stream.mock.MockConnectionFactoryImpl;
import io.pravega.stream.mock.MockController;
import io.pravega.stream.mock.MockSegmentStreamFactory;

import java.util.Iterator;
import java.util.Map.Entry;

import lombok.Cleanup;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RevisionedStreamClientTest {
    private static final int SERVICE_PORT = 12345;
    
    @Test
    public void testWriteWhileReading() {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory);
        
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, new JavaSerializer<>(), config);
        
        Revision initialRevision = client.fetchRevision();
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
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory);
        
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, new JavaSerializer<>(), config);
        
        client.writeUnconditionally("a");
        Revision revision = client.fetchRevision();
        Revision newRevision = client.writeConditionally(revision, "b");
        assertNotNull(newRevision);
        assertTrue(newRevision.compareTo(revision) > 0);
        assertEquals(newRevision, client.fetchRevision());
        
        Revision failed = client.writeConditionally(revision, "fail");
        assertNull(failed);
        assertEquals(newRevision, client.fetchRevision());
        
        Iterator<Entry<Revision, String>> iter = client.readFrom(revision);
        assertTrue(iter.hasNext());
        Entry<Revision, String> entry = iter.next();
        assertEquals(newRevision, entry.getKey());
        assertEquals("b", entry.getValue());
        assertFalse(iter.hasNext());
    }
}
