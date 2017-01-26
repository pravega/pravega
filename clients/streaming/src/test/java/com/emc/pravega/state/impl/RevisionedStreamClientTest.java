package com.emc.pravega.state.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.RevisionedStreamClient;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockConnectionFactoryImpl;
import com.emc.pravega.stream.mock.MockController;
import com.emc.pravega.stream.mock.MockSegmentStreamFactory;

import java.util.Iterator;
import java.util.Map.Entry;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RevisionedStreamClientTest {
    
    
    @Test
    public void testWriteWhileReading() {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 1234);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, streamFactory, streamFactory);
        
        SynchronizerConfig config = new SynchronizerConfig(null, null);
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, new JavaSerializer<>(), config);
        
        Revision initialRevision = client.getCurrentRevision();
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
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 1234);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, streamFactory, streamFactory);
        
        SynchronizerConfig config = new SynchronizerConfig(null, null);
        RevisionedStreamClient<String> client = clientFactory.createRevisionedStreamClient(stream, new JavaSerializer<>(), config);
        
        client.writeUnconditionally("a");
        Revision revision = client.getCurrentRevision();
        Revision newRevision = client.writeConditionally(revision, "b");
        assertNotNull(newRevision);
        assertTrue(newRevision.compareTo(revision) > 0);
        assertEquals(newRevision, client.getCurrentRevision());
        
        Revision failed = client.writeConditionally(revision, "fail");
        assertNull(failed);
        assertEquals(newRevision, client.getCurrentRevision());
        
        Iterator<Entry<Revision, String>> iter = client.readFrom(revision);
        assertTrue(iter.hasNext());
        Entry<Revision, String> entry = iter.next();
        assertEquals(newRevision, entry.getKey());
        assertEquals("b", entry.getValue());
        assertFalse(iter.hasNext());
    }
}
