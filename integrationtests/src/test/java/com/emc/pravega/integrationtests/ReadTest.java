package com.emc.pravega.integrationtests;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.emc.pravega.common.netty.CommandDecoder;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.mocks.InMemoryServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;

import static org.junit.Assert.*;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;

public class ReadTest {
    private Level originalLevel;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        originalLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);

        this.serviceBuilder = new InMemoryServiceBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.getContainerManager().initialize(Duration.ofMinutes(1)).get();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
        ResourceLeakDetector.setLevel(originalLevel);
    }

    @Test
    public void testReadDirectlyFromStore() throws InterruptedException, ExecutionException, IOException {
        String segmentName = "testReadFromStore";
        int entries = 10;
        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        UUID clientId = UUID.randomUUID();

        StreamSegmentStore segmentStore = serviceBuilder.createStreamSegmentService();
        
        fillStoreForSegment(segmentName, clientId, data, entries, segmentStore);

        ReadResult result = segmentStore.read(segmentName, 0, entries*data.length, Duration.ZERO).get();
        int count = 0; 
        while (result.hasNext()) {
            ReadResultEntry entry = result.next();
            ReadResultEntryType type = entry.getType();
            assertEquals(ReadResultEntryType.Cache, type);
            ReadResultEntryContents contents = entry.getContent().get();
            assertEquals(data.length, contents.getLength());
            byte[] entryData = new byte[data.length];
            contents.getData().read(entryData);
            assertArrayEquals(data, entryData);
            count++;
        }
        assertEquals(entries, count);
    }
    
    @Test
    public void testReceivingReadCall() throws Exception {
        String segmentName = "testReceivingReadCall";
        int entries = 10;
        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        UUID clientId = UUID.randomUUID();
        CommandDecoder decoder = new CommandDecoder();

        StreamSegmentStore segmentStore = serviceBuilder.createStreamSegmentService();
        
        fillStoreForSegment(segmentName, clientId, data, entries, segmentStore);
        
        EmbeddedChannel channel = AppendTest.createChannel(segmentStore);
        
        SegmentRead result = (SegmentRead) AppendTest.sendRequest(channel, decoder, new ReadSegment(segmentName, 0, 10000));
        
        assertEquals(result.getSegment(), segmentName);
        assertEquals(result.getOffset(), 0);
        assertTrue(result.isAtTail());
        assertFalse(result.isEndOfSegment());
        
        ByteBuffer expected = ByteBuffer.allocate(entries * data.length);
        for (int i=0;i<entries;i++) {
            expected.put(data);
        }
        assertEquals(expected, result.getData());
    }
    

    private void fillStoreForSegment(String segmentName, UUID clientId, byte[] data, int numEntries,
            StreamSegmentStore segmentStore) {
        segmentStore.createStreamSegment(segmentName, Duration.ZERO);
        for (int eventNumber = 1; eventNumber <= numEntries; eventNumber++) {
            AppendContext appendContext = new AppendContext(clientId, eventNumber);
            try {
                segmentStore.append(segmentName, data, appendContext, Duration.ZERO).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
