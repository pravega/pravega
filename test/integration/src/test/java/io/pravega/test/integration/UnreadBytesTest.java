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
package io.pravega.test.integration;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class UnreadBytesTest extends ThreadPooledTestSuite {
    
    @ClassRule
    public static final PravegaResource PRAVEGA = new PravegaResource();

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Test(timeout = 50000)
    public void testUnreadBytes() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                .build();
        String streamName = "testUnreadBytes";
        Controller controller = PRAVEGA.getLocalController();
        controller.createScope("unreadbytes").get();
        controller.createStream("unreadbytes", streamName, config).get();

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope("unreadbytes", ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        String group = "testUnreadBytes-group";
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope("unreadbytes", ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build());
        groupManager.createReaderGroup(group, ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream("unreadbytes/" + streamName).build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup(group);

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", group, new JavaSerializer<>(),
                ReaderConfig.builder().build());
        long unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 0);

        writer.writeEvent("0", "data of size 30").get();
        writer.writeEvent("0", "data of size 30").get();

        EventRead<String> firstEvent = reader.readNextEvent(15000);
        EventRead<String> secondEvent = reader.readNextEvent(15000);
        assertNotNull(firstEvent);
        assertEquals("data of size 30", firstEvent.getEvent());
        assertNotNull(secondEvent);
        assertEquals("data of size 30", secondEvent.getEvent());

        // trigger a checkpoint.
        CompletableFuture<Checkpoint> chkPointResult = readerGroup.initiateCheckpoint("test", executorService());
        EventRead<String> chkpointEvent = reader.readNextEvent(15000);
        assertEquals("test", chkpointEvent.getCheckpointName());
        
        EventRead<String> emptyEvent = reader.readNextEvent(100);
        assertEquals(false, emptyEvent.isCheckpoint());
        assertEquals(null, emptyEvent.getEvent());
        chkPointResult.join();

        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 0);

        writer.writeEvent("0", "data of size 30").get();
        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bytes: " + unreadBytes, unreadBytes == 30);
    }


    @Test(timeout = 50000)
    public void testUnreadBytesWithEndStreamCuts() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        String streamName = "testUnreadBytesWithEndStreamCuts";
        Controller controller = PRAVEGA.getLocalController();
        controller.createScope("unreadbytes").get();
        controller.createStream("unreadbytes", streamName, config).get();

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope("unreadbytes", ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        //Write just 2 events to simplify simulating a checkpoint.
        writer.writeEvent("0", "data of size 30").get();
        writer.writeEvent("0", "data of size 30").get();

        String group = "testUnreadBytesWithEndStreamCuts-group";
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope("unreadbytes", ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build());
        //create a bounded reader group.
        groupManager.createReaderGroup(group, ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().stream("unreadbytes/" + streamName, StreamCut.UNBOUNDED,
                        getStreamCut(streamName, 90L, 0)).build());

        ReaderGroup readerGroup = groupManager.getReaderGroup(group);
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", group, new JavaSerializer<>(),
                ReaderConfig.builder().build());

        EventRead<String> firstEvent = reader.readNextEvent(15000);
        EventRead<String> secondEvent = reader.readNextEvent(15000);
        assertNotNull(firstEvent);
        assertEquals("data of size 30", firstEvent.getEvent());
        assertNotNull(secondEvent);
        assertEquals("data of size 30", secondEvent.getEvent());

        // trigger a checkpoint.
        CompletableFuture<Checkpoint> chkPointResult = readerGroup.initiateCheckpoint("test", executorService());
        EventRead<String> chkpointEvent = reader.readNextEvent(15000);
        assertEquals("test", chkpointEvent.getCheckpointName());
        
        EventRead<String> emptyEvent = reader.readNextEvent(100);
        assertEquals(false, emptyEvent.isCheckpoint());
        assertEquals(null, emptyEvent.getEvent());
        
        chkPointResult.join();

        //Writer events, to ensure 120Bytes are written.
        writer.writeEvent("0", "data of size 30").get();
        writer.writeEvent("0", "data of size 30").get();

        long unreadBytes = readerGroup.getMetrics().unreadBytes();
        //Ensure the endoffset of 90 Bytes is taken into consideration when computing unread
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 30);
    }

    @Test
    public void testUnreadBytesWithCheckpointsAndStreamCuts() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                .build();
        String streamName = "testUnreadBytesWithCheckpointsAndStreamCuts";
        Controller controller = PRAVEGA.getLocalController();
        controller.createScope("unreadbytes").get();
        controller.createStream("unreadbytes", streamName, config).get();

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope("unreadbytes", ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        String group = "testUnreadBytesWithCheckpointsAndStreamCuts-group";
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope("unreadbytes",  ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build());
        groupManager.createReaderGroup(group, ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream("unreadbytes/" + streamName).build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup(group);

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", group, new JavaSerializer<>(),
                ReaderConfig.builder().build());
        long unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 0);

        writer.writeEvent("0", "data of size 30").get();
        writer.writeEvent("0", "data of size 30").get();

        EventRead<String> firstEvent = reader.readNextEvent(15000);
        EventRead<String> secondEvent = reader.readNextEvent(15000);
        assertNotNull(firstEvent);
        assertEquals("data of size 30", firstEvent.getEvent());
        assertNotNull(secondEvent);
        assertEquals("data of size 30", secondEvent.getEvent());

        // trigger a checkpoint.
        CompletableFuture<Checkpoint> chkPointResult = readerGroup.initiateCheckpoint("test", executorService());
        EventRead<String> chkpointEvent = reader.readNextEvent(15000);
        assertEquals("test", chkpointEvent.getCheckpointName());

        EventRead<String> emptyEvent = reader.readNextEvent(100);
        assertEquals(false, emptyEvent.isCheckpoint());
        assertEquals(null, emptyEvent.getEvent());
        chkPointResult.join();

        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 0);

        // starting from checkpoint "test", data of size 30 is read
        writer.writeEvent("0", "data of size 30").get();
        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bytes: " + unreadBytes, unreadBytes == 30);

        // trigger a stream-cut
        CompletableFuture<Map<Stream, StreamCut>> scResult = readerGroup.generateStreamCuts(executorService());
        EventRead<String> scEvent = reader.readNextEvent(15000);

        reader.readNextEvent(100);

        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 30);

        // starting from checkpoint "test", data of size 60 is written => stream-cut does not change last checkpointed position
        writer.writeEvent("0", "data of size 30").get();
        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bytes: " + unreadBytes, unreadBytes == 60);
    }

    /*
     * Test method to create StreamCuts. In the real world StreamCuts are obtained via the Pravega client apis.
     */
    private StreamCut getStreamCut(String streamName, long offset, int... segmentNumbers) {
        ImmutableMap.Builder<Segment, Long> builder = ImmutableMap.<Segment, Long>builder();
        Arrays.stream(segmentNumbers).forEach(seg -> {
            builder.put(new Segment("unreadbytes", streamName, seg), offset);
        });

        return new StreamCutImpl(Stream.of("unreadbytes", streamName), builder.build());
    }
}
