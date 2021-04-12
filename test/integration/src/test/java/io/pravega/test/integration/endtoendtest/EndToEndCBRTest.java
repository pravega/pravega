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
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EndToEndCBRTest extends AbstractEndToEndTest {

    private static final long CLOCK_ADVANCE_INTERVAL = 60 * 1000000000L;

    @Test(timeout = 60000)
    public void testReaderGroupAutoRetention() throws Exception {
        String scope = "test";
        String streamName = "testReaderGroupAutoRetention";
        String groupName = "testReaderGroupAutoRetention-group";
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .retentionPolicy(RetentionPolicy.bySizeBytes(10, Long.MAX_VALUE))
                .build();
        LocalController controller = (LocalController) PRAVEGA.getLocalController();
        controller.createScope(scope).get();
        controller.createStream(scope, streamName, config).get();
        Stream stream = Stream.of(scope, streamName);
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(PRAVEGA.getControllerURI())
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        // write events
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer,
                EventWriterConfig.builder().build());
        writer.writeEvent("1", "e1").join();
        writer.writeEvent("2", "e2").join();

        // Create a ReaderGroup
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory);
        groupManager.createReaderGroup(groupName, ReaderGroupConfig
                .builder().disableAutomaticCheckpoints()
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .stream(stream).build());

        // Create a Reader
        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("reader1", groupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals("e1", read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        ReaderGroup readerGroup = groupManager.getReaderGroup(groupName);
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("Checkpoint", backgroundExecutor);
        assertFalse(checkpoint.isDone());
        read = reader.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint", read.getCheckpointName());
        assertNull(read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals("e2", read.getEvent());
        Checkpoint cpResult = checkpoint.get(5, TimeUnit.SECONDS);
        assertTrue(checkpoint.isDone());
        assertEquals("Checkpoint", cpResult.getName());
        read = reader.readNextEvent(100);
        assertNull(read.getEvent());
        assertFalse(read.isCheckpoint());

        AssertExtensions.assertEventuallyEquals(true, () -> controller.getSegmentsAtTime(new StreamImpl(scope, streamName), 0L)
                .join().values().stream().anyMatch(off -> off > 0), 30 * 1000L);
        String group2 = groupName + "2";
        groupManager.createReaderGroup(group2, ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(NameUtils.getScopedStreamName(scope, streamName)).build());
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", group2, serializer, ReaderConfig.builder().build());
        EventRead<String> eventRead2 = reader2.readNextEvent(10000);
        assertEquals("e2", eventRead2.getEvent());
    }

    @Test(timeout = 60000)
    public void testReaderGroupManualRetention() throws Exception {
        String scope = "test";
        String streamName = "testReaderGroupManualRetention";
        String groupName = "testReaderGroupManualRetention-group";
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .retentionPolicy(RetentionPolicy.bySizeBytes(10, Long.MAX_VALUE))
                .build();
        LocalController controller = (LocalController) PRAVEGA.getLocalController();
        controller.createScope(scope).get();
        controller.createStream(scope, streamName, config).get();
        Stream stream = Stream.of(scope, streamName);
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(PRAVEGA.getControllerURI())
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        // write events
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer,
                EventWriterConfig.builder().build());
        writer.writeEvent("1", "e1").join();
        writer.writeEvent("2", "e2").join();

        // Create a ReaderGroup
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory);
        groupManager.createReaderGroup(groupName, ReaderGroupConfig
                .builder().disableAutomaticCheckpoints()
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .stream(stream).build());

        // Create a Reader
        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("reader1", groupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals("e1", read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals("e2", read.getEvent());

        ReaderGroup readerGroup = groupManager.getReaderGroup(groupName);

        Map<Segment, Long> segmentMap = new HashMap<>();
        segmentMap.put(new Segment(scope, streamName, 0), 17L);
        Map<Stream, StreamCut> scResult2 = new HashMap<>();
        scResult2.put(stream, new StreamCutImpl(stream, segmentMap));

        readerGroup.updateRetentionStreamCut(scResult2);

        AssertExtensions.assertEventuallyEquals(true, () -> controller.getSegmentsAtTime(stream, 0L)
                .join().values().stream().anyMatch(off -> off > 0), 30 * 1000L);
        String group2 = groupName + "2";
        groupManager.createReaderGroup(group2, ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(NameUtils.getScopedStreamName(scope, streamName)).build());
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", group2, serializer, ReaderConfig.builder().build());
        EventRead<String> eventRead2 = reader2.readNextEvent(10000);
        assertEquals("e2", eventRead2.getEvent());
    }
}
