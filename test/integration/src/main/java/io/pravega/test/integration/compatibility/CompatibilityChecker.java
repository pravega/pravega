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
package io.pravega.test.integration.compatibility;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
public class CompatibilityChecker {
    public  URI controllerURI;
    public  StreamManager streamManager;
    private static final int READER_TIMEOUT_MS = 2000;
    public StreamConfiguration streamConfig;

    public  void setUp() {
        controllerURI = URI.create("tcp://localhost:9090");
        streamManager = StreamManager.create(controllerURI);
        streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
    }

    /**
    * This method is Checking the working of the read and write event of the stream.
    * Here we are trying to create a stream and a scope, and then we are writing a couple of events.
    * And then reading those written event from the stream.
    */
    public void CheckWriteAndReadEvent() {
        String scopeName = "write-and-read-test-scope";
        String streamName = "write-and-read-test-stream";
        streamManager.createScope(scopeName);
        streamManager.createStream(scopeName, streamName, streamConfig);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(), EventWriterConfig.builder().build());
        String readerGroupId = UUID.randomUUID().toString().replace("-", "");
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        @Cleanup
        EventStreamReader<String> reader =  clientFactory.createReader("reader-1", readerGroupId, new UTF8StringSerializer(), ReaderConfig.builder().build());
        // Writing 10 Events to the stream
        for( int event = 0; event < 10; event++)
            writer.writeEvent("event test" + event);
        log.info("Reading all the events from {}, {}", scopeName, streamName);
        EventRead<String> event = null;
        // Reading all those 10 events from the stream
        do {
            try {
                event = reader.readNextEvent(READER_TIMEOUT_MS);
                if (event.getEvent() != null) {
                    System.out.format("Read event '%s'%n", event.getEvent());
                }
            } catch (ReinitializationRequiredException e) {
                //There are certain circumstances where the reader needs to be reinitialized
                e.printStackTrace();
            }
        } while (event.getEvent() != null);
        log.info("No more events from {}, {}", scopeName, streamName);
    }

    /**
    * This method is checking the Truncation feature of the stream.
    * Here we are creating some stream and writing events to that stream.
    * After that we are truncating to that stream and validating whether the stream truncation is working properly.
    */
    public void checkTruncationOfStream() {
        String scopeName = "truncate-test-scope";
        String streamName = "truncate-test-stream";
        streamManager.createScope(scopeName);
        streamManager.createStream(scopeName, streamName, streamConfig);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());

        String readerGroupId = UUID.randomUUID().toString().replace("-", "");

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupId);

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,new UTF8StringSerializer(), EventWriterConfig.builder().build());
        for( int event = 0; event < 2; event++)
            writer.writeEvent(String.valueOf(event)).join();
        EventStreamReader<String> reader = clientFactory.createReader(readerGroupId + "1", readerGroupId,
                new UTF8StringSerializer(), ReaderConfig.builder().build());
        assertEquals(reader.readNextEvent(5000).getEvent(), "0");
        reader.close();

        // Create a Checkpoint, get StreamCut and truncate the Stream at that point.
        StreamCut streamCut = readerGroup.getStreamCuts().get(Stream.of(scopeName, streamName));
        assertTrue(streamManager.truncateStream(scopeName, streamName, streamCut));

        // Verify that a new reader reads from event 1 onwards.
        final String newReaderGroupName = readerGroupId + "new";
        readerGroupManager.createReaderGroup(newReaderGroupName, ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build());
        @Cleanup
        final EventStreamReader<String> newReader = clientFactory.createReader(newReaderGroupName + "2",
                newReaderGroupName, new UTF8StringSerializer(), ReaderConfig.builder().build());

        assertEquals("Expected read event: ", "1", newReader.readNextEvent(5000).getEvent());
    }

    /**
    * This method is trying to check the stream seal feature of the stream.
    * Here we are creating a stream and writing some event to it.
    * And then sealing that stream by using stream manager and validating the stream whether sealing worked properly.
    */
    public  void checkSealStream() {
        String scopeName = "stream-seal-test-scope";
        String streamName = "stream-seal-test-stream";
        streamManager.createScope(scopeName);
        streamManager.createStream(scopeName, streamName, streamConfig);
        assertTrue(streamManager.checkStreamExists(scopeName, streamName));
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(), EventWriterConfig.builder().build());
        // Before sealing of the stream it must write to the existing stream.
        writer.writeEvent("0").join();
        assertTrue(streamManager.sealStream(scopeName, streamName));
        // It must complete Exceptionally because stream is sealed already.
        assertTrue(writer.writeEvent("1").completeExceptionally(new IllegalStateException()));
    }

    /**
     * This method is trying to create and delete a scope.
     * And also validating it.
     * @throws DeleteScopeFailedException
     */
    public  void checkDeleteScope() throws DeleteScopeFailedException {
        String scopeName = "scope-delete-test-scope";
        String streamName = "scope-delete-test-stream";
        streamManager.createScope(scopeName);
        streamManager.createStream(scopeName, streamName, streamConfig);
        assertTrue(streamManager.checkScopeExists(scopeName));

        assertTrue(streamManager.deleteScopeRecursive(scopeName));

        //validating whether the scope deletion is successful, expression must be false.
        assertFalse(streamManager.checkScopeExists(scopeName));
    }

    /**
     * This method is trying to create a stream and writing a couple of events.
     * Then deleting the newly created stream and validating it.
     */
    public void checkStreamDelete() {
        String scopeName = "stream-delete-test-scope";
        String streamName = "stream-delete-test-stream";
        streamManager.createScope(scopeName);
        streamManager.createStream(scopeName, streamName, streamConfig);
        assertTrue(streamManager.checkStreamExists(scopeName, streamName));
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName,ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(), EventWriterConfig.builder().build());
        // Before sealing of the stream it must write to the existing stream.
        writer.writeEvent("0").join();
        assertTrue(streamManager.sealStream(scopeName, streamName));

        assertTrue(streamManager.deleteStream(scopeName, streamName));
        // It must complete Exceptionally because stream is sealed already.
        assertTrue(writer.writeEvent("1").completeExceptionally(new IllegalStateException()));

    }

    public static void main(String[] args) throws DeleteScopeFailedException {
        CompatibilityChecker compatibilityChecker = new CompatibilityChecker();
        compatibilityChecker.setUp();
        compatibilityChecker.CheckWriteAndReadEvent();
        compatibilityChecker.checkTruncationOfStream();
        compatibilityChecker.checkSealStream();
        compatibilityChecker.checkDeleteScope();
        compatibilityChecker.checkStreamDelete();
    }
}