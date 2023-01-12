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

/**
 * CompatibilityChecker class is exercising all APIs we have in Pravega Samples.
 * This class can be used for compatibility check against a server endpoint passed by parameter.
 */
@Slf4j
public class CompatibilityChecker {
    private static final int READER_TIMEOUT_MS = 2000;
    private URI controllerURI;
    private StreamManager streamManager;
    private StreamConfiguration streamConfig;

    private void setUp(String uri) {
        controllerURI = URI.create(uri);
        streamManager = StreamManager.create(controllerURI);
        streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
    }

    /**
    * This method is Checking the working of the read and write event of the stream.
    * Here we are trying to create a stream and a scope, and then we are writing a couple of events.
    * And then reading those written event from the stream.
    */
    private void checkWriteAndReadEvent() {
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
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        @Cleanup
        EventStreamReader<String> reader =  clientFactory.createReader("reader-1", readerGroupId, new UTF8StringSerializer(), ReaderConfig.builder().build());
        int writeCount = 0;
        // Writing 10 Events to the stream
        for (int event = 1; event <= 10; event++) {
            writer.writeEvent("event test" + event);
            writeCount++;
        }
        log.info("Reading all the events from {}, {}", scopeName, streamName);
        int eventNumber = 1;
        int readCount = 0;
        EventRead<String> event = reader.readNextEvent(READER_TIMEOUT_MS);
        // Reading written Events
        while (event.getEvent() != null) {
            assertEquals("event test" + eventNumber, event.getEvent());
            event = reader.readNextEvent(READER_TIMEOUT_MS);
            readCount++;
            eventNumber++;
        }
        // Validating the readCount and writeCount
        assertEquals(readCount, writeCount);
        log.info("No more events from {}, {}", scopeName, streamName);
    }

    /**
    * This method is checking the Truncation feature of the stream.
    * Here we are creating some stream and writing events to that stream.
    * After that we are truncating to that stream and validating whether the stream truncation is working properly.
    */
    private void checkTruncationOfStream() {
        String scopeName = "truncate-test-scope";
        String streamName = "truncate-test-stream";
        streamManager.createScope(scopeName);
        streamManager.createStream(scopeName, streamName, streamConfig);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());

        String readerGroupId = UUID.randomUUID().toString().replace("-", "");

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        @Cleanup
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupId);

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(), EventWriterConfig.builder().build());
        for (int event = 0; event < 2; event++) {
            writer.writeEvent(String.valueOf(event)).join();
        }
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerGroupId + "1", readerGroupId,
                new UTF8StringSerializer(), ReaderConfig.builder().build());
        assertEquals(reader.readNextEvent(5000).getEvent(), "0");
        reader.close();

        // Get StreamCut and truncate the Stream at that point.
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
    private void checkSealStream() {
        String scopeName = "stream-seal-test-scope";
        String streamName = "stream-seal-test-stream";
        streamManager.createScope(scopeName);
        streamManager.createStream(scopeName, streamName, streamConfig);
        assertTrue(streamManager.checkStreamExists(scopeName, streamName));
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(), EventWriterConfig.builder().build());
        String readerGroupId = UUID.randomUUID().toString().replace("-", "");

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerGroupId + "1", readerGroupId,
                new UTF8StringSerializer(), ReaderConfig.builder().build());
        // Before sealing of the stream it must write to the existing stream.
        writer.writeEvent("0").join();
        assertTrue(streamManager.sealStream(scopeName, streamName));
        // It must complete Exceptionally because stream is sealed already.
        assertTrue(writer.writeEvent("1").completeExceptionally(new IllegalStateException()));
        // But there should not be any problem while reading from a sealed stream.
        assertEquals(reader.readNextEvent(READER_TIMEOUT_MS).getEvent(), "0");
    }

    /**
     * This method attempts to create, delete and validate the existence of a scope.
     * @throws DeleteScopeFailedException if unable to seal and delete a stream.
     */
    private void checkDeleteScope() throws DeleteScopeFailedException {
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
    private void checkStreamDelete() {
        String scopeName = "stream-delete-test-scope";
        String streamName = "stream-delete-test-stream";
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

        assertTrue(streamManager.deleteStream(scopeName, streamName));
        assertFalse(streamManager.checkStreamExists(scopeName, streamName));
    }

    public static void main(String[] args) throws DeleteScopeFailedException {
        String uri = System.getProperty("controllerUri");
        if (uri == null) {
            log.error("Input correct controller URI (e.g., \"tcp://localhost:9090\")");
            System.exit(1);
        }
        CompatibilityChecker compatibilityChecker = new CompatibilityChecker();
        compatibilityChecker.setUp(uri);
        compatibilityChecker.checkWriteAndReadEvent();
        compatibilityChecker.checkTruncationOfStream();
        compatibilityChecker.checkSealStream();
        compatibilityChecker.checkDeleteScope();
        compatibilityChecker.checkStreamDelete();
        System.exit(0);
    }
}