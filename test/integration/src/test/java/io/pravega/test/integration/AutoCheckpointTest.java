/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Sequence;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.test.common.TestUtils;
import java.util.Collections;
import lombok.Cleanup;
import org.junit.Test;

import static org.junit.Assert.*;

public class AutoCheckpointTest {

    @Test(timeout = 60000)
    public void testCheckpointsOccur() throws ReinitializationRequiredException, DurableDataLogException,
                                       InterruptedException {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerName = "reader";
        String readerGroup = "group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world: ";
        String scope = "Scope1";
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                                                         .startingPosition(Sequence.MIN_VALUE)
                                                         .automaticCheckpointIntervalMillis(100)
                                                         .build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, null);
        streamManager.createReaderGroup(readerGroup, groupConfig, Collections.singleton(streamName));
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer,
                                                                             EventWriterConfig.builder().build());
        for (int i = 0; i < 100; i++) {
            producer.writeEvent(testString + i);
        }
        producer.flush();

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName, readerGroup, serializer,
                                                                      ReaderConfig.builder().build());
        int numRead = 0;
        int checkpointCount = 0;
        while (numRead < 100) {
            EventRead<String> event = reader.readNextEvent(1000);
            if (event.isCheckpoint()) {
                checkpointCount++;
            } else {
                String message = event.getEvent();
                assertEquals(testString + numRead, message);
                numRead++;
                Thread.sleep(10);
            }
        }
        assertTrue("Count was " + checkpointCount, checkpointCount > 5);
        assertTrue("Count was " + checkpointCount, checkpointCount < 20);
    }

}
