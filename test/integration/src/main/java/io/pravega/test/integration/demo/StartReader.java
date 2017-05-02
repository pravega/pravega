/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.demo;

import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockStreamManager;

import java.util.Collections;
import java.util.UUID;

import lombok.Cleanup;

public class StartReader {

    private static final String READER_GROUP = "ExampleReaderGroup";

    public static void main(String[] args) throws Exception {
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(StartLocalService.SCOPE,
                                                                "localhost",
                                                                StartLocalService.PORT);
        streamManager.createScope(StartLocalService.SCOPE);
        streamManager.createStream(StartLocalService.SCOPE, StartLocalService.STREAM_NAME, null);
        streamManager.createReaderGroup(READER_GROUP,
                                        ReaderGroupConfig.builder().startingTime(0).build(),
                                        Collections.singleton(StartLocalService.STREAM_NAME));
        EventStreamReader<String> reader = streamManager.getClientFactory().createReader(UUID.randomUUID().toString(),
                                                                                         READER_GROUP,
                                                                                         new JavaSerializer<>(),
                                                                                         ReaderConfig.builder().build());
        for (int i = 0; i < 20; i++) {
            String event = reader.readNextEvent(60000).getEvent();
            System.err.println("Read event: " + event);
        }
        reader.close();
        System.exit(0);
    }
}
