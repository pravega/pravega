/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.impl.JavaSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import static java.util.stream.Collectors.toList;

/**
 * Abstract class that provides convenient event read/write methods for testing purposes.
 */
@Slf4j
abstract class AbstractReadWriteTest {

    private static final int READ_TIMEOUT = 1000;

    void writeEvents(ClientFactory clientFactory, String streamName, int totalEvents) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < totalEvents; i++) {
            writer.writeEvent(streamName + String.valueOf(i)).join();
            log.debug("Writing event: {} to stream {}.", streamName + String.valueOf(i), streamName);
        }
    }

    List<CompletableFuture<Integer>> readEventFutures(ClientFactory client, String rGroup, int numReaders) {
        List<EventStreamReader<String>> readers = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            readers.add(client.createReader(String.valueOf(i), rGroup, new JavaSerializer<>(), ReaderConfig.builder().build()));
        }

        return readers.stream().map(r -> CompletableFuture.supplyAsync(() -> readEvents(r))).collect(toList());
    }

    private <T> int readEvents(EventStreamReader<T> reader) {
        EventRead<T> event = null;
        int validEvents = 0;
        boolean reinitializationRequired;
        do {
            try {
                event = reader.readNextEvent(READ_TIMEOUT);
                log.debug("Read event result in readEvents: {}.", event.getEvent());
                if (event.getEvent() != null) {
                    validEvents++;
                }
                reinitializationRequired = false;
            } catch (ReinitializationRequiredException e) {
                log.warn("Reinitialization of readers required: {}.", e);
                reinitializationRequired = true;
            }
        } while (reinitializationRequired || event.getEvent() != null || event.isCheckpoint());

        reader.close();
        return validEvents;
    }
}
