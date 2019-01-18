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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.util.RetriesExhaustedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.toList;

/**
 * Class to provide handy read/write methods for integration tests.
 */
@Slf4j
public final class ReadWriteUtils {

    public static List<CompletableFuture<Integer>> readEvents(EventStreamClientFactory client, String rGroup, int numReaders, int limit) {
        List<EventStreamReader<String>> readers = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            readers.add(client.createReader(String.valueOf(i), rGroup, new JavaSerializer<>(), ReaderConfig.builder().build()));
        }

        return readers.stream().map(r -> CompletableFuture.supplyAsync(() -> readEvents(r, limit))).collect(toList());
    }

    public static List<CompletableFuture<Integer>> readEvents(EventStreamClientFactory clientFactory, String readerGroup, int numReaders) {
        return readEvents(clientFactory, readerGroup, numReaders, Integer.MAX_VALUE);
    }

    @SneakyThrows
    private static <T> int readEvents(EventStreamReader<T> reader, int limit) {
        final int timeout = 1000;
        final int interReadWait = 50;
        EventRead<T> event;
        int validEvents = 0;
        try {
            do {
                event = reader.readNextEvent(timeout);
                Exceptions.handleInterrupted(() -> Thread.sleep(interReadWait));
                if (event.getEvent() != null) {
                    validEvents++;
                }
            } while ((event.getEvent() != null || event.isCheckpoint()) && validEvents < limit);

            reader.close();
        } catch (TruncatedDataException e) {
            reader.close();
            throw new TruncatedDataException(e.getCause());
        } catch (RuntimeException e) {
            if (e.getCause() instanceof RetriesExhaustedException) {
                throw new RetriesExhaustedException(e.getCause());
            } else {
                throw e;
            }
        }

        return validEvents;
    }

    public static void writeEvents(EventStreamClientFactory clientFactory, String streamName, int totalEvents, int offset) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = offset; i < totalEvents; i++) {
            writer.writeEvent(String.valueOf(i)).join();
            log.info("Writing event: {} to stream {}", i, streamName);
        }
    }

    public static void writeEvents(EventStreamClientFactory clientFactory, String streamName, int totalEvents) {
        writeEvents(clientFactory, streamName, totalEvents, 0);
    }
}
