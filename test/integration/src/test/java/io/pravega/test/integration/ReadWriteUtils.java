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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.util.RetriesExhaustedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

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
            readers.add(client.createReader(String.valueOf(i), rGroup, new UTF8StringSerializer(), ReaderConfig.builder().build()));
        }

        return readers.stream().map(r -> CompletableFuture.supplyAsync(() -> {
            int count = readEvents(r, limit, 50);
            r.close();
            return count;
        })).collect(toList());
    }

    public static List<CompletableFuture<Integer>> readEvents(EventStreamClientFactory clientFactory, String readerGroup, int numReaders) {
        return readEvents(clientFactory, readerGroup, numReaders, Integer.MAX_VALUE);
    }

    @SneakyThrows
    public static <T> int readEvents(EventStreamReader<T> reader, int limit,  final int interReadWait) {
        return readEventsUntil(reader, eventRead -> eventRead.getEvent() != null || eventRead.isCheckpoint(), limit, interReadWait);
    }

    @SneakyThrows
    public static <T> int readEventsUntil(EventStreamReader<T> reader, Predicate<EventRead<T>> condition, int limit, final int interReadWait) {
        final int timeout = 1000;
        EventRead<T> event;
        int validEvents = 0;
        try {
            do {
                event = reader.readNextEvent(timeout);
                Exceptions.handleInterrupted(() -> Thread.sleep(interReadWait));
                if (event.getEvent() != null) {
                    validEvents++;
                }
            } while ((condition.test(event)) && validEvents < limit);
        } catch (TruncatedDataException e) {
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
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(),
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
