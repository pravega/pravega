/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.mocks;

import io.pravega.stream.EventPointer;
import io.pravega.stream.EventRead;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.ReaderConfig;
import io.pravega.stream.ReinitializationRequiredException;
import io.pravega.stream.impl.EventReadImpl;
import io.pravega.stream.impl.segment.NoSuchEventException;
import lombok.SneakyThrows;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Mock EventStreamReader.
 */
public class EventStreamReaderMock<T> implements EventStreamReader<T> {
    BlockingQueue<T> queue;

    public EventStreamReaderMock(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    @Override
    @SneakyThrows
    public EventRead<T> readNextEvent(long timeout) throws ReinitializationRequiredException {
        T event = queue.poll(timeout, TimeUnit.MILLISECONDS);
        return new EventReadImpl<>(null, event, null, null, null);
    }

    @Override
    public ReaderConfig getConfig() {
        return null;
    }

    @Override
    public T read(EventPointer pointer) throws NoSuchEventException {
        return null;
    }

    @Override
    public void close() {
    }
}
