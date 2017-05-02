/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
    @SneakyThrows(value = InterruptedException.class)
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
