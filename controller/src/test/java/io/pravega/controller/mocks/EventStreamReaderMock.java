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
package io.pravega.controller.mocks;

import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.impl.EventReadImpl;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;

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
    public EventRead<T> readNextEvent(long timeoutMillis) throws ReinitializationRequiredException {
        T event = queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        return new EventReadImpl<>(event, null, null, null);
    }

    @Override
    public ReaderConfig getConfig() {
        return null;
    }

    @Override
    public T fetchEvent(EventPointer pointer) throws NoSuchEventException {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public TimeWindow getCurrentTimeWindow(Stream stream) {
        return null;
    }

    @Override
    public void closeAt(Position position) {
    }
}
