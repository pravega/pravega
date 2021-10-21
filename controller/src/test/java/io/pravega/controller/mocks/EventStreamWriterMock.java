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

import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.lang3.NotImplementedException;

/**
 * Mock EventStreamWriter.
 */
public class EventStreamWriterMock<T> implements EventStreamWriter<T> {
    BlockingQueue<T> eventList = new LinkedBlockingQueue<>();

    @Override
    public CompletableFuture<Void> writeEvent(T event) {
        eventList.add(event);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> writeEvent(String routingKey, T event) {
        eventList.add(event);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> writeEvents(String routingKey, List<T> events) {
        throw new NotImplementedException("mock doesnt require this");
    }

    @Override
    public EventWriterConfig getConfig() {
        throw new NotImplementedException("getClientConfig");
    }

    @Override
    public void flush() {
        throw new NotImplementedException("flush");
    }

    @Override
    public void close() {

    }

    public List<T> getEventList() {
        List<T> list = new ArrayList<>();
        eventList.drainTo(list);
        return list;
    }

    public EventStreamReader<T> getReader() {
        return new EventStreamReaderMock<>(eventList);
    }

    @Override
    public void noteTime(long timestamp) {
        throw new NotImplementedException("noteTime");
    }
}
