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

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.shared.controller.event.ControllerEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Data;
import org.apache.commons.lang3.NotImplementedException;

/**
 * Mock EventStreamWriter.
 */
@Data
public class ControllerEventStreamWriterMock implements EventStreamWriter<ControllerEvent> {
    private final StreamRequestHandler streamRequestHandler;
    private final ScheduledExecutorService executor;

    @Override
    public CompletableFuture<Void> writeEvent(ControllerEvent event) {
        Futures.delayedFuture(() ->
                streamRequestHandler.processEvent(event), 1000, executor);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> writeEvent(String routingKey, ControllerEvent event) {
        return writeEvent(event);
    }

    @Override
    public CompletableFuture<Void> writeEvents(String routingKey, List<ControllerEvent> events) {
        throw new NotImplementedException("mock doesnt require this");
    }

    @Override
    public EventWriterConfig getConfig() {
        return null;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public void noteTime(long timestamp) {
        
    }
}
