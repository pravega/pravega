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
package io.pravega.controller.eventProcessor.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.client.stream.Position;

import java.util.concurrent.CompletableFuture;

/**
 * Event processor interface.
 */
public abstract class EventProcessor<T extends ControllerEvent> {

    @FunctionalInterface
    public interface Checkpointer {
        void store(Position position) throws CheckpointStoreException;
    }

    @FunctionalInterface
    public interface Writer<T extends ControllerEvent> {
        CompletableFuture<Void> write(T event);
    }

    Checkpointer checkpointer;

    Writer<T> selfWriter;

    /**
     * AbstractActor initialization hook that is called before actor starts receiving events.
     */
    protected void beforeStart() { }

    /**
     * User defined event processing logic.
     * @param event Event received from Pravega Stream.
     * @param position Received event's position.
     */
    protected abstract void process(T event, Position position);

    /**
     * AbstractActor shutdown hook that is called on shut down.
     */
    protected void afterStop() { }

    /**
     * AbstractActor preRestart hook that is called before actor restarts
     * after recovering from a failure. After this method call, preStart is
     * called before the Actor starts again.
     * @param t Throwable error.
     * @param event Event being processed when error is thrown.
     */
    protected void beforeRestart(Throwable t, T event) { }

    /**
     * Returns a handle to checkpointer which can be used to store reader position.
     * @return a handle to checkpointer which can be used to store reader position.
     */
    protected Checkpointer getCheckpointer() {
        return this.checkpointer;
    }

    /**
     * Returns a stream writer that can be used to write events to the underlying event stream.
     * @return a stream writer that can be used to write events to the underlying event stream.
     */
    @VisibleForTesting
    public Writer<T> getSelfWriter() {
        return selfWriter;
    }
}
