/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.eventProcessor.ControllerEvent;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.Position;

/**
 * Event processor interface.
 */
public abstract class EventProcessor<T extends ControllerEvent> {

    @FunctionalInterface
    public interface Checkpointer {
        void store(Position position) throws CheckpointStoreException;
    }

    protected Checkpointer checkpointer;

    protected EventStreamWriter<T> selfWriter;

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

}
