/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.eventProcessor.CheckpointStore;
import com.emc.pravega.controller.eventProcessor.CheckpointStoreException;
import com.emc.pravega.controller.eventProcessor.Decider;
import com.emc.pravega.controller.eventProcessor.EventProcessorInitException;
import com.emc.pravega.controller.eventProcessor.EventProcessorReinitException;
import com.emc.pravega.controller.eventProcessor.Props;
import com.emc.pravega.controller.eventProcessor.StreamEvent;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.Position;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Service;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;

/**
 * This is an internal class that embeds the following.
 * 1. Event processor instance.
 * 2. Checkpoint state encapsulating checkpoint persistence logic.
 * 3. A reader reference that is part of the reader group associated
 *    with the EventProcessor group to which this event processor belongs.
 * 4. A delegate, which provides a single thread of execution for invoking
 *    the event processor methods like process, beforeStart, afterStop, etc.
 *
 * This object manages life cycle of an event processor by invoking it's beforeStart, process, afterStop methods.
 *
 * @param <T> Event type parameter.
 */
@Slf4j
class EventProcessorCell<T extends StreamEvent> {

    private final EventStreamReader<T> reader;

    @VisibleForTesting
    @Getter(value = AccessLevel.PACKAGE)
    private EventProcessor<T> actor;

    /**
     * Event processor cell encapsulates a delegate which extends AbstractExecutionThreadService.
     * This delegate provides a single thread of execution for the event processor.
     * This prevents sub-classes of EventProcessor from controlling EventProcessor's lifecycle.
     */
    private final Service delegate;

    private final CheckpointState state;

    private class Delegate extends AbstractExecutionThreadService {

        private final Props<T> props;
        private EventRead<T> event;

        Delegate(Props<T> props) {
            this.props = props;
        }

        @Override
        protected final void startUp() {
            try {
                actor.beforeStart();
            } catch (Exception e) {
                log.warn("Failed while executing preStart for event processor " + this, e);
                handleException(new EventProcessorInitException(actor, e));
            }
        }

        @Override
        protected final void run() throws Exception {
            final long defaultTimeout = Long.MAX_VALUE;

            while (isRunning()) {
                try {
                    event = reader.readNextEvent(defaultTimeout);

                    // invoke the user specified event processing method
                    actor.process(event.getEvent());

                    // possibly persist event position
                    state.store(event.getPosition());

                } catch (Exception e) {
                    handleException(e);
                }
            }

        }

        @Override
        protected final void shutDown() throws Exception {
            try {
                actor.afterStop();
            } catch (Exception e) {
                // Error encountered while cleanup is just logged.
                // AbstractExecutionThreadService shall transition the service to failed state.
                log.warn("Failed while executing afterStop for event processor " + this, e);
                throw e;
            } finally {

                // If exception is thrown in any of the following operations, it is just logged.
                // Some other controller process is responsible for cleaning up reader and its position object

                // First close the reader, which implicitly notifies reader position to the reader group
                reader.close();

                // Next, clean up the reader and its position from checkpoint store
                state.stop();
            }
        }

        private void restart(Throwable error, T event) {
            try {

                actor.beforeRestart(error, event);

                // Now clean up the event processor state by re-creating it and then invoke startUp.
                actor = createEventProcessor(props);

                startUp();

            } catch (Exception e) {
                log.warn("Failed while executing preRestart for event processor " + this, e);
                handleException(new EventProcessorReinitException(actor, e));
            }
        }

        private void handleException(Exception e) {
            Decider.Directive directive = props.getDecider().run(e);
            switch (directive) {
                case Restart:
                    this.restart(e, event == null ? null : event.getEvent());
                    break;

                case Resume:
                    // no action
                    break;

                case Stop:
                    this.stopAsync();
                    break;
            }
        }
    }

    private class CheckpointState {

        private final CheckpointStore checkpointStore;
        private final String process;
        private final String readerGroupName;
        private final String readerId;
        private final CheckpointConfig.CheckpointPeriod checkpointPeriod;

        private int count;
        private int previousCheckpointIndex;
        private long previousCheckpointTimestamp;

        CheckpointState(final CheckpointStore checkpointStore,
                        final String process,
                        final String readerGroupName,
                        final String readerId,
                        final CheckpointConfig.CheckpointPeriod checkpointPeriod) {
            this.checkpointStore = checkpointStore;
            this.process = process;
            this.readerGroupName = readerGroupName;
            this.readerId = readerId;
            this.checkpointPeriod = checkpointPeriod;

            count = 0;
            previousCheckpointIndex = 0;
            previousCheckpointTimestamp = System.currentTimeMillis();
        }

        void store(Position position) {
            count++;
            final long timestamp = System.currentTimeMillis();

            final int countInterval = count - previousCheckpointIndex;
            final long timeInterval = timestamp - previousCheckpointTimestamp;

            if (countInterval >= checkpointPeriod.getNumEvents() ||
                    timeInterval >= 1000 * checkpointPeriod.getNumSeconds()) {

                try {

                    checkpointStore.setPosition(process, readerGroupName, readerId, position);
                    // update the previous checkpoint stats if successful,
                    // otherwise, we again attempt checkpointing after processing next event
                    previousCheckpointIndex = count;
                    previousCheckpointTimestamp = timestamp;

                } catch (CheckpointStoreException cse) {
                    // Log the exception, without updating previous checkpoint index or timestamp.
                    // So that persisting checkpoint shall be attempted again after processing next message.
                    log.warn("Failed persisting checkpoint", cse.getCause());
                }
            }
        }

        void stop() {
            checkpointStore.removeReader(process, readerGroupName, readerId);
        }
    }

    EventProcessorCell(final Props<T> props,
                       final EventStreamReader<T> reader,
                       final String process,
                       final String readerId,
                       final CheckpointStore checkpointStore) {

        this.reader = reader;
        this.actor = createEventProcessor(props);
        this.delegate = new Delegate(props);
        this.state = new CheckpointState(checkpointStore, process, props.getConfig().getReaderGroupName(),
                readerId, props.getConfig().getCheckpointConfig().getCheckpointPeriod());
    }

    final void startAsync() {
        delegate.startAsync();
    }

    final void stopAsync() {
        delegate.stopAsync();
    }

    final void awaitStopped() {
        try {
            delegate.awaitTerminated();
        } catch (IllegalStateException e) {
            // This exception means that the delegate failed.
            log.warn("Event processor terminated with failure ", e);
        }
    }

    private EventProcessor<T> createEventProcessor(final Props<T> props) {
        try {
            return props.getConstructor().newInstance(props.getArgs());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Error instantiating event processor");
        }
    }

}
