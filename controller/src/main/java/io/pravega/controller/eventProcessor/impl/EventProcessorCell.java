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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.LoggerHelpers;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.eventProcessor.ExceptionHandler;
import io.pravega.controller.eventProcessor.EventProcessorInitException;
import io.pravega.controller.eventProcessor.EventProcessorReinitException;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Position;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Service;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.atomic.AtomicReference;

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
class EventProcessorCell<T extends ControllerEvent> {

    private final EventStreamReader<T> reader;
    private final EventStreamWriter<T> selfWriter;
    private final CheckpointStore checkpointStore;
    @Getter(AccessLevel.PACKAGE)
    private final String process;
    private final String readerGroupName;
    @Getter(AccessLevel.PACKAGE)
    private final String readerId;
    private final String objectId;
    private final AtomicReference<Position> lastCheckpoint;

    @VisibleForTesting
    @Getter(value = AccessLevel.PACKAGE)
    private EventProcessor<T> actor;

    /**
     * Event processor cell encapsulates a delegate which extends AbstractExecutionThreadService.
     * This delegate provides a single thread of execution for the event processor.
     * This prevents sub-classes of EventProcessor from controlling EventProcessor's lifecycle.
     */
    private final Delegate delegate;

    private class Delegate extends AbstractExecutionThreadService {

        private final long defaultTimeout = 2000L;
        private final EventProcessorConfig<T> eventProcessorConfig;
        private EventRead<T> event;
        private final CheckpointState state;
        private final AtomicReference<Thread> currentThread = new AtomicReference<>();

        Delegate(final EventProcessorConfig<T> eventProcessorConfig) {
            this.eventProcessorConfig = eventProcessorConfig;
            this.state = new CheckpointState(eventProcessorConfig.getConfig().getCheckpointConfig());
        }

        @Override
        protected final void startUp() {
            log.info("Event processor STARTUP {}, state={}", objectId, state());
            try {
                actor.beforeStart();
            } catch (Exception e) {
                log.warn(String.format("Failed while executing preStart for event processor %s", objectId), e);
                handleException(new EventProcessorInitException(e));
            }
        }

        @Override
        protected final void run() throws Exception {
            log.debug("Event processor RUN {}, state={}", objectId, state());
            this.currentThread.set(Thread.currentThread());
            while (isRunning()) {
                try {
                    event = reader.readNextEvent(defaultTimeout);
                    if (event != null && event.getEvent() != null) {
                        // invoke the user specified event processing method
                        actor.process(event.getEvent(), event.getPosition());

                        // possibly persist event position
                        state.store(event.getPosition());
                    }
                } catch (Exception e) {
                    log.debug("Exception while reading the events {}", e);
                    if (this.currentThread.get() != null) {
                        handleException(e);
                    }
                }
            }
            log.info("Event processor RUN {}, state={}, isRunning={}", objectId, state(), isRunning());
        }

        @Override
        protected final void shutDown() throws Exception {
            log.info("Event processor SHUTDOWN {}, state={}, ISRUNNING={}", objectId, state(), isRunning());
            try {
                actor.afterStop();
            } catch (Exception e) {
                // Error encountered while cleanup is just logged.
                // AbstractExecutionThreadService shall transition the service to failed state.
                log.warn(String.format("Failed while executing afterStop for event processor %s", objectId), e);
                throw e;
            } finally {

                // If exception is thrown in any of the following operations, it is just logged.
                // Some other controller process is responsible for cleaning up reader and its position object

                // First close the reader, which implicitly notifies reader position to the reader group
                log.info("Closing reader for {}", objectId);
                try {
                    reader.closeAt(getCheckpoint());
                } catch (Exception e) {
                    log.warn("Exception while closing EventProcessorCell reader from checkpointStore: {}.", e.getMessage());
                }
            }
        }

        private void restart(Throwable error, T event) {
            log.debug("Event processor RESTART {}, state={}", objectId, state());
            try {

                actor.beforeRestart(error, event);

                // Now clean up the event processor state by re-creating it and then invoke startUp.
                actor = createEventProcessor(eventProcessorConfig);

                startUp();

            } catch (Exception e) {
                log.warn(String.format("Failed while executing preRestart for event processor %s", objectId), e);
                handleException(new EventProcessorReinitException(e));
            }
        }

        private void handleException(Exception e) {
            ExceptionHandler.Directive directive = eventProcessorConfig.getExceptionHandler().run(e);
            log.info("Exception handler directive is {}", directive);
            switch (directive) {
                case Restart:
                    log.warn("Restarting event processor: {} due to exception: {}", objectId, e);
                    this.restart(e, event == null ? null : event.getEvent());
                    break;

                case Resume:
                    // no action
                    log.debug("Resuming event processor: {} after receiving exception: {}", objectId, e);
                    break;

                case Stop:
                    log.warn("Stopping event processor: {} due to exception: {}", objectId, e);
                    this.stopAsync();
                    break;
            }
        }
    }

    @NotThreadSafe
    private class CheckpointState {

        private final boolean enableCheckpoint;
        private final CheckpointConfig.CheckpointPeriod checkpointPeriod;
        private int count;
        private int previousCheckpointIndex;
        private long previousCheckpointTimestamp;

        CheckpointState(final CheckpointConfig checkpointConfig) {
            if (checkpointConfig.getType() == CheckpointConfig.Type.Periodic) {
                this.enableCheckpoint = true;
                this.checkpointPeriod = checkpointConfig.getCheckpointPeriod();
            } else {
                this.enableCheckpoint = false;
                this.checkpointPeriod = null;
            }
            count = 0;
            previousCheckpointIndex = 0;
            previousCheckpointTimestamp = System.currentTimeMillis();
        }

        void store(Position position) {
            if (!enableCheckpoint) {
                return;
            }
            count++;
            final long timestamp = System.currentTimeMillis();
            final int countInterval = count - previousCheckpointIndex;
            final long timeInterval = timestamp - previousCheckpointTimestamp;

            if (countInterval >= checkpointPeriod.getNumEvents() ||
                    timeInterval >= 1000 * checkpointPeriod.getNumSeconds()) {

                try {
                    actor.getCheckpointer().store(position);
                    // update the previous checkpoint stats if successful,
                    // otherwise, we again attempt checkpointing after processing next event
                    previousCheckpointIndex = count;
                    previousCheckpointTimestamp = timestamp;
                } catch (CheckpointStoreException cse) {
                    // Log the exception, without updating previous checkpoint index or timestamp.
                    // So that persisting checkpoint shall be attempted again after processing next message.
                    log.warn(String.format("Failed persisting checkpoint for event processor %s", objectId),
                            cse.getCause());
                }
            }
        }
    }

    EventProcessorCell(final EventProcessorConfig<T> eventProcessorConfig,
                       final EventStreamReader<T> reader,
                       final EventStreamWriter<T> selfWriter,
                       final String process,
                       final String readerId,
                       final int index,
                       final CheckpointStore checkpointStore) {
        Preconditions.checkNotNull(eventProcessorConfig);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(process));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(readerId));
        this.reader = Preconditions.checkNotNull(reader);
        this.selfWriter = Preconditions.checkNotNull(selfWriter);
        this.checkpointStore = Preconditions.checkNotNull(checkpointStore);
        this.process = process;
        this.readerGroupName = eventProcessorConfig.getConfig().getReaderGroupName();
        this.readerId = readerId;
        this.objectId = String.format("EventProcessor[%s:%d:%s]", this.readerGroupName, index, readerId);
        this.actor = createEventProcessor(eventProcessorConfig);
        this.delegate = new Delegate(eventProcessorConfig);
        this.lastCheckpoint = new AtomicReference<>();
    }

    final void startAsync() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startAsync");
        try {
            delegate.startAsync();
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startAsync", traceId);
        }
    }

    final void stopAsync(boolean interruptDelegate) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "stopAsync");
        try {
            Thread thread = delegate.currentThread.get();
            delegate.stopAsync();
            if (thread != null && interruptDelegate) {
                delegate.currentThread.set(null);
                log.debug("Event Processor {} is interrupted", objectId);
                thread.interrupt();
            }
            log.info("Event processor cell {} SHUTDOWN issued", this.objectId);
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "stopAsync", traceId);
        }
    }

    final void awaitStartupComplete() {
        try {
            // Wait for delegate to reach running state.
            delegate.awaitRunning();
        } catch (IllegalStateException e) {
            // If service state is NEW or RUNNING or STARTING, control wouldn't reach this point.
            Service.State state = delegate.state();
            // If service state is Stopping or Terminated, then startup has completed.
            if (state != Service.State.STOPPING && state != Service.State.TERMINATED) {
                // If service state is FAILED, throw error.
                throw e;
            }
        }
    }

    final void awaitTerminated() {
        delegate.awaitTerminated();
        log.info("Event processor cell {} Terminated", this.objectId);
    }

    private EventProcessor<T> createEventProcessor(final EventProcessorConfig<T> eventProcessorConfig) {
        EventProcessor<T> eventProcessor = eventProcessorConfig.getSupplier().get();
        eventProcessor.checkpointer = (Position position) -> {
            checkpointStore.setPosition(process, readerGroupName, readerId, position);
            lastCheckpoint.set(position);
        };
        eventProcessor.selfWriter = selfWriter::writeEvent;
        return eventProcessor;
    }

    Position getCheckpoint() {
        return lastCheckpoint.get();    
    }
    
    @Override
    public String toString() {
        return String.format("%s[%s]", objectId, this.delegate.state());
    }
}
