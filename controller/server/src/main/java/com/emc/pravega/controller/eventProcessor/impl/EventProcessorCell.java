/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.eventProcessor.CheckpointStore;
import com.emc.pravega.controller.eventProcessor.CheckpointStoreException;
import com.emc.pravega.controller.eventProcessor.ExceptionHandler;
import com.emc.pravega.controller.eventProcessor.EventProcessorInitException;
import com.emc.pravega.controller.eventProcessor.EventProcessorReinitException;
import com.emc.pravega.controller.eventProcessor.EventProcessorConfig;
import com.emc.pravega.controller.eventProcessor.ControllerEvent;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.Position;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Service;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;

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

    @VisibleForTesting
    @Getter(value = AccessLevel.PACKAGE)
    private EventProcessor<T> actor;

    /**
     * Event processor cell encapsulates a delegate which extends AbstractExecutionThreadService.
     * This delegate provides a single thread of execution for the event processor.
     * This prevents sub-classes of EventProcessor from controlling EventProcessor's lifecycle.
     */
    private final Service delegate;

    private class Delegate extends AbstractExecutionThreadService {

        private final long defaultTimeout = 4000L;
        private final EventProcessorConfig<T> eventProcessorConfig;
        private EventRead<T> event;
        private final CheckpointState state;

        Delegate(final String process, final String readerId, final EventProcessorConfig<T> eventProcessorConfig,
                 final CheckpointStore checkpointStore) {
            this.eventProcessorConfig = eventProcessorConfig;
            this.state = new CheckpointState(checkpointStore, process,
                    eventProcessorConfig.getConfig().getReaderGroupName(), readerId,
                    eventProcessorConfig.getConfig().getCheckpointConfig().getCheckpointPeriod());
        }

        @Override
        protected final void startUp() {
            log.debug("Event processor STARTUP " + this.serviceName());
            try {
                actor.beforeStart();
            } catch (Exception e) {
                log.warn("Failed while executing preStart for event processor " + this, e);
                handleException(new EventProcessorInitException(e));
            }
        }

        @Override
        protected final void run() throws Exception {
            log.debug("Event processor RUN " + this.serviceName());

            while (isRunning()) {
                try {
                    event = reader.readNextEvent(defaultTimeout);
                    if (event != null && event.getEvent() != null) {
                        // invoke the user specified event processing method
                        actor.process(event.getEvent());

                        // possibly persist event position
                        state.store(reader.getPosition());
                    }
                } catch (Exception e) {
                    log.warn("Failed in run method of event processor " + this, e);
                    handleException(e);
                }
            }

        }

        @Override
        protected final void shutDown() throws Exception {
            log.debug("Event processor SHUTDOWN " + this.serviceName());
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
            log.debug("Event processor RESTART " + this.serviceName());
            try {

                actor.beforeRestart(error, event);

                // Now clean up the event processor state by re-creating it and then invoke startUp.
                actor = createEventProcessor(eventProcessorConfig);

                startUp();

            } catch (Exception e) {
                log.warn("Failed while executing preRestart for event processor " + this, e);
                handleException(new EventProcessorReinitException(e));
            }
        }

        private void handleException(Exception e) {
            ExceptionHandler.Directive directive = eventProcessorConfig.getExceptionHandler().run(e);
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

    @NotThreadSafe
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

        void stop() throws CheckpointStoreException {
            checkpointStore.removeReader(process, readerGroupName, readerId);
        }
    }

    EventProcessorCell(final EventProcessorConfig<T> eventProcessorConfig,
                       final EventStreamReader<T> reader,
                       final String process,
                       final String readerId,
                       final CheckpointStore checkpointStore) {

        this.reader = reader;
        this.actor = createEventProcessor(eventProcessorConfig);
        this.delegate = new Delegate(process, readerId, eventProcessorConfig, checkpointStore);
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

    private EventProcessor<T> createEventProcessor(final EventProcessorConfig<T> eventProcessorConfig) {
        return eventProcessorConfig.getSupplier().get();
    }

}
