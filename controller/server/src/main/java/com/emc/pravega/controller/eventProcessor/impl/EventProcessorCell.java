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
import com.emc.pravega.controller.eventProcessor.Decider;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.EventProcessorInitException;
import com.emc.pravega.controller.eventProcessor.EventProcessorReinitException;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.Props;
import com.emc.pravega.controller.eventProcessor.StreamEvent;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.Position;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Service;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;

@Slf4j
class EventProcessorCell<T extends StreamEvent> {

    private final EventProcessorSystem actorSystem;

    private final EventProcessorGroup<T> actorGroup;

    private final EventStreamReader<T> reader;

    private final String readerId;

    private final Props<T> props;

    private final CheckpointStore checkpointStore;

    private EventProcessor<T> actor;

    /**
     * Actor encapsulates a delegate which extends AbstractExecutionThreadService. This delegate provides a single
     * thread of execution for the actor. This prevents sub-classes of Actor from controlling Actor's lifecycle.
     */
    private Service delegate;
    private CheckpointState state;
    private EventRead<T> event;

    private class Delegate extends AbstractExecutionThreadService {

        @Override
        protected final void startUp() {
            try {
                actor.beforeStart();
            } catch (Throwable t) {
                log.warn("Failed while executing preStart for Actor " + this, t);
                handleException(new EventProcessorInitException(actor, t));
            }
        }

        @Override
        protected final void run() throws Exception {
            final long defaultTimeout = Long.MAX_VALUE;

            while (isRunning()) {
                try {
                    event = reader.readNextEvent(defaultTimeout);

                    // invoke the user specified event processing method
                    actor.receive(event.getEvent());

                    // possibly persist event position
                    state.store(event.getPosition());

                } catch (Throwable t) {
                    handleException(t);
                }
            }

        }

        @Override
        protected final void shutDown() throws Exception {
            try {
                actor.afterStop();
            } catch (Throwable t) {
                // Error encountered while cleanup is just logged.
                // AbstractExecutionThreadService shall transition the service to failed state.
                log.warn("Failed while executing postStop for Actor " + this, t);
                throw t;
            }
        }

        private void restart(Throwable error, T event) {
            try {

                actor.beforeRestart(error, event);

                // Now clean up the actor state by re-creating it and then invoke startUp.
                actor = createAndSetupActor(props, actorGroup);
                startUp();

            } catch (Exception e) {
                log.warn("Failed while executing preRestart for Actor " + this, e);
                handleException(new EventProcessorReinitException(actor, e));
            }
        }

        private void handleException(Throwable t) {
            Decider.Directive directive = props.getDecider().run(t);
            switch (directive) {
                case Restart:
                    this.restart(t, event == null ? null : event.getEvent());
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
        private int count;
        private int previousCheckpointIndex;
        private long previousCheckpointTimestamp;

        CheckpointState() {
            count = 0;
            previousCheckpointIndex = 0;
            previousCheckpointTimestamp = System.currentTimeMillis();
        }

        void store(Position position) {
            count++;
            final long timestamp = System.currentTimeMillis();

            final int countInterval = count - previousCheckpointIndex;
            final long timeInterval = timestamp - previousCheckpointTimestamp;
            final CheckpointConfig.CheckpointPeriod config =
                    props.getConfig().getCheckpointConfig().getCheckpointPeriod();

            if (countInterval >= config.getNumEvents() || timeInterval >= 1000 * config.getNumSeconds()) {
                try {
                    checkpointStore.setPosition(actorSystem.getHost(),
                            props.getConfig().getReaderGroupName(), readerId, position);
                    previousCheckpointIndex = count;
                    previousCheckpointTimestamp = timestamp;
                } catch (RuntimeException e) {
                    // log the exception. ignore it
                    // do not increment previous count or timestamp, after next event, checkpoint shall be retried
                }
            }
        }
    }

    EventProcessorCell(final EventProcessorSystem actorSystem,
                       final EventProcessorGroup<T> actorGroup,
                       final Props<T> props,
                       final EventStreamReader<T> reader,
                       final String readerId,
                       final CheckpointStore checkpointStore) {

        this.actorSystem = actorSystem;
        this.actorGroup = actorGroup;
        this.reader = reader;
        this.readerId = readerId;
        this.props = props;
        this.checkpointStore = checkpointStore;
        this.actor = createAndSetupActor(props, actorGroup);
        this.delegate = new Delegate();
        this.state = new CheckpointState();
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
            log.warn("Actor terminated with failure ", e);
        }
    }

    private EventProcessor<T> createAndSetupActor(final Props<T> props,
                                                  final EventProcessorGroup<T> actorGroup) {
        EventProcessor<T> temp;
        try {
            temp = props.getConstructor().newInstance(props.getArgs());
            temp.setup(actorGroup.getSelf());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Error instantiating Actor");
        }
        return temp;
    }

}
