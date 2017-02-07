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
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Service;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;

@Slf4j
public class EventProcessorCell<T extends StreamEvent> {

    private final EventProcessorSystem actorSystem;

    private final EventProcessorGroup actorGroup;

    private final EventStreamReader<T> reader;

    private final String readerId;

    @Getter(AccessLevel.PACKAGE)
    private final Props<T> props;

    private EventProcessor<T> actor;

    /**
     * Actor encapsulates a delegate which extends AbstractExecutionThreadService. This delegate provides a single
     * thread of execution for the actor. This prevents sub-classes of Actor from controlling Actor's lifecycle.
     */
    private Service delegate;
    private int count = 0;
    private EventRead<T> event;

    private class Delegate extends AbstractExecutionThreadService {

        @Override
        protected final void startUp() {
            try {
                actor.preStart();
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
                    actor.receive(event.getEvent());

                    // persist reader position if persistenceFrequency number of events are processed
                    count++;
                    //if (props.getPersister() != null && count % props.getConfig().getCheckpointFrequency() == 0) {
                    //    props.getPersister()
                    //            .setPosition(props.getConfig().getReaderGroupName(), readerId, event.getPosition())
                    //            .join();
                    //}
                } catch (Throwable t) {
                    handleException(t);
                }
            }

        }

        @Override
        protected final void shutDown() throws Exception {
            try {
                actor.postStop();
            } catch (Throwable t) {
                // Error encountered while cleanup is just logged.
                // AbstractExecutionThreadService shall transition the service to failed state.
                log.warn("Failed while executing postStop for Actor " + this, t);
                throw t;
            }
        }

        private void restart(Throwable error, T event) {
            try {

                actor.preRestart(error, event);

                // Now clean up the actor state by re-creating it and then invoke startUp.
                actor = createAndSetupActor(props, actorSystem, actorGroup);
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

    EventProcessorCell(final EventProcessorSystem actorSystem,
                       final EventProcessorGroup actorGroup,
                       final Props<T> props,
                       final EventStreamReader<T> reader,
                       final String readerId) {

        this.actorSystem = actorSystem;
        this.actorGroup = actorGroup;
        this.reader = reader;
        this.readerId = readerId;
        this.props = props;
        this.actor = createAndSetupActor(props, actorSystem, actorGroup);
        this.delegate = new Delegate();
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
                                                  final EventProcessorSystem actorSystem,
                                                  final EventProcessorGroup actorGroup) {
        EventProcessor<T> temp;
        try {
            temp = props.getConstructor().newInstance(props.getArgs());
            temp.setup(actorSystem, actorGroup.getSelf());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Error instantiating Actor");
        }
        return temp;
    }

}
