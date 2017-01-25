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
package com.emc.pravega.controller.actor.impl;

import com.emc.pravega.controller.actor.ActorGroupRef;
import com.emc.pravega.controller.actor.Props;
import com.emc.pravega.controller.actor.StreamEvent;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Service;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

@Slf4j
public abstract class Actor<T extends StreamEvent> {

    @Getter(AccessLevel.PACKAGE)
    private EventStreamReader<T> reader;

    @Getter(AccessLevel.PACKAGE)
    private Props props;

    @Getter(AccessLevel.PACKAGE)
    private String readerId;

    @Getter(AccessLevel.PACKAGE)
    private ActorSystemImpl actorSystem;

    @Getter(AccessLevel.PACKAGE)
    private Executor executor;

    @GuardedBy("actorGroups")
    private List<ActorGroupImpl<T>> actorGroups;

    private ActorContext<T> context;

    private int count = 0;

    /**
     * Actor encapsulates a delegate which extends AbstractExecutionThreadService. This delegate provides a single
     * thread of execution for the actor. This prevents sub-classes of Actor from controlling Actor's lifecycle.
     */
    private Service delegate = new Delegate();

    private class Delegate extends AbstractExecutionThreadService {
        @Override
        protected final void startUp() throws Exception {
            preStart();
        }

        @Override
        protected final void run() throws Exception {
            final long defaultTimeout = Long.MAX_VALUE;

            while (isRunning()) {
                EventRead<T> event = reader.readNextEvent(defaultTimeout);
                receive(event.getEvent());

                // persist reader position if persistenceFrequency number of events are processed
                count++;
                if (props.getPersister() != null && count % props.getConfig().getCheckpointFrequency() == 0) {
                    props.getPersister()
                            .setPosition(props.getConfig().getReaderGroupName(), readerId, event.getPosition())
                            .join();
                }
            }
        }

        @Override
        protected final void shutDown() throws Exception {
            actorGroups.forEach(ActorGroupImpl::doStop);
            postStop();
        }

        @Override
        protected final void triggerShutdown() {
            actorGroups.forEach(ActorGroupImpl::doStop);
            this.stopAsync();
        }
    }

    final void startAsync() {
        delegate.startAsync();
    }

    final void stopAsync() {
        delegate.stopAsync();
        this.actorGroups.stream().forEach(ActorGroupImpl::stopAsync);
    }

    final void restartAsync() {
        Service.State delegateState = delegate.state();
        if (delegateState == Service.State.FAILED) {

            try {
                preRestart();
            } catch (Throwable restartFailureCause) {
                log.error("Failed executing preRestart for Actor " + this, restartFailureCause);
                return;
            }

            // Recreate the delegate and start it.
            delegate = new Delegate();
            delegate.startAsync();
        }
    }

    final void addListener(Service.Listener listener, Executor executor) {
        delegate.addListener(listener, executor);
    }

    final void setup(ActorSystemImpl actorSystem, Executor executor, Props props) {
        this.actorSystem = actorSystem;
        this.executor = executor;
        this.props = props;
        this.actorGroups = new ArrayList<>();
        this.context = new ActorContext<>(actorSystem, executor, actorGroups);
    }

    final void setReader(EventStreamReader<T> reader, String readerId) {
        this.reader = reader;
        this.readerId = readerId;
    }

    /**
     * AbstractActor initialization hook that is called before actor starts receiving events.
     * @throws Exception Exception thrown from user defined preStart method.
     */
    protected void preStart() throws Exception { }

    /**
     * User defined event processing logic.
     * @param event Event received from Pravega Stream.
     * @throws Exception Exception thrown from user defined preStart method.
     */
    protected abstract void receive(T event) throws Exception;

    /**
     * AbstractActor shutdown hook that is called on shut down.
     * @throws Exception Exception thrown from user defined preStart method.
     */
    protected void postStop() throws Exception { }

    /**
     * AbstractActor preRestart hook that is called before actor restarts
     * after recovering from a failure. After this method call, preStart is
     * called before the Actor starts again.
     * @throws Exception Exception thrown from user defined preStart method.
     */
    protected void preRestart() throws Exception { }

    /**
     * Get the current context for creating new ActorGroup as child of this Actor.
     * @return ActorContext.
     */
    protected final ActorContext getContext() {
        return this.context;
    }

    @AllArgsConstructor
    public static class ActorContext<T extends StreamEvent> {

        private final ActorSystemImpl actorSystem;
        private final Executor executor;
        private final List<ActorGroupImpl<T>> actorGroups;

        public ActorGroupRef actorOf(Props<T> props) {
            synchronized (actorGroups) {
                ActorGroupImpl<T> actorGroup;

                // Create the actor group, add it to the list of actor groups and start it.
                actorGroup = new ActorGroupImpl<>(actorSystem, executor, props);

                actorGroups.add(actorGroup);

                actorGroup.startAsync();

                return actorGroup.getRef();
            }
        }
    }
}
