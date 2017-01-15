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
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

// TODO: fault tolerance

public abstract class Actor extends AbstractExecutionThreadService {

    @Getter(AccessLevel.PACKAGE)
    private EventStreamReader<byte[]> reader;

    @Getter(AccessLevel.PACKAGE)
    private Props props;

    @Getter(AccessLevel.PACKAGE)
    private String readerId;

    @Getter(AccessLevel.PACKAGE)
    private ActorSystemImpl actorSystem;

    @Getter(AccessLevel.PACKAGE)
    private Executor executor;

    private List<ActorGroupImpl> actorGroups;

    private ActorContext context;

    private int count = 0;

    protected final void setup(ActorSystemImpl actorSystem, Executor executor, Props props) {
        this.actorSystem = actorSystem;
        this.executor = executor;
        this.props = props;
        this.actorGroups = new ArrayList<>();
        this.context = new ActorContext(actorSystem, executor, actorGroups);
    }

    protected final void setReader(EventStreamReader<byte[]> reader, String readerId) {
        this.reader = reader;
        this.readerId = readerId;
    }

    @Override
    protected final void startUp() throws Exception {
        preStart();
    }

    @Override
    protected final void run() throws Exception {
        final long defaultTimeout = Long.MAX_VALUE;

        while (isRunning()) {
            EventRead<byte[]> event = reader.readNextEvent(defaultTimeout);
            receive(event.getEvent());

            // persist reader position if persistenceFrequency number of events are processed
            count++;
            if (props.getPersister() != null && count % props.getConfig().getPersistenceFrequency() == 0) {
                props.getPersister()
                        .setPosition(props.getConfig().getReaderGroupName(), readerId, event.getPosition())
                        .join();
            }
        }
    }

    @Override
    protected final void shutDown() throws Exception {
        this.actorGroups.forEach(ActorGroupImpl::doStop);
        postStop();
    }

    @Override
    protected final void triggerShutdown() {
        this.actorGroups.forEach(ActorGroupImpl::doStop);
        this.stopAsync();
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
    protected abstract void receive(byte[] event) throws Exception;

    /**
     * AbstractActor shutdown hook that is called on shut down.
     * @throws Exception Exception thrown from user defined preStart method.
     */
    protected void postStop() throws Exception { }

    /**
     * Get the current context for creating new ActorGroup as child of this Actor.
     * @return ActorContext.
     */
    protected final ActorContext getContext() {
        return this.context;
    }

    @AllArgsConstructor
    public static class ActorContext {

        private final ActorSystemImpl actorSystem;
        private final Executor executor;
        private final List<ActorGroupImpl> actorGroups;

        public ActorGroupRef actorOf(Props props) {
            ActorGroupImpl actorGroup;

            // Create the actor group and start it.
            actorGroup = new ActorGroupImpl(actorSystem, executor, props);
            actorGroups.add(actorGroup);
            actorGroup.startAsync();

            return actorGroup.getRef();
        }
    }
}
