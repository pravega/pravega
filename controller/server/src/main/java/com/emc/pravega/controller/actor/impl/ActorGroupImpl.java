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
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.impl.ByteArraySerializer;
import com.google.common.util.concurrent.AbstractService;
import lombok.AccessLevel;
import lombok.Getter;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

public final class ActorGroupImpl extends AbstractService {

    private final ActorSystemImpl actorSystem;

    @Getter(AccessLevel.PACKAGE)
    private final Props props;

    @Getter(AccessLevel.PACKAGE)
    private final List<Actor> actors;

    @Getter(AccessLevel.PACKAGE)
    private final ActorGroupRef ref;

    private final ReaderGroup readerGroup;

    private final Executor executor;

    ActorGroupImpl(final ActorSystemImpl actorSystem, final Executor executor, final Props props)
            throws IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        this.actorSystem = actorSystem;
        this.executor = executor;
        this.props = props;
        this.actors = new ArrayList<>();
        this.ref = new ActorGroupRefImpl(actorSystem, props.getConfig().getStreamName());

        // Todo: what if reader group already exists, we just want to be part of that group.
        readerGroup =
                actorSystem.streamManager
                        .createReaderGroup(props.getConfig().getReaderGroupName(),
                                new ReaderGroupConfig(),
                                Collections.singletonList(actorSystem.getScope()));

        for (int i = 0; i < props.getConfig().getActorCount(); i++) {
            String readerId = UUID.randomUUID().toString();
            EventStreamReader<byte[]> reader =
                    actorSystem.clientFactory.createReader(readerId,
                            props.getConfig().getReaderGroupName(),
                            new ByteArraySerializer(),
                            new ReaderConfig());

            // create a new actor, and add it to the list
            Actor actor = (Actor) props.getConstructor().newInstance(props.getArgs());
            actor.setReader(reader);
            actor.setProps(props);
            actor.setReaderId(readerId);
            actor.addListener(new ActorFailureListener(actors, i, executor), executor);
            actors.add(actor);
        }

        // todo: persist the readerIds against this host in the persister

        // start the group of actors
        this.doStart();
    }

    @Override
    final protected void doStart() {
        // If an exception is thrown while starting an actor, it will be processed by the ActorFailureListener.
        // Current ActorFailureListener just logs failures encountered while starting.
        actors.stream().forEach(actor -> actor.startAsync());
        notifyStarted();
    }

    @Override
    final protected void doStop() {
        // If an exception is thrown while stopping an actor, it will be processed by the ActorFailureListener.
        // Current ActorFailureListener just logs failures encountered while stopping.
        actors.stream().forEach(actor -> actor.stopAsync());
        notifyStopped();
    }

    public void notifyHostFailure(String host) {
        if (readerGroup.getOnlineReaders().contains(host)) {
            this.props.getPersister()
                    .getPositions(props.getConfig().getReaderGroupName(), host)
                    .thenAccept(readerPositions ->
                            readerPositions
                                    .entrySet()
                                    .stream()
                                    .forEach(entry -> readerGroup.readerOffline(entry.getKey(), entry.getValue())));
        }
    }

    public void increaseReaderCount(int count) throws IllegalAccessException, InvocationTargetException, InstantiationException {
        for (int i = 0; i < count; i++) {
            String readerId = UUID.randomUUID().toString();
            EventStreamReader<byte[]> reader =
                    actorSystem.clientFactory.createReader(readerId,
                            props.getConfig().getReaderGroupName(),
                            new ByteArraySerializer(),
                            new ReaderConfig());

            // create a new actor, and add it to the list
            Actor actor = (Actor) props.getConstructor().newInstance(props.getArgs());
            actor.setReader(reader);
            actor.setProps(props);
            actor.setReaderId(readerId);
            actor.addListener(new ActorFailureListener(actors, i, executor), executor);
            actors.add(actor);
        }
    }
}
