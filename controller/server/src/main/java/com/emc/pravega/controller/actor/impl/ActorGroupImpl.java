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

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.controller.actor.ActorGroup;
import com.emc.pravega.controller.actor.ActorGroupRef;
import com.emc.pravega.controller.actor.Props;
import com.emc.pravega.controller.actor.StreamEvent;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.google.common.util.concurrent.AbstractService;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang.NotImplementedException;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

public final class ActorGroupImpl<T extends StreamEvent> extends AbstractService implements ActorGroup {

    private final ActorSystemImpl actorSystem;

    @Getter(AccessLevel.PACKAGE)
    private final Props<T> props;

    @Getter(AccessLevel.PACKAGE)
    private final List<Actor<T>> actors;

    @Getter(AccessLevel.PACKAGE)
    private final ActorGroupRef ref;

    private final ReaderGroup readerGroup;

    private final Executor executor;

    ActorGroupImpl(final ActorSystemImpl actorSystem, final Executor executor, final Props<T> props) {
        this.actorSystem = actorSystem;
        this.executor = executor;
        this.props = props;
        this.actors = new ArrayList<>();
        this.ref = new ActorGroupRefImpl(actorSystem, props.getConfig().getStreamName());

        // todo: what if reader group already exists, we just want to be part of that group.
        // todo: properly instantiate ReaderGroupConfig passed as null in the following statement.
        readerGroup =
                actorSystem.streamManager
                        .createReaderGroup(props.getConfig().getReaderGroupName(),
                                null,
                                Collections.singletonList(actorSystem.getScope()));

        try {
            createActors(0, props.getConfig().getActorCount());
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new RuntimeException("Error instantiating Actors");
        }
    }

    private void createActors(final int startIndex, final int count) throws IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        for (int i = startIndex; i < startIndex+count; i++) {
            String readerId = UUID.randomUUID().toString();
            EventStreamReader<T> reader =
                    actorSystem.clientFactory.createReader(readerId,
                            props.getConfig().getReaderGroupName(),
                            props.getSerializer(),
                            new ReaderConfig());

            // create a new actor, and add it to the list
            Actor<T> actor = props.getConstructor().newInstance(props.getArgs());
            actor.setReader(reader, readerId);
            actor.setup(this.actorSystem, this.executor, props);
            actor.addListener(new ActorFailureListener<>(actors.get(i)), executor);
            actors.add(actor);
            // todo: persist the readerIds against this host in the persister
        }
    }

    @Override
    final protected void doStart() {
        // If an exception is thrown while starting an actor, it will be
        // processed by the ActorFailureListener. Current ActorFailureListener
        // just logs failures encountered while starting.
        actors.stream().forEach(Actor::startAsync);
    }

    @Override
    final protected void doStop() {
        // If an exception is thrown while stopping an actor, it will be processed by the ActorFailureListener.
        // Current ActorFailureListener just logs failures encountered while stopping.
        actors.stream().forEach(Actor::stopAsync);
    }

    @Override
    public void notifyHostFailure(Host host) {
        // todo: validate the logic to identify if this is an interesting host
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

    @Override
    public void changeActorCount(int count) {
        if (count <= 0) {
            throw new NotImplementedException();
        } else {
            try {
                createActors(this.actors.size(), count);
            }  catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
                throw new RuntimeException("Error instantiating Actors");
            }
        }
    }
}
