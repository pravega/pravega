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

import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.impl.segment.SegmentOutputConfiguration;
import com.google.common.util.concurrent.AbstractService;
import org.apache.commons.lang.NotImplementedException;

import javax.annotation.concurrent.GuardedBy;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public final class EventProcessorGroupImpl<T extends StreamEvent> extends AbstractService implements EventProcessorGroup<T> {

    private final EventProcessorSystemImpl actorSystem;

    private final Props<T> props;

    @GuardedBy("actors")
    private final List<EventProcessorCell<T>> actors;

    private final EventStreamWriter<T> ref;

    private final ReaderGroup readerGroup;

    EventProcessorGroupImpl(final EventProcessorSystemImpl actorSystem, final Props<T> props) {
        this.actorSystem = actorSystem;
        this.props = props;
        this.actors = new ArrayList<>();
        this.ref = actorSystem
                .clientFactory
                .createEventWriter(props.getConfig().getStreamName(),
                        props.getSerializer(),
                        new EventWriterConfig(new SegmentOutputConfiguration()));

        // todo: what if reader group already exists, we just want to be part of that group.
        // todo: properly instantiate ReaderGroupConfig passed as null in the following statement.
        readerGroup =
                actorSystem.streamManager
                        .createReaderGroup(props.getConfig().getReaderGroupName(),
                                null,
                                Collections.singletonList(actorSystem.getScope()));

        try {
            createActors(props.getConfig().getActorCount());
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new RuntimeException("Error instantiating Actors");
        }
    }

    private void createActors(final int count) throws IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        for (int i = 0; i < count; i++) {
            String readerId = UUID.randomUUID().toString();
            EventStreamReader<T> reader =
                    actorSystem.clientFactory.createReader(readerId,
                            props.getConfig().getReaderGroupName(),
                            props.getSerializer(),
                            new ReaderConfig());

            // create a new actor, and add it to the list
            EventProcessorCell<T> actorCell = new EventProcessorCell<>(actorSystem, this, props, reader, readerId);
            actors.add(actorCell);
            // todo: persist the readerIds against this host in the persister
        }
    }

    @Override
    final protected void doStart() {
        // If an exception is thrown while starting an actor, it will be
        // processed by the ActorFailureListener. Current ActorFailureListener
        // just logs failures encountered while starting.
        synchronized (actors) {
            actors.stream().forEach(EventProcessorCell::startAsync);
        }
    }

    @Override
    final protected void doStop() {
        // If an exception is thrown while stopping an actor, it will be processed by the ActorFailureListener.
        // Current ActorFailureListener just logs failures encountered while stopping.
        synchronized (actors) {
            actors.stream().forEach(EventProcessorCell::stopAsync);
        }
    }

    final protected void awaitStopped() {
        synchronized (actors) {
            actors.stream().forEach(EventProcessorCell::awaitStopped);
        }
    }

    @Override
    public void notifyHostFailure(String host) {
        throw new NotImplementedException();
    }

    @Override
    public void changeEventProcessorCount(int count) {
        synchronized (actors) {
            if (count <= 0) {
                throw new NotImplementedException();
            } else {
                try {
                    createActors(count);
                } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
                    throw new RuntimeException("Error instantiating Actors");
                }
            }
        }
    }

    @Override
    public EventStreamWriter<T> getSelf() {
        return this.ref;
    }

    public Set<String> getHosts() {
        return readerGroup.getOnlineReaders();
    }

    public void stopAll() {
        this.doStop();
        this.awaitStopped();
    }
}
