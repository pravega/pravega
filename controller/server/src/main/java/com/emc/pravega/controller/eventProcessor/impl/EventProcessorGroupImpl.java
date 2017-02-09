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

import com.emc.pravega.StreamManager;
import com.emc.pravega.controller.eventProcessor.CheckpointStore;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.Props;
import com.emc.pravega.controller.eventProcessor.StreamEvent;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.impl.segment.SegmentOutputConfiguration;
import com.google.common.util.concurrent.AbstractService;
import lombok.Synchronized;
import org.apache.commons.lang.NotImplementedException;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public final class EventProcessorGroupImpl<T extends StreamEvent> extends AbstractService implements EventProcessorGroup<T> {

    private final EventProcessorSystemImpl actorSystem;

    private final Props<T> props;

    private final List<EventProcessorCell<T>> actors;

    private final EventStreamWriter<T> ref;

    private final ReaderGroup readerGroup;

    private final CheckpointStore checkpointStore;

    EventProcessorGroupImpl(final EventProcessorSystemImpl actorSystem, final Props<T> props) {
        this.actorSystem = actorSystem;
        this.props = props;
        this.actors = new ArrayList<>();
        this.ref = actorSystem
                .clientFactory
                .createEventWriter(props.getConfig().getStreamName(),
                        props.getSerializer(),
                        new EventWriterConfig(new SegmentOutputConfiguration()));

        // todo: properly instantiate ReaderGroupConfig passed as null in the following statement.
        readerGroup = createIfNotExists(
                actorSystem.streamManager,
                props.getConfig().getReaderGroupName(),
                ReaderGroupConfig.builder().startingTime(0).build(),
                Collections.singletonList(props.getConfig().getStreamName()));

        try {
            createActors(props.getConfig().getActorCount());
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new RuntimeException("Error instantiating Actors");
        }

        this.checkpointStore = new InMemoryCheckpointStore();
    }

    private ReaderGroup createIfNotExists(final StreamManager streamManager,
                                          final String groupName,
                                          final ReaderGroupConfig groupConfig,
                                          final List<String> streamNanes) {
        // todo: getReaderGroup currently throws NotImplementedException
        ReaderGroup readerGroup = streamManager.getReaderGroup(groupName);
        if (readerGroup == null) {
            readerGroup = streamManager.createReaderGroup(groupName, groupConfig, streamNanes);
        }
        return  readerGroup;
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
            EventProcessorCell<T> actorCell =
                    new EventProcessorCell<>(actorSystem, this, props, reader, readerId, checkpointStore);
            actors.add(actorCell);
        }
    }

    @Override
    @Synchronized
    final protected void doStart() {
        // If an exception is thrown while starting an actor, it will be
        // processed by the ActorFailureListener. Current ActorFailureListener
        // just logs failures encountered while starting.
        actors.stream().forEach(EventProcessorCell::startAsync);
    }

    @Override
    @Synchronized
    final protected void doStop() {
        // If an exception is thrown while stopping an actor, it will be processed by the ActorFailureListener.
        // Current ActorFailureListener just logs failures encountered while stopping.
        actors.stream().forEach(EventProcessorCell::stopAsync);
    }

    @Synchronized
    final protected void awaitStopped() {
        actors.stream().forEach(EventProcessorCell::awaitStopped);
    }

    @Override
    public void notifyHostFailure(String host) {
        checkpointStore.getPositions(host, this.readerGroup.getGroupName())
                .entrySet()
                // todo handle errors/exceptions
                .forEach(entry -> readerGroup.readerOffline(entry.getKey(), entry.getValue()));
    }

    @Override
    @Synchronized
    public void changeEventProcessorCount(int count) {
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
