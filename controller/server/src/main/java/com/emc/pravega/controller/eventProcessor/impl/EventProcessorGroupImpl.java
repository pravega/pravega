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
import com.emc.pravega.stream.Sequence;
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

    private final EventStreamWriter<T> writer;

    private final ReaderGroup readerGroup;

    private final CheckpointStore checkpointStore;

    EventProcessorGroupImpl(final EventProcessorSystemImpl actorSystem, final Props<T> props) {
        this.actorSystem = actorSystem;
        this.props = props;
        this.actors = new ArrayList<>();
        this.writer = actorSystem
                .clientFactory
                .createEventWriter(props.getConfig().getStreamName(),
                        props.getSerializer(),
                        new EventWriterConfig(new SegmentOutputConfiguration()));

        this.checkpointStore = new InMemoryCheckpointStore();

        this.checkpointStore.addReaderGroup(this.actorSystem.getProcess(), this.props.getConfig().getReaderGroupName());

        // todo: check whether ReaderGroupConfig is initialized properly to 0 in the following statement.
        readerGroup = createIfNotExists(
                actorSystem.streamManager,
                props.getConfig().getReaderGroupName(),
                ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build(),
                Collections.singletonList(props.getConfig().getStreamName()));

        try {
            createActors(props.getConfig().getActorCount());
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new RuntimeException("Error instantiating Actors");
        }
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
            // create a reader id
            String readerId = UUID.randomUUID().toString();

            // store the readerId in checkpoint store
            checkpointStore.addReader(actorSystem.getProcess(), props.getConfig().getReaderGroupName(), readerId);

            // create the reader
            EventStreamReader<T> reader =
                    actorSystem.clientFactory.createReader(readerId,
                            props.getConfig().getReaderGroupName(),
                            props.getSerializer(),
                            new ReaderConfig());

            // create a new actor, and add it to the actors list
            EventProcessorCell<T> actorCell =
                    new EventProcessorCell<>(actorSystem, props, reader, readerId, checkpointStore);
            actors.add(actorCell);
        }
    }

    @Override
    @Synchronized
    final protected void doStart() {
        actors.stream().forEach(EventProcessorCell::startAsync);
    }

    @Override
    @Synchronized
    final protected void doStop() {
        actors.stream().forEach(EventProcessorCell::stopAsync);
    }

    @Synchronized
    final protected void awaitStopped() {
        // If exception is thrown in any of the following operations, it is just logged.
        // Some other controller process is responsible for cleaning up reader group,
        // its readers and their position objects from checkpoint store.

        // First, wait for all event processors to stop.
        actors.stream().forEach(EventProcessorCell::awaitStopped);

        // Next, clean up reader group from checkpoint store.
        checkpointStore.removeReaderGroup(actorSystem.getProcess(), props.getConfig().getReaderGroupName());
    }

    @Override
    @Synchronized
    public void notifyProcessFailure(String process) {
        checkpointStore.getPositions(process, readerGroup.getGroupName())
                .entrySet()
                // todo handle errors/exceptions
                .forEach(entry -> {
                    // first notify reader group about failed readers
                    readerGroup.readerOffline(entry.getKey(), entry.getValue());

                    // 2. clean up reader from checkpoint store
                    checkpointStore.removeReader(actorSystem.getProcess(), props.getConfig().getReaderGroupName(),
                            entry.getKey());
                });

        // finally, remove reader group from checkpoint store
        checkpointStore.removeReaderGroup(this.actorSystem.getProcess(),
                this.props.getConfig().getReaderGroupName());
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
    public EventStreamWriter<T> getWriter() {
        return this.writer;
    }

    @Override
    public Set<String> getProcesses() {
        return readerGroup.getOnlineReaders();
    }

    public void stopAll() {
        this.doStop();
        this.awaitStopped();
    }
}
