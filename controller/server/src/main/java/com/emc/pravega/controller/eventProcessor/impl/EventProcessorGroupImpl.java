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
import com.emc.pravega.controller.eventProcessor.CheckpointStoreException;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.Props;
import com.emc.pravega.controller.eventProcessor.StreamEvent;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.impl.segment.SegmentOutputConfiguration;
import com.google.common.util.concurrent.AbstractService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public final class EventProcessorGroupImpl<T extends StreamEvent> extends AbstractService implements EventProcessorGroup<T> {

    private final EventProcessorSystemImpl actorSystem;

    private final Props<T> props;

    private final ConcurrentHashMap<String, EventProcessorCell<T>> eventProcessorMap;

    private final EventStreamWriter<T> writer;

    private ReaderGroup readerGroup;

    private final CheckpointStore checkpointStore;

    EventProcessorGroupImpl(final EventProcessorSystemImpl actorSystem, final Props<T> props) {
        this.actorSystem = actorSystem;
        this.props = props;
        this.eventProcessorMap = new ConcurrentHashMap<>();
        this.writer = actorSystem
                .clientFactory
                .createEventWriter(props.getConfig().getStreamName(),
                        props.getSerializer(),
                        new EventWriterConfig(new SegmentOutputConfiguration()));

        this.checkpointStore = CheckpointStoreFactory.create(props.getConfig().getCheckpointConfig().getStoreType(),
                props.getConfig().getCheckpointConfig().getCheckpointStoreClient());
    }

    void initialize() throws CheckpointStoreException {

        checkpointStore.addReaderGroup(actorSystem.getProcess(), props.getConfig().getReaderGroupName());

        // Continue creating reader group if adding reader group to checkpoint store succeeds.

        readerGroup = createIfNotExists(
                actorSystem.streamManager,
                props.getConfig().getReaderGroupName(),
                ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build(),
                Collections.singletonList(props.getConfig().getStreamName()));

        createEventProcessors(props.getConfig().getEventProcessorCount());
    }

    private ReaderGroup createIfNotExists(final StreamManager streamManager,
                                          final String groupName,
                                          final ReaderGroupConfig groupConfig,
                                          final List<String> streamNanes) {
        return streamManager.createReaderGroup(groupName, groupConfig, streamNanes);
        // todo: getReaderGroup currently throws NotImplementedException
        //ReaderGroup readerGroup = streamManager.getReaderGroup(groupName);
        //if (readerGroup == null) {
        //    readerGroup = streamManager.createReaderGroup(groupName, groupConfig, streamNanes);
        //}
        //return  readerGroup;
    }

    private List<String> createEventProcessors(final int count) throws CheckpointStoreException {

        List<String> readerIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            // Create a reader id.
            String readerId = UUID.randomUUID().toString();

            // Store the readerId in checkpoint store.
            checkpointStore.addReader(actorSystem.getProcess(), props.getConfig().getReaderGroupName(), readerId);

            // Once readerId is successfully persisted, create readers and event processors
            // Create reader.
            EventStreamReader<T> reader =
                    actorSystem.clientFactory.createReader(readerId,
                            props.getConfig().getReaderGroupName(),
                            props.getSerializer(),
                            new ReaderConfig());

            // Create event processor, and add it to the actors list.
            EventProcessorCell<T> actorCell =
                    new EventProcessorCell<>(props, reader, actorSystem.getProcess(), readerId, checkpointStore);

            // Add new event processors to the map
            eventProcessorMap.put(readerId, actorCell);
            readerIds.add(readerId);
        }
        return readerIds;
    }

    @Override
    final protected void doStart() {
        eventProcessorMap.entrySet().forEach(entry -> entry.getValue().startAsync());
        notifyStarted();
    }

    @Override
    final protected void doStop() {
        try {
            checkpointStore.sealReaderGroup(actorSystem.getProcess(), readerGroup.getGroupName())
                    .entrySet()
                    .forEach(entry -> {
                        if (eventProcessorMap.containsKey(entry.getKey())) {
                            eventProcessorMap.get(entry.getKey()).stopAsync();
                        } else {
                            // 1. Notify reader group about stopped reader.
                            readerGroup.readerOffline(entry.getKey(), entry.getValue());

                            // 2. Clean up reader from checkpoint store.
                            try {
                                checkpointStore.removeReader(actorSystem.getProcess(), readerGroup.getGroupName(), entry.getKey());
                            } catch (CheckpointStoreException e) {
                                log.warn("Error removing reader " + entry.getKey() + " from reader group "
                                        + readerGroup.getGroupName(), e);
                            }
                        }
                    });
        } catch (CheckpointStoreException e) {
            log.warn("Error sealing reader group " + this.readerGroup, e);
        }
    }

    final protected void awaitStopped() throws CheckpointStoreException {
        // If exception is thrown in any of the following operations, it is just logged.
        // Some other controller process is responsible for cleaning up reader group, its
        // readers and their position objects from checkpoint store.

        // First, wait for all event processors to stop.
        eventProcessorMap.entrySet()
                .forEach(entry -> entry.getValue().awaitStopped());

        // Next, clean up reader group from checkpoint store.
        checkpointStore.removeReaderGroup(actorSystem.getProcess(), readerGroup.getGroupName());
    }

    @Override
    public void notifyProcessFailure(String process) throws CheckpointStoreException {
        Map<String, Position> map = checkpointStore.sealReaderGroup(process, readerGroup.getGroupName());

        for (Map.Entry<String, Position> entry : map.entrySet()) {

            // 1. Notify reader group about failed readers
            readerGroup.readerOffline(entry.getKey(), entry.getValue());

            // 2. Clean up reader from checkpoint store
            checkpointStore.removeReader(actorSystem.getProcess(), readerGroup.getGroupName(), entry.getKey());

        }
        // finally, remove reader group from checkpoint store
        checkpointStore.removeReaderGroup(process, readerGroup.getGroupName());
    }

    @Override
    public void changeEventProcessorCount(int count) throws CheckpointStoreException {
        if (this.isRunning()) {
            if (count <= 0) {
                throw new NotImplementedException();
            } else {

                // create new event processors
                List<String> readerIds = createEventProcessors(count);

                // start the new event processors
                readerIds.stream().forEach(readerId -> eventProcessorMap.get(readerId).startAsync());
            }
        } else {
            throw new IllegalStateException(this.state().name());
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

    public void stopAll() throws CheckpointStoreException {
        this.stopAsync();
        this.awaitStopped();
    }
}
