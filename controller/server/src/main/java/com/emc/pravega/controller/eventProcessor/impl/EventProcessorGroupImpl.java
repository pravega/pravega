/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.controller.store.checkpoint.CheckpointStore;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.EventProcessorConfig;
import com.emc.pravega.controller.eventProcessor.ControllerEvent;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.Sequence;
import com.google.common.base.Preconditions;
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
public final class EventProcessorGroupImpl<T extends ControllerEvent> extends AbstractService implements EventProcessorGroup<T> {

    private final EventProcessorSystemImpl actorSystem;

    private final EventProcessorConfig<T> eventProcessorConfig;

    private final ConcurrentHashMap<String, EventProcessorCell<T>> eventProcessorMap;

    private final EventStreamWriter<T> writer;

    private ReaderGroup readerGroup;

    private final CheckpointStore checkpointStore;

    EventProcessorGroupImpl(final EventProcessorSystemImpl actorSystem,
                            final EventProcessorConfig<T> eventProcessorConfig,
                            final CheckpointStore checkpointStore) {
        this.actorSystem = actorSystem;
        this.eventProcessorConfig = eventProcessorConfig;
        this.eventProcessorMap = new ConcurrentHashMap<>();
        this.writer = actorSystem
                .clientFactory
                .createEventWriter(eventProcessorConfig.getConfig().getStreamName(),
                        eventProcessorConfig.getSerializer(),
                        EventWriterConfig.builder().build());
        this.checkpointStore = checkpointStore;
    }

    void initialize() throws CheckpointStoreException {

        checkpointStore.addReaderGroup(actorSystem.getProcess(), eventProcessorConfig.getConfig().getReaderGroupName());

        // Continue creating reader group if adding reader group to checkpoint store succeeds.

        readerGroup = createIfNotExists(
                actorSystem.readerGroupManager,
                eventProcessorConfig.getConfig().getReaderGroupName(),
                ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build(),
                Collections.singleton(eventProcessorConfig.getConfig().getStreamName()));

        createEventProcessors(eventProcessorConfig.getConfig().getEventProcessorCount());
    }

    private ReaderGroup createIfNotExists(final ReaderGroupManager readerGroupManager,
                                          final String groupName,
                                          final ReaderGroupConfig groupConfig,
                                          final Set<String> streamNanes) {
        return readerGroupManager.createReaderGroup(groupName, groupConfig, streamNanes);
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
            checkpointStore.addReader(actorSystem.getProcess(), eventProcessorConfig.getConfig().getReaderGroupName(), readerId);

            // Once readerId is successfully persisted, create readers and event processors
            // Create reader.
            EventStreamReader<T> reader =
                    actorSystem.clientFactory.createReader(readerId,
                            eventProcessorConfig.getConfig().getReaderGroupName(),
                            eventProcessorConfig.getSerializer(),
                            ReaderConfig.builder().build());

            // Create event processor, and add it to the actors list.
            EventProcessorCell<T> actorCell =
                    new EventProcessorCell<>(eventProcessorConfig, reader, actorSystem.getProcess(), readerId, checkpointStore);

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

    /**
     * Increase/decrease the number of event processors reading from the Pravega
     * Stream and participating in the ReaderGroup. This method may be
     * invoked if the number of active segments in the Pravega Stream
     * increases/decreased on account of a Scale event due to increased/
     * decreased event throughput.
     * @param count Number of event processors to add. Negative number indicates
     *              decreasing the Actor count.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    public void changeEventProcessorCount(int count) throws CheckpointStoreException {
        Preconditions.checkState(this.isRunning(), this.state().name());
        if (count <= 0) {
            throw new NotImplementedException();
        } else {

            // create new event processors
            List<String> readerIds = createEventProcessors(count);

            // start the new event processors
            readerIds.stream().forEach(readerId -> eventProcessorMap.get(readerId).startAsync());
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
