/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import static com.emc.pravega.controller.eventProcessor.RetryHelper.CONNECTIVITY_PREDICATE;
import static com.emc.pravega.controller.eventProcessor.RetryHelper.withRetries;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.controller.requests.ControllerEvent;
import com.emc.pravega.controller.store.checkpoint.CheckpointStore;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.EventProcessorConfig;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.Sequence;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public final class EventProcessorGroupImpl<T extends ControllerEvent> extends AbstractIdleService
        implements EventProcessorGroup<T> {

    private final String objectId;

    private final EventProcessorSystemImpl actorSystem;

    private final EventProcessorConfig<T> eventProcessorConfig;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final ConcurrentHashMap<String, EventProcessorCell<T>> eventProcessorMap;

    private final EventStreamWriter<T> writer;

    private ReaderGroup readerGroup;

    private final CheckpointStore checkpointStore;

    /**
     * We use this lock for mutual exclusion between shutDown and changeEventProcessorCount methods.
     */
    private final Object lock = new Object();

    EventProcessorGroupImpl(final EventProcessorSystemImpl actorSystem,
                            final EventProcessorConfig<T> eventProcessorConfig,
                            final CheckpointStore checkpointStore) {
        this.objectId = String.format("EventProcessorGroup[%s]", eventProcessorConfig.getConfig().getReaderGroupName());
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
            EventProcessorCell<T> actorCell = new EventProcessorCell<>(eventProcessorConfig, reader, writer,
                    actorSystem.getProcess(), readerId, i, checkpointStore);
            log.info("Created event processor {}, id={}", i, actorCell.toString());

            // Add new event processors to the map
            eventProcessorMap.put(readerId, actorCell);
            readerIds.add(readerId);
        }
        return readerIds;
    }

    @Override
    protected void startUp() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        try {
            log.info("Attempting to start all event processors in {}", this.toString());
            eventProcessorMap.entrySet().forEach(entry -> entry.getValue().startAsync());
            log.info("Waiting for all all event processors in {} to start", this.toString());
            eventProcessorMap.entrySet().forEach(entry -> entry.getValue().awaitRunning());
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    @Override
    protected void shutDown() {
        synchronized (lock) {
            long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "shutDown");
            try {
                // If any of the following operations error out, the fact is just logged.
                // Some other controller process is responsible for cleaning up the reader group,
                // its readers and their position objects from checkpoint store.
                try {
                    log.info("Attempting to seal the reader group entry from checkpoint store");
                    checkpointStore.sealReaderGroup(actorSystem.getProcess(), readerGroup.getGroupName());
                } catch (CheckpointStoreException e) {
                    log.warn("Error sealing reader group " + this.objectId, e);
                }

                // Initiate stop on all event processor cells and await their termination.
                for (EventProcessorCell<T> cell : eventProcessorMap.values()) {
                    log.info("Stopping {}", cell);
                    cell.stopAsync();
                    log.info("Awaiting termination of {}", cell);
                    try {
                        cell.awaitTerminated();
                    } catch (IllegalStateException e) {
                        log.warn(String.format("Failed terminating %s", cell), e.getMessage());
                    }
                }

                // Finally, clean up reader group from checkpoint store.
                try {
                    log.info("Attempting to clean up reader group entry from checkpoint store");
                    checkpointStore.removeReaderGroup(actorSystem.getProcess(), readerGroup.getGroupName());
                } catch (CheckpointStoreException e) {
                    log.warn("Error removing reader group " + this.objectId, e);
                }

                log.info("Shutdown of {} complete", this.toString());
            } finally {
                LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
            }
        }
    }

    @Override
    public void notifyProcessFailure(String process) throws CheckpointStoreException {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "notifyProcessFailure", process);
        log.info("Notifying failure of process {} participating in reader group {}", process, this.objectId);
        try {
            Map<String, Position> map = withRetries(() -> {
                try {
                    return checkpointStore.sealReaderGroup(process, readerGroup.getGroupName());
                } catch (CheckpointStoreException e) {
                    if (e.getType().equals(CheckpointStoreException.Type.NoNode)) {
                        return Collections.emptyMap();
                    }
                    throw new CompletionException(e);
                }
            }, CONNECTIVITY_PREDICATE, 10);

            for (Map.Entry<String, Position> entry : map.entrySet()) {

                // 1. Notify reader group about failed readers
                withRetries(() -> {
                    if (readerGroup.getOnlineReaders().contains(entry.getKey())) {
                        log.info("{} Notifying readerOffline reader={}, position={}", this.objectId, entry.getKey(), entry.getValue());
                        readerGroup.readerOffline(entry.getKey(), entry.getValue());
                    }
                    return null;
                }, throwable -> true, 10);

                // 2. Clean up reader from checkpoint store
                log.info("{} removing reader={} from checkpoint store", this.objectId, entry.getKey());
                withRetries(() -> {
                    try {
                        checkpointStore.removeReader(actorSystem.getProcess(), readerGroup.getGroupName(), entry.getKey());
                        return null;
                    } catch (CheckpointStoreException e) {
                        if (e.getType().equals(CheckpointStoreException.Type.NoNode)) {
                            return null;
                        }
                        throw new CompletionException(e);
                    }
                }, CONNECTIVITY_PREDICATE, 10);
            }
            // finally, remove reader group from checkpoint store
            log.info("Removing reader group {} from process {}", readerGroup.getGroupName(), process);
            withRetries(() -> {
                try {
                    checkpointStore.removeReaderGroup(process, readerGroup.getGroupName());
                    return null;
                } catch (CheckpointStoreException e) {
                    if (e.getType().equals(CheckpointStoreException.Type.NoNode)) {
                        return null;
                    }
                    throw new CompletionException(e);
                }
            }, CONNECTIVITY_PREDICATE, 10);

        } finally {
            LoggerHelpers.traceLeave(log, "notifyProcessFailure", traceId, process);
        }
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
        synchronized (lock) {
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
    }

    @Override
    public EventStreamWriter<T> getWriter() {
        return this.writer;
    }

    @Override
    public Set<String> getProcesses() throws CheckpointStoreException {
        return checkpointStore.getProcesses();
    }

    @Override
    public void close() throws Exception {
        this.stopAsync();
    }

    @Override
    public String toString() {
        return this.objectId;
    }
}
