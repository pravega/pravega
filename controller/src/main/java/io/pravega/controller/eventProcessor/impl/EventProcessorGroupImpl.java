/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.eventProcessor.impl;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.ReaderSegmentDistribution;
import io.pravega.client.stream.Stream;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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

    private final ScheduledExecutorService rebalanceExecutor;
    
    private ScheduledFuture<?> rebalanceFuture;
    
    private final long rebalancePeriodMillis;
    /**
     * We use this lock for mutual exclusion between shutDown and changeEventProcessorCount methods.
     */
    private final Object lock = new Object();

    EventProcessorGroupImpl(final EventProcessorSystemImpl actorSystem,
                            final EventProcessorConfig<T> eventProcessorConfig,
                            final CheckpointStore checkpointStore, 
                            final ScheduledExecutorService rebalanceExecutor) {
        this.objectId = String.format("EventProcessorGroup[%s]", eventProcessorConfig.getConfig().getReaderGroupName());
        this.actorSystem = actorSystem;
        this.eventProcessorConfig = eventProcessorConfig;
        this.rebalanceExecutor = rebalanceExecutor;
        this.eventProcessorMap = new ConcurrentHashMap<>();
        this.writer = actorSystem
                .clientFactory
                .createEventWriter(eventProcessorConfig.getConfig().getStreamName(),
                        eventProcessorConfig.getSerializer(),
                        EventWriterConfig.builder().enableConnectionPooling(true).retryAttempts(Integer.MAX_VALUE).build());
        this.checkpointStore = checkpointStore;
        this.rebalancePeriodMillis = eventProcessorConfig.getRebalancePeriodMillis();
    }

    void initialize() throws CheckpointStoreException {

        try {
            checkpointStore.addReaderGroup(actorSystem.getProcess(), eventProcessorConfig.getConfig().getReaderGroupName());
        } catch (CheckpointStoreException e) {
            if (!e.getType().equals(CheckpointStoreException.Type.NodeExists)) {
                throw e;
            } else {
                log.debug("reader group {} exists", eventProcessorConfig.getConfig().getReaderGroupName());
            }
        }

        // Continue creating reader group if adding reader group to checkpoint store succeeds.
        readerGroup = createIfNotExists(
                actorSystem.readerGroupManager,
                eventProcessorConfig.getConfig().getReaderGroupName(),
                ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                 .stream(Stream.of(actorSystem.getScope(), eventProcessorConfig.getConfig().getStreamName())).build());

        createEventProcessors(eventProcessorConfig.getConfig().getEventProcessorCount() - eventProcessorMap.values().size());
    }

    private ReaderGroup createIfNotExists(final ReaderGroupManager readerGroupManager,
                                          final String groupName,
                                          final ReaderGroupConfig groupConfig) {
        readerGroupManager.createReaderGroup(groupName, groupConfig);
        return readerGroupManager.getReaderGroup(groupName);
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
                            ReaderConfig.builder().disableTimeWindows(true).build());

            // Create event processor, and add it to the actors list.
            EventProcessorCell<T> actorCell = new EventProcessorCell<>(eventProcessorConfig, reader, writer,
                    actorSystem.getProcess(), readerId, i, checkpointStore);
            log.info("Created event processor {}, id={}", i, actorCell.toString());

            // Add new event processors to the map
            eventProcessorMap.put(readerId, actorCell);
            readerIds.add(readerId);
            try {
                // Add reader to the checkpoint store again after creating the reader. 
                // During failover, the reader is removed from the readergroup. 
                // If the zk session expires after adding reader to checkpoint store but before it is added to readergroup
                // then we can be in a situation where the reader was removed from readergroup before it was 
                // added to the readergroup. This can lead to a situation where the reader is added but there is no corresponding
                // failover information. To mitigate this we will attempt to idempotently add the reader to the checkpoint store again
                // before calling readNextEvent on it (which leads to segment assignment). 
                checkpointStore.addReader(actorSystem.getProcess(), eventProcessorConfig.getConfig().getReaderGroupName(), readerId);
            } catch (CheckpointStoreException ex) {
                if (!ex.getType().equals(CheckpointStoreException.Type.NodeExists)) {
                    throw ex;
                }
            }
        }
        return readerIds;
    }

    @Override
    protected void startUp() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        try {
            log.info("Attempting to start all event processors in {}", this.toString());
            eventProcessorMap.entrySet().forEach(entry -> entry.getValue().startAsync());
            eventProcessorMap.entrySet().forEach(entry -> entry.getValue().awaitStartupComplete());
            log.info("All event processors in {} started successfully.", this.toString());

            if (rebalancePeriodMillis > 0 && rebalanceExecutor != null) {
                rebalanceFuture = rebalanceExecutor.scheduleWithFixedDelay(this::rebalance,
                        rebalancePeriodMillis, rebalancePeriodMillis, TimeUnit.MILLISECONDS);
            } else {
                rebalanceFuture = null;
            }
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    @Override
    protected void shutDown() {
        log.info("Shutting down all event processors in {}", this.toString());
        synchronized (lock) {
            long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "shutDown");
            try {
                // If any of the following operations error out, the fact is just logged.
                // And some other controller process would clean-up the reader group data
                // its readers and their position objects from checkpoint store.

                // Seal the Reader Group
                Map<String, Position> readerPositions = Map.of();
                try {
                    readerPositions = sealReaderGroup(actorSystem.getProcess());
                } catch (CheckpointStoreException e) {
                    log.warn("Error sealing Reader Group {}.", readerGroup.getGroupName(), e);
                }

                // Initiate stop on all event processor cells
                for (EventProcessorCell<T> cell : eventProcessorMap.values()) {
                    log.info("Terminating event processor cell: {}", cell);
                    cell.stopAsync(true);
                }
                // Await termination of cells
                for (EventProcessorCell<T> cell : eventProcessorMap.values()) {
                    try {
                        cell.awaitTerminated();
                        log.info("Termination of event processor cell: {} completed successfully.", cell);
                    } catch (Exception e) {
                        log.warn("Failed terminating event processor cell {}.", cell, e);
                    }
                }

                // Finally, clean up Reader Group data from Checkpoint store.
                try {
                    cleanupReaderGroup(actorSystem.getProcess(), readerPositions);
                } catch (Exception e) {
                    log.warn("Error removing data for Reader Group {} from checkpoint store. ", readerGroup.getGroupName(), e);
                }
                readerGroup.close();

                if (rebalanceFuture != null) {
                    rebalanceFuture.cancel(true);
                }
                this.writer.close();
            } finally {
                LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
            }
        }
        log.info("Shut down of all event processors in {} complete.", this.toString());
    }

    @Override
    public void notifyProcessFailure(String controllerId) throws CheckpointStoreException {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "notifyProcessFailure", controllerId);
        log.info("Notifying failure of process {} participating in reader group {}", controllerId, this.objectId);
        try {
            Map<String, Position> map = sealReaderGroup(controllerId);
            cleanupReaderGroup(controllerId, map);
        } finally {
            LoggerHelpers.traceLeave(log, "notifyProcessFailure", traceId, controllerId);
        }
    }

    private Map<String, Position> sealReaderGroup(String controllerId) throws CheckpointStoreException {
        Map<String, Position> readerPositions = ImmutableMap.of();
        try {
            log.debug("Attempting to seal Reader Group {} for Controller {}", readerGroup.getGroupName(), controllerId);
            // Change Reader Group State to SEALED so no new Readers can join
            readerPositions = checkpointStore.sealReaderGroup(controllerId, readerGroup.getGroupName());
        } catch (CheckpointStoreException e) {
            if (!e.getType().equals(CheckpointStoreException.Type.NoNode)) {
                throw e;
            }
        }
        log.info("Sealed Reader Group {} for Controller {}", readerGroup.getGroupName(), controllerId);
        return readerPositions;
    }

    private void cleanupReaderGroup(String controllerId, Map<String, Position> readerPositions) throws CheckpointStoreException {
            for (Map.Entry<String, Position> entry : readerPositions.entrySet()) {
                // 1. Remove failed readers from Reader Group in State Synchronizer
                log.info("{} Notifying readerOffline reader={}, position={} for Reader Group {}", this.objectId, entry.getKey(), entry.getValue(), readerGroup.getGroupName());
                readerGroup.readerOffline(entry.getKey(), entry.getValue());

                // 2. Remove reader from Checkpoint Store
                log.info("{} Removing reader={} from checkpoint store", this.objectId, entry.getKey());
                checkpointStore.removeReader(controllerId, readerGroup.getGroupName(), entry.getKey());
            }

            // Finally, remove reader group from the checkpoint store
            log.info("Removing reader group {} from process {}", readerGroup.getGroupName(), controllerId);
            checkpointStore.removeReaderGroup(controllerId, readerGroup.getGroupName());
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
    void changeEventProcessorCount(int count) throws CheckpointStoreException {
        synchronized (lock) {
            Preconditions.checkState(this.isRunning(), this.state().name());
            if (count <= 0) {
                throw new NotImplementedException("Decrease processor count");
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
    
    @VisibleForTesting
    void rebalance() {
        try {
            ReaderSegmentDistribution readerSegmentDistribution = readerGroup.getReaderSegmentDistribution();
            Map<String, Integer> distribution = readerSegmentDistribution.getReaderSegmentDistribution();
            int readerCount = distribution.size();
            int unassigned = readerSegmentDistribution.getUnassignedSegments();
            int segmentCount = distribution.values().stream().reduce(0, Integer::sum) + unassigned;
            
            // If there are idle readers (no segment assignments, then identify and replace overloaded readers). 
            boolean idleReaders = distribution.entrySet().stream().anyMatch(x -> !Strings.isNullOrEmpty(x.getKey()) && x.getValue() == 0);
            if (idleReaders) {
                distribution.forEach((readerId, assigned) -> {
                    if (!Strings.isNullOrEmpty(readerId)) {
                        // check if the reader belongs to this group and the reader is eligible for rebalance
                        if (eventProcessorMap.containsKey(readerId) && isRebalanceCandidate(assigned, readerCount, segmentCount)) {
                            replaceCell(readerId);
                        }
                    }
                });
            }
        } catch (Exception e) {
            Throwable realException = Exceptions.unwrap(e);
            log.warn("Re-balance failed with exception {} {}", realException.getClass().getSimpleName(), e.getMessage());
        }
    }

    private boolean isRebalanceCandidate(int assigned, int readerCount, int segmentCount) {
        double fair = (double) segmentCount / (double) readerCount;
        return assigned >= fair + 1.0;
    }

    private void replaceCell(String readerId) {
        synchronized (lock) {
            Preconditions.checkState(this.isRunning(), this.state().name());

            // add a replacement reader and then shutdown existing reader
            log.info("Found overloaded reader: {}", readerId);

            String newReaderId;
            try {
                List<String> newReaders = createEventProcessors(1);
                assert newReaders.size() == 1;
                newReaderId = newReaders.get(0);
                eventProcessorMap.get(newReaderId).startAsync();
            } catch (CheckpointStoreException e) {
                log.warn("Unable to create a new event processor cell", e.getMessage());
                return;
            }

            EventProcessorCell<T> cell = eventProcessorMap.get(readerId);
            log.info("Stopping event processor cell: {}", cell);
            try {
                cell.stopAsync(false);
                cell.awaitTerminated();
                checkpointStore.removeReader(cell.getProcess(), readerGroup.getGroupName(), readerId);
                eventProcessorMap.remove(readerId);
                log.info("Termination of event processor cell: {} completed successfully.", cell);
            } catch (Exception e) {
                log.warn("Failed terminating event processor cell {}.", cell, e);
            }
        }
    }
    
    @Override
    public void close() {
        this.stopAsync();
        this.writer.close();
    }

    @Override
    public String toString() {
        return this.objectId;
    }
}
