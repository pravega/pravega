/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.getMetadataSegmentName;

/**
 * Utility methods for data recovery.
 */
@Slf4j
public class SegmentsRecovery {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /**
     * Lists all segments from a given long term storage and then re-creates them using their corresponding debug segment
     * container.
     * @param storage                           Long term storage.
     * @param debugStreamSegmentContainers      A hashmap which has debug segment container instances to create segments.
     * @param executorService                   A thread pool for execution.
     * @throws                                  Exception in case of exception during the execution.
     */
    public static void recoverAllSegments(Storage storage, Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainers,
                                          ExecutorService executorService) throws Exception {
        log.info("Recovery started for all containers...");
        Map<DebugStreamSegmentContainer, Set<String>> metadataSegmentsByContainer = new HashMap<>();

        // Add all segments present in the container metadata in a set for each debug segment container instance.
        for (Map.Entry<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainer : debugStreamSegmentContainers.entrySet()) {
            ContainerTableExtension tableExtension = debugStreamSegmentContainer.getValue().getExtension(ContainerTableExtension.class);
            AsyncIterator<IteratorItem<TableKey>> keyIterator = tableExtension.keyIterator(getMetadataSegmentName(
                    debugStreamSegmentContainer.getKey()), IteratorArgs.builder().fetchTimeout(TIMEOUT).build()).get(TIMEOUT.toMillis(),
                    TimeUnit.MILLISECONDS);
            Set<String> metadataSegments = new HashSet<>();
            keyIterator.forEachRemaining(k -> metadataSegments.addAll(k.getEntries().stream().map(entry -> entry.getKey().toString())
                    .collect(Collectors.toSet())), executorService).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            metadataSegmentsByContainer.put(debugStreamSegmentContainer.getValue(), metadataSegments);
        }

        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(debugStreamSegmentContainers.size());

        Iterator<SegmentProperties> it = storage.listSegments();
        if (it == null) {
            log.info("No segments found in the long term storage.");
            return;
        }

        // Iterate through all segments. Create each one of their using their respective debugSegmentContainer instance.
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        while (it.hasNext()) {
            SegmentProperties curr = it.next();
            int containerId = segToConMapper.getContainerId(curr.getName());
            log.info("Segment to be recovered = {}", curr.getName());
            metadataSegmentsByContainer.get(debugStreamSegmentContainers.get(containerId)).remove(curr.getName());
            futures.add(CompletableFuture.runAsync(new SegmentRecovery(debugStreamSegmentContainers.get(containerId), curr)));
        }
        Futures.allOf(futures).join();

        for (Map.Entry<DebugStreamSegmentContainer, Set<String>> metadataSegmentsSetEntry : metadataSegmentsByContainer.entrySet()) {
            for (String segmentName : metadataSegmentsSetEntry.getValue()) {
                log.info("Deleting segment '{}' as it is not in storage", segmentName);
                metadataSegmentsSetEntry.getKey().deleteStreamSegment(segmentName, TIMEOUT).join();
            }
        }
    }

    /**
     * Creates the given segment with the given DebugStreamSegmentContainer instance.
     */
    public static class SegmentRecovery implements Runnable {
        private final DebugStreamSegmentContainer container;
        private final SegmentProperties storageSegment;

        public SegmentRecovery(DebugStreamSegmentContainer container, SegmentProperties segment) {
            Preconditions.checkNotNull(container);
            Preconditions.checkNotNull(segment);
            this.container = container;
            this.storageSegment = segment;
        }

        @Override
        public void run() {
            long segmentLength = storageSegment.getLength();
            boolean isSealed = storageSegment.isSealed();
            String segmentName = storageSegment.getName();

            log.info("Recovering segment with name = {}, length = {}, sealed status = {}.", segmentName, segmentLength, isSealed);
            /*
                1. segment exists in both metadata and storage, re-create it
                2. segment only in metadata, delete
                3. segment only in storage, re-create it
             */
            val streamSegmentInfo = container.getStreamSegmentInfo(storageSegment.getName(), TIMEOUT)
                    .thenAccept(e -> {
                        if (segmentLength != e.getLength() || isSealed != e.isSealed()) {
                            container.metadataStore.deleteSegment(segmentName, TIMEOUT).join();
                            container.registerSegment(segmentName, segmentLength, isSealed).join();
                        }
                    });

            Futures.exceptionallyComposeExpecting(streamSegmentInfo, ex -> Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException,
                    () -> container.registerSegment(segmentName, segmentLength, isSealed)).join();
        }
    }

    /**
     * Deletes container-metadata segment and attribute segment of the container with given container Id.
     * @param storage       Long term storage to delete the segments from.
     * @param containerId   Id of the container for which the segments has to be deleted.
     */
    public static void deleteContainerMetadataSegments(Storage storage, int containerId) {
        String metadataSegmentName = NameUtils.getMetadataSegmentName(containerId);
        deleteSegment(storage, metadataSegmentName);
        String attributeSegmentName = NameUtils.getAttributeSegmentName(metadataSegmentName);
        deleteSegment(storage, attributeSegmentName);
    }

    /**
     * Deletes the segment with given segment name from the given long term storage.
     * @param storage       Long term storage to delete the segment from.
     * @param segmentName   Name of the segment to be deleted.
     */
    private static void deleteSegment(Storage storage, String segmentName) {
        try {
            SegmentHandle segmentHandle = storage.openWrite(segmentName).join();
            storage.delete(segmentHandle, TIMEOUT).join();
        } catch (Exception e) {
            if (Exceptions.unwrap(e) instanceof StreamSegmentNotExistsException) {
                log.info("Segment '{}' doesn't exist.", segmentName);
            } else {
                throw e;
            }
        }
    }
}
