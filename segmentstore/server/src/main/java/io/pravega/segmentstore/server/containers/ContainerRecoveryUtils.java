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
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.getMetadataSegmentName;

/**
 * Utility methods for container recovery.
 */
@Slf4j
public class ContainerRecoveryUtils {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /**
     * This method lists the segments from the given storage instance. It then registers all segments except Attribute
     * segments to the container metadata segment(s).
     * {@link DebugStreamSegmentContainer} instance(s) are provided to this method which can have some segments already present
     * in their respective container metadata segment(s). After the method successfully completes, only the segments which
     * existed in the {@link Storage} will remain in the container metadata. All segments which only existed in the container
     * metadata or which existed in both container metadata and the storage but with different lengths and/or sealed status,
     * will be deleted from the container metadata. If the method fails while execution, appropriate exception is thrown.
     * All segments from the storage are listed one by one, then mapped to their corresponding {@link DebugStreamSegmentContainer}
     * instances for registering them to container metadata segment.
     * @param storage                           A {@link Storage} instance that will be used to list segments from.
     * @param debugStreamSegmentContainers      A Map of Container Ids to {@link DebugStreamSegmentContainer} instances
     *                                          representing the containers that will be recovered.
     * @param executorService                   A thread pool for execution.
     * @throws InterruptedException             Required for Futures.get()
     * @throws ExecutionException               Required for Futures.get()
     * @throws TimeoutException                 Required for Futures.get()
     * @throws IOException                      Requited for Storage.listSegments()
     */
    public static void recoverAllSegments(Storage storage, Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainers,
                                          ExecutorService executorService) throws InterruptedException, ExecutionException,
            TimeoutException, IOException {
        Preconditions.checkNotNull(storage);
        Preconditions.checkNotNull(executorService);
        Preconditions.checkNotNull(debugStreamSegmentContainers);
        Preconditions.checkArgument(debugStreamSegmentContainers.size() > 0, "There should be at least one " +
                "debug segment container instance.");

        log.info("Recovery started for all containers...");
        // Get all segments in the container metadata for each debug segment container instance.
        Map<Integer, Set<String>> existingSegmentsMap = getExistingSegments(debugStreamSegmentContainers, executorService);

        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(debugStreamSegmentContainers.size());

        Iterator<SegmentProperties> segmentIterator = storage.listSegments();
        Preconditions.checkNotNull(segmentIterator);

        // Iterate through all segments. Create each one of their using their respective debugSegmentContainer instance.
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        while (segmentIterator.hasNext()) {
            val currentSegment = segmentIterator.next();

            // skip recovery if the segment is an attribute segment.
            if (NameUtils.isAttributeSegment(currentSegment.getName())) {
                continue;
            }

            int containerId = segToConMapper.getContainerId(currentSegment.getName());
            existingSegmentsMap.get(containerId).remove(currentSegment.getName());
            futures.add(recoverSegment(debugStreamSegmentContainers.get(containerId), currentSegment));
        }
        Futures.allOf(futures).join();

        for (val metadataSegmentsSetEntry : existingSegmentsMap.entrySet()) {
            for (String segmentName : metadataSegmentsSetEntry.getValue()) {
                log.info("Deleting segment '{}' as it is not in the storage.", segmentName);
                debugStreamSegmentContainers.get(metadataSegmentsSetEntry.getKey()).deleteStreamSegment(segmentName, TIMEOUT).join();
            }
        }
    }

    /**
     * The method lists all segments present in the container metadata segments of the given {@link DebugStreamSegmentContainer}
     * instances, stores their names by container Id in a map and returns it.
     * @param containerMap              A Map of Container Ids to {@link DebugStreamSegmentContainer} instances
     *                                  representing the containers to list the segments from.
     * @param executorService           A thread pool for execution.
     * @return                          A Map of Container Ids to segment names representing all segments present in the
     *                                  container metadata segment of a Container.
     * @throws InterruptedException     Required for Futures.get()
     * @throws ExecutionException       Required for Futures.get()
     * @throws TimeoutException         Required for Futures.get()
     */
    private static Map<Integer, Set<String>> getExistingSegments(Map<Integer, DebugStreamSegmentContainer> containerMap,
                                                                 ExecutorService executorService)
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<Integer, Set<String>> metadataSegmentsMap = new HashMap<>();
        val args = IteratorArgs.builder().fetchTimeout(TIMEOUT).build();

        // Get all segments for each container entry
        for (val containerEntry : containerMap.entrySet()) {
            Preconditions.checkNotNull(containerEntry.getValue());
            val tableExtension = containerEntry.getValue().getExtension(ContainerTableExtension.class);
            val keyIterator = tableExtension.keyIterator(getMetadataSegmentName(
                    containerEntry.getKey()), args).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Store the segments in a set
            Set<String> metadataSegments = new HashSet<>();
            keyIterator.forEachRemaining(k ->
                    metadataSegments.addAll(k.getEntries().stream()
                            .map(entry -> entry.getKey().toString())
                            .collect(Collectors.toSet())), executorService).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            metadataSegmentsMap.put(containerEntry.getKey(), metadataSegments);
        }
        return metadataSegmentsMap;
    }

    /**
     * This method takes a {@link DebugStreamSegmentContainer} instance and a {@link SegmentProperties} object as arguments
     * and takes one of the following actions:
     * 1. If the segment is present in the container metadata and its length or sealed status or both doesn't match with the
     * given {@link SegmentProperties}, then it is deleted from there and registered using the properties from the given
     * {@link SegmentProperties} instance.
     * 2. If the segment is absent in the container metadata, then it is registered using the properties from the given
     * {@link SegmentProperties}.
     * @param container         A {@link DebugStreamSegmentContainer} instance for registering the given segment and checking
     *                          its existence in the container metadata.
     * @param storageSegment    A {@link SegmentProperties} instance which has properties of the segment to be registered.
     * @return                  CompletableFuture which when completed will have the segment registered on to the container
     *                          metadata.
     */
    private static CompletableFuture<Void> recoverSegment(DebugStreamSegmentContainer container, SegmentProperties storageSegment) {
        Preconditions.checkNotNull(container);
        Preconditions.checkNotNull(storageSegment);
        long segmentLength = storageSegment.getLength();
        boolean isSealed = storageSegment.isSealed();
        String segmentName = storageSegment.getName();

        log.info("Registering: {}, {}, {}.", segmentName, segmentLength, isSealed);
        return Futures.exceptionallyComposeExpecting(
                container.getStreamSegmentInfo(storageSegment.getName(), TIMEOUT)
                        .thenAccept(e -> {
                            if (segmentLength != e.getLength() || isSealed != e.isSealed()) {
                                container.metadataStore.deleteSegment(segmentName, TIMEOUT)
                                        .thenAccept(x -> container.registerSegment(segmentName, segmentLength, isSealed));
                            }
                        }), ex -> Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException,
                () -> container.registerSegment(segmentName, segmentLength, isSealed));
    }

    /**
     * Deletes container metadata segment and its Attribute segment from the {@link Storage} for the given container Id.
     * @param storage       A {@link Storage} instance to delete the segments from.
     * @param containerId   Id of the container for which the segments has to be deleted.
     */
    public static CompletableFuture<Void> deleteMetadataAndAttributeSegments(Storage storage, int containerId) {
        Preconditions.checkNotNull(storage);
        String metadataSegmentName = NameUtils.getMetadataSegmentName(containerId);
        String attributeSegmentName = NameUtils.getAttributeSegmentName(metadataSegmentName);
        return deleteSegmentFromStorage(storage, metadataSegmentName)
                .thenAccept(x -> deleteSegmentFromStorage(storage, attributeSegmentName));
    }

    /**
     * Deletes the segment with given name from the given {@link Storage} instance. If the segment doesn't exist, it does
     * nothing and returns.
     * @param storage       A {@link Storage} instance to delete the segments from.
     * @param segmentName   Name of the segment to be deleted.
     * @return              CompletableFuture which when completed will have the segment deleted. In case segment didn't
     *                      exist, a completed future will be returned.
     */
    private static CompletableFuture<Void> deleteSegmentFromStorage(Storage storage, String segmentName) {
        log.info("Deleting Segment '{}'", segmentName);
        return Futures.exceptionallyComposeExpecting(
                storage.openWrite(segmentName).thenCompose(segmentHandle -> storage.delete(segmentHandle, TIMEOUT)),
                ex -> Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException,
                () -> CompletableFuture.completedFuture(null));
    }
}
