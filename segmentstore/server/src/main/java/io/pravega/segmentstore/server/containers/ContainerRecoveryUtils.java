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
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.getMetadataSegmentName;

/**
 * Utility methods for container recovery.
 */
@Slf4j
public class ContainerRecoveryUtils {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int BUFFER_SIZE = 8 * 1024 * 1024;

    /**
     * Recovers Segments from the given Storage instance. This is done by:
     * 1. Listing all Segments from the given Storage instance and partitioning them by their assigned Container Id using
     * the standard {@link SegmentToContainerMapper}.
     * 2. Filtering out all shadow Segments (such as Attribute Segments).
     * 3. Registering all remaining (external) Segments to the owning Container's {@link MetadataStore}.
     *
     * The {@link DebugStreamSegmentContainer} instance(s) that are provided to this method may have some segments already
     * present in their respective {@link MetadataStore}.
     * After the method successfully completes, the following are true:
     * - Only the segments which exist in the {@link Storage} will remain in the Container's {@link MetadataStore}.
     * - If a Segment exists both in the Container's {@link MetadataStore} and in {@link Storage}, then the information
     * that exists in {@link Storage} (length, sealed) will prevail.
     *
     * If the method fails during execution, the appropriate exception is thrown and the Containers' {@link MetadataStore}
     * may be left in an inconsistent state.
     * @param storage                           A {@link Storage} instance that will be used to list segments from.
     * @param debugStreamSegmentContainersMap   A Map of Container Ids to {@link DebugStreamSegmentContainer} instances
     *                                          representing the containers that will be recovered.
     * @param executorService                   A thread pool for execution.
     * @throws Exception                        If an exception occurred. This could be one of the following:
     *                                              * TimeoutException:     If the calls for computation(used in the method)
     *                                                                      didn't complete in time.
     *                                              * IOException     :     If a general IO exception occurred.
     */
    public static void recoverAllSegments(Storage storage, Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainersMap,
                                          ExecutorService executorService) throws Exception {
        Preconditions.checkNotNull(storage);
        Preconditions.checkNotNull(executorService);
        Preconditions.checkNotNull(debugStreamSegmentContainersMap);
        Preconditions.checkArgument(debugStreamSegmentContainersMap.size() > 0, "There should be at least one " +
                "debug segment container instance.");
        int containerCount = debugStreamSegmentContainersMap.size();
        validateContainerIds(debugStreamSegmentContainersMap, containerCount);

        log.info("Recovery started for all containers...");
        // Get all segments in the metadata store for each debug segment container instance.
        Map<Integer, Set<String>> existingSegmentsMap = getExistingSegments(debugStreamSegmentContainersMap, executorService);

        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(containerCount);

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
            futures.add(recoverSegment(debugStreamSegmentContainersMap.get(containerId), currentSegment));
        }
        Futures.allOf(futures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        futures.clear();
        // Delete segments which only exist in the container metadata, not in storage.
        for (val existingSegmentsSetEntry : existingSegmentsMap.entrySet()) {
            for (String segmentName : existingSegmentsSetEntry.getValue()) {
                log.info("Deleting segment '{}' as it is not in the storage.", segmentName);
                futures.add(debugStreamSegmentContainersMap.get(existingSegmentsSetEntry.getKey())
                        .deleteStreamSegment(segmentName, TIMEOUT));
            }
        }
        Futures.allOf(futures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Verifies if the given map of {@link DebugStreamSegmentContainer} instance(s) contains:
     *      1. Only Valid container Ids. A valid container Id is defined here as:
     *              * Id value should be non-negative and less than container count.
     *      2. A key in the map corresponding to each container Id(all non-negative integers less than the given containerCount)
     * @param debugStreamSegmentContainersMap   A Map of Container Ids to {@link DebugStreamSegmentContainer} instances
     *                                          to be validated.
     * @param containerCount                    Expected number of {@link DebugStreamSegmentContainer} instances.
     * @throws IllegalArgumentException         If the given map of {@link DebugStreamSegmentContainer} instance(s) doesn't
     *                                          satisfy the above two criteria.
     */
    private static void validateContainerIds(Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainersMap,
                                          int containerCount) throws IllegalArgumentException {
        Set<Integer> containerIdsSet = new HashSet<>();
        for (val containerId : debugStreamSegmentContainersMap.keySet()) {
            if (containerId < 0 || containerId >= containerCount) {
                throw new IllegalArgumentException("Container Id is not valid. It should be non-negative and less than container count.");
            }
            containerIdsSet.add(containerId);
        }
        if (containerIdsSet.size() != containerCount) {
            throw new IllegalArgumentException("All container Ids should be present.");
        }
    }

    /**
     * The method lists all segments present in the {@link MetadataStore} of the given {@link DebugStreamSegmentContainer}
     * instances, stores their names by container Id in a map and returns it.
     * @param containerMap              A Map of Container Ids to {@link DebugStreamSegmentContainer} instances
     *                                  representing the containers to list the segments from.
     * @param executorService           A thread pool for execution.
     * @return                          A Map of Container Ids to segment names representing all segments present in the
     *                                  container metadata segment of a Container.
     * @throws Exception                If an exception occurred. This could be one of the following:
     *                                      * TimeoutException:     If If the call for computation(used in the method)
     *                                                              didn't complete in time.
     *                                      * IOException     :     If a general IO exception occurred.
     */
    private static Map<Integer, Set<String>> getExistingSegments(Map<Integer, DebugStreamSegmentContainer> containerMap,
                                                                 ExecutorService executorService) throws Exception {
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
     * 1. If the segment is present in the {@link MetadataStore} of the container and its length or sealed status or both
     * doesn't match with the corresponding details from the given {@link SegmentProperties}, then it is deleted from there
     * and registered using the details from the given {@link SegmentProperties} instance.
     * 2. If the segment is absent in the {@link MetadataStore}, then it is registered using the details from the given
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
                        .thenCompose(e -> {
                            if (segmentLength != e.getLength() || isSealed != e.isSealed()) {
                                log.debug("Segment '{}' exists in the container's metadata store, but with a different length" +
                                        "or sealed status or both, so deleting it from there and then registering it.", segmentName);
                                return container.metadataStore.deleteSegment(segmentName, TIMEOUT)
                                        .thenCompose(x -> container.registerSegment(segmentName, segmentLength, isSealed));
                            } else {
                                return null;
                            }
                        }), ex -> ex instanceof StreamSegmentNotExistsException,
                () -> {
                    log.debug("Segment '{}' doesn't exist in the container metadata. Registering it.", segmentName);
                    return container.registerSegment(segmentName, segmentLength, isSealed);
                });
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
        return Futures.exceptionallyExpecting(
                storage.openWrite(segmentName).thenCompose(segmentHandle -> storage.delete(segmentHandle, TIMEOUT)),
                ex -> ex instanceof StreamSegmentNotExistsException, null);
    }


    /**
     * Copies the contents of container metadata segment and its attribute segment to new segments. The new names to the segments
     * are passed as parameters.
     * @param storage                    A {@link Storage} instance where segments are stored.
     * @param containerId                A Container Id to get the name of the metadata segment.
     * @param backUpMetadataSegmentName  A name to the back up metadata segment.
     * @param backUpAttributeSegmentName A name to the back attribute segment.
     * @param executorService            A thread pool for execution.
     * @return                           CompletableFuture which when completed will have the segments' contents copied to another segments.
     */
    public static CompletableFuture<Void> backUpMetadataAndAttributeSegments(Storage storage, int containerId,
                                                                             String backUpMetadataSegmentName,
                                                                             String backUpAttributeSegmentName,
                                                                             ExecutorService executorService) {
        Preconditions.checkNotNull(storage);
        String metadataSegmentName = NameUtils.getMetadataSegmentName(containerId);
        String attributeSegmentName = NameUtils.getAttributeSegmentName(metadataSegmentName);
        return copySegment(storage, metadataSegmentName, backUpMetadataSegmentName, executorService)
                        .thenAcceptAsync(x -> copySegment(storage, attributeSegmentName, backUpAttributeSegmentName,
                                executorService));
    }

    /**
     * Given the back up of metadata segments, this method iterates through all segments in it and gets their attributes
     * and updates it in the container metadata segment of the given {@link DebugStreamSegmentContainer} instance.
     * @param metadataSegments  A map of back of metadata segments along with their container Ids.
     * @param containersMap     A map of {@link DebugStreamSegmentContainer} instances with their container Ids.
     * @param executorService   A thread pool for execution.
     * @throws Exception        If an exception occurred. This could be one of the following:
     *                              * TimeoutException:     If the calls for computation(used in the method)
     *                                                      didn't complete in time.
     *                              * IOException     :     If a general IO exception occurred.
     */
    public static void updateCoreAttributes(Map<Integer, String> metadataSegments,
                                            Map<Integer, DebugStreamSegmentContainer> containersMap,
                                            ExecutorService executorService) throws Exception {
        val args = IteratorArgs.builder().fetchTimeout(TIMEOUT).build();
        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(containersMap.size());

        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (val metadataEntry : metadataSegments.entrySet()) {
            val containerForSegmentsContained = containersMap.get(metadataEntry.getKey());
            val containerForBackUpMetadataSegment = containersMap.get(segToConMapper.getContainerId(metadataEntry.getValue()));
            log.info("container id: {}, segment Name: {}", containerForBackUpMetadataSegment.getId(), metadataEntry.getValue());
            val tableExtension = containerForBackUpMetadataSegment.getExtension(ContainerTableExtension.class);
            val entryIterator = tableExtension.entryIterator(metadataEntry.getValue(), args)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            val metadataSegmentName = NameUtils.getMetadataSegmentName(containerForSegmentsContained.getId());
            // Store the segments in a set
            entryIterator.forEachRemaining(item -> {
                for (val entry : item.getEntries()) {
                    val segmentInfo = MetadataStore.SegmentInfo.deserialize(entry.getValue());
                    val properties = segmentInfo.getProperties();

                    // skip, if this is a metadata segment
                    if (metadataSegmentName.equals(properties.getName())) {
                        continue;
                    }

                    List<AttributeUpdate> attributeUpdates = properties.getAttributes().entrySet().stream()
                            .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.Replace, e.getValue()))
                            .collect(Collectors.toList());
                    log.info("Updating attributes = {} for segment '{}'", attributeUpdates, properties.getName());
                    futures.add(Futures.exceptionallyExpecting(
                            containerForSegmentsContained.updateAttributes(properties.getName(), attributeUpdates, TIMEOUT),
                            ex -> ex instanceof StreamSegmentNotExistsException, null));
                }
            }, executorService).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
        Futures.allOf(futures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a target segment with the given name and copies the contents of the source segment to the target segment.
     * @param storage                   A storage instance to create the segment.
     * @param sourceSegment             The name of the source segment to copy the contents from.
     * @param targetSegment             The name of the segment to write the contents to.
     * @param executor                  A thread pool for execution.
     * @return                          A CompletableFuture that, when completed normally, will indicate the operation
     * completed. If the operation failed, the future will be failed with the causing exception.
     */
    protected static CompletableFuture<Void> copySegment(Storage storage, String sourceSegment, String targetSegment, ExecutorService executor) {
        byte[] buffer = new byte[BUFFER_SIZE];
        return storage.create(targetSegment, TIMEOUT).thenComposeAsync(targetHandle -> {
            return storage.getStreamSegmentInfo(sourceSegment, TIMEOUT).thenComposeAsync(info -> {
                return storage.openRead(sourceSegment).thenComposeAsync(sourceHandle -> {
                    AtomicInteger offset = new AtomicInteger(0);
                    AtomicInteger bytesToRead = new AtomicInteger((int) info.getLength());
                    return Futures.loop(
                            () -> bytesToRead.get() > 0,
                            () -> {
                                log.info("Reading");
                                return storage.read(sourceHandle, offset.get(), buffer, 0, Math.min(BUFFER_SIZE, bytesToRead.get()), TIMEOUT)
                                        .thenComposeAsync(size -> {
                                            return (size > 0) ? storage.write(targetHandle, offset.get(), new ByteArrayInputStream(buffer, 0, size), size, TIMEOUT)
                                                    .thenAcceptAsync(r -> {
                                                        bytesToRead.addAndGet(-size);
                                                        offset.addAndGet(size);
                                            }, executor) : null;
                                        }, executor);
                            }, executor);
                }, executor);
            }, executor);
        }, executor);
    }
}
