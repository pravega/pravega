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
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.io.ByteArrayInputStream;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static io.pravega.shared.NameUtils.getMetadataSegmentName;
import static io.pravega.shared.NameUtils.getStorageMetadataSegmentName;

/**
 * Utility methods for container recovery.
 */
@Slf4j
public class ContainerRecoveryUtils {
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
     * @param timeout                           Timeout for the operation.
     * @throws Exception                        If an exception occurred. This could be one of the following:
     *                                              * TimeoutException:     If the calls for computation(used in the method)
     *                                                                      didn't complete in time.
     *                                              * IOException     :     If a general IO exception occurred.
     */
    public static void recoverAllSegments(Storage storage, Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainersMap,
                                          ExecutorService executorService, Duration timeout) throws Exception {
        Preconditions.checkNotNull(storage);
        Preconditions.checkNotNull(executorService);
        Preconditions.checkNotNull(debugStreamSegmentContainersMap);
        Preconditions.checkArgument(debugStreamSegmentContainersMap.size() > 0, "There should be at least one " +
                "debug segment container instance.");
        int containerCount = debugStreamSegmentContainersMap.size();
        validateContainerIds(debugStreamSegmentContainersMap, containerCount);

        log.info("Recovery started for all containers...");
        // Get all segments in the metadata store for each debug segment container instance.
        Map<Integer, Set<String>> existingSegmentsMap = getExistingSegments(debugStreamSegmentContainersMap, executorService, false,
                timeout);

        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(containerCount, true);

        Iterator<SegmentProperties> segmentIterator = storage.listSegments().join();
        Preconditions.checkNotNull(segmentIterator);

        // Iterate through all segments. Create each one of their using their respective debugSegmentContainer instance.
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        while (segmentIterator.hasNext()) {
            val currentSegment = segmentIterator.next();
            int containerId = segToConMapper.getContainerId(currentSegment.getName());
            String currentSegmentName = currentSegment.getName();
            // skip recovery if the segment is an attribute segment or metadata segment.
            if (NameUtils.isAttributeSegment(currentSegmentName) || getMetadataSegmentName(containerId).equals(currentSegmentName)) {
                continue;
            }

            existingSegmentsMap.get(containerId).remove(currentSegment.getName());
            futures.add(recoverSegment(debugStreamSegmentContainersMap.get(containerId), currentSegment, timeout));
        }
        Futures.allOf(futures).get(timeout.toMillis(), TimeUnit.MILLISECONDS);

        futures.clear();
        // Delete segments which only exist in the container metadata, not in storage.
        for (val existingSegmentsSetEntry : existingSegmentsMap.entrySet()) {
            for (String segmentName : existingSegmentsSetEntry.getValue()) {
                log.info("Deleting segment '{}' as it is not in the storage.", segmentName);
                futures.add(debugStreamSegmentContainersMap.get(existingSegmentsSetEntry.getKey())
                        .deleteStreamSegment(segmentName, timeout));
            }
        }
        Futures.allOf(futures).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
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
     * @param isStorageMetadata         If true iterate storage_metadata else container_metadata.
     * @param timeout                   Timeout for the operation.
     * @return                          A Map of Container Ids to segment names representing all segments present in the
     *                                  container metadata segment of a Container.
     * @throws Exception                If an exception occurred. This could be one of the following:
     *                                      * TimeoutException:     If If the call for computation(used in the method)
     *                                                              didn't complete in time.
     *                                      * IOException     :     If a general IO exception occurred.
     */
    public static Map<Integer, Set<String>> getExistingSegments(Map<Integer, DebugStreamSegmentContainer> containerMap,
                                                                 ExecutorService executorService, boolean isStorageMetadata,
                                                                 Duration timeout) throws Exception {
        Map<Integer, Set<String>> metadataSegmentsMap = new HashMap<>();
        val args = IteratorArgs.builder().fetchTimeout(timeout).build();

        // Get all segments for each container entry
        for (val containerEntry : containerMap.entrySet()) {
            Preconditions.checkNotNull(containerEntry.getValue());
            val tableExtension = containerEntry.getValue().getExtension(ContainerTableExtension.class);
            val metadataSegmentName = isStorageMetadata ? getStorageMetadataSegmentName(containerEntry.getKey()) : getMetadataSegmentName(containerEntry.getKey());
            val keyIterator = tableExtension.keyIterator(metadataSegmentName, args).get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            // Store the segments in a set
            Set<String> metadataSegments = new HashSet<>();
            Futures.exceptionallyExpecting(keyIterator.forEachRemaining(k ->
                    metadataSegments.addAll(k.getEntries().stream()
                            .map(entry -> new String(entry.getKey().getCopy()))
                            .collect(Collectors.toSet())), executorService),
                    ex -> ex instanceof StreamSegmentNotExistsException, null).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
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
     * @param timeout           Timeout for the operation.
     * @return                  CompletableFuture which when completed will have the segment registered on to the container
     *                          metadata.
     */
    private static CompletableFuture<Void> recoverSegment(DebugStreamSegmentContainer container, SegmentProperties storageSegment,
                                                          Duration timeout) {
        Preconditions.checkNotNull(container);
        Preconditions.checkNotNull(storageSegment);
        long segmentLength = storageSegment.getLength();
        boolean isSealed = storageSegment.isSealed();
        String segmentName = storageSegment.getName();

        log.info("Registering: {}, {}, {}.", segmentName, segmentLength, isSealed);
        return Futures.exceptionallyComposeExpecting(
                container.getStreamSegmentInfo(storageSegment.getName(), timeout)
                        .thenCompose(e -> {
                            if (segmentLength != e.getLength() || isSealed != e.isSealed()) {
                                log.debug("Segment '{}' exists in the container's metadata store, but with a different length" +
                                        "or sealed status or both, so deleting it from there and then registering it.", segmentName);
                                return container.metadataStore.deleteSegment(segmentName, timeout)
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
     * @param timeout       Timeout for the operation.
     * @return              A CompletableFuture that, when completed normally, will indicate the operation
     * completed. If the operation failed, the future will be failed with the causing exception.
     */
    public static CompletableFuture<Void> deleteMetadataAndAttributeSegments(Storage storage, int containerId, Duration timeout) {
        Preconditions.checkNotNull(storage);
        String metadataSegmentName = NameUtils.getMetadataSegmentName(containerId);
        String attributeSegmentName = NameUtils.getAttributeSegmentName(metadataSegmentName);
        return CompletableFuture.allOf(deleteSegmentFromStorage(storage, metadataSegmentName, timeout),
                deleteSegmentFromStorage(storage, attributeSegmentName, timeout));
    }

    /**
     * Deletes the segment with given name from the given {@link Storage} instance. If the segment doesn't exist, it does
     * nothing and returns.
     * @param storage       A {@link Storage} instance to delete the segments from.
     * @param segmentName   Name of the segment to be deleted.
     * @param timeout       Timeout for the operation.
     * @return              CompletableFuture which when completed will have the segment deleted. In case segment didn't
     *                      exist, a completed future will be returned.
     */
    private static CompletableFuture<Void> deleteSegmentFromStorage(Storage storage, String segmentName, Duration timeout) {
        log.info("Deleting Segment '{}'", segmentName);
        return Futures.exceptionallyExpecting(
                storage.openWrite(segmentName).thenCompose(segmentHandle -> storage.delete(segmentHandle, timeout)),
                ex -> ex instanceof StreamSegmentNotExistsException, null);
    }


    /**
     * Copies the contents of container metadata segment and its attribute segment to new segments. The new names of the segments
     * are passed as parameters.
     * @param storage                    A {@link Storage} instance where segments are stored.
     * @param containerId                A Container Id to get the name of the metadata segment.
     * @param backUpMetadataSegmentName  A name of the back up metadata segment.
     * @param backUpAttributeSegmentName A name of the back attribute segment.
     * @param executorService            A thread pool for execution.
     * @param timeout                    Timeout for the operation.
     * @return                           A CompletableFuture which when completed will indicate the operation has completed.
     *                                   If the operation failed, the future will be failed with the causing exception.
     */
    public static CompletableFuture<Void> backUpMetadataAndAttributeSegments(Storage storage, int containerId,
                                                                             String backUpMetadataSegmentName,
                                                                             String backUpAttributeSegmentName,
                                                                             ExecutorService executorService,
                                                                             Duration timeout) {
        Preconditions.checkNotNull(storage);
        String metadataSegmentName = NameUtils.getMetadataSegmentName(containerId);
        String attributeSegmentName = NameUtils.getAttributeSegmentName(metadataSegmentName);
        return CompletableFuture.allOf(copySegment(storage, metadataSegmentName, backUpMetadataSegmentName, executorService, timeout),
                copySegment(storage, attributeSegmentName, backUpAttributeSegmentName, executorService, timeout));
    }

    /**
     * Updates Core Attributes for all Segments for the given Containers.
     * This method iterates through all the back copies of Container Metadata Segments, interprets all entries as
     * Segment-SegmentInfo mappings and extracts the Core Attributes for each. These Core Attributes are then applied
     * to the same segments in the given Containers.
     * @param backUpMetadataSegments    A map of back copies of metadata segments along with their container Ids.
     * @param containersMap             A map of {@link DebugStreamSegmentContainer} instances with their container Ids.
     * @param executorService           A thread pool for execution.
     * @param timeout                   Timeout for the operation.
     * @throws InterruptedException     If the operation was interrupted while waiting.
     * @throws TimeoutException         If the timeout expired prior to being able to complete update attributes for all segments.
     * @throws ExecutionException       When execution of update attributes to all segments encountered an error.
     */
    public static void updateCoreAttributes(Map<Integer, String> backUpMetadataSegments,
                                            Map<Integer, DebugStreamSegmentContainer> containersMap,
                                            ExecutorService executorService,
                                            Duration timeout) throws InterruptedException, ExecutionException,
            TimeoutException {
        Preconditions.checkState(backUpMetadataSegments.size() == containersMap.size(), "The number of " +
                "back-up metadata segments = %s and the number of containers = %s should match.", backUpMetadataSegments.size(),
                containersMap.size());

        val args = IteratorArgs.builder().fetchTimeout(timeout).build();
        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(containersMap.size(), true);

        // Iterate through all back up metadata segments
        for (val backUpMetadataSegmentEntry : backUpMetadataSegments.entrySet()) {
            // Get the name of original metadata segment
            val metadataSegment = NameUtils.getMetadataSegmentName(backUpMetadataSegmentEntry.getKey());

            // Get the name of back up metadata segment
            val backUpMetadataSegment = backUpMetadataSegmentEntry.getValue();

            // Get the container for back up metadata segment
            val containerForBackUpMetadataSegment = containersMap.get(segToConMapper.getContainerId(
                    backUpMetadataSegment));
            log.info("Back up container metadata segment name: {} and its container id: {}", backUpMetadataSegment,
                    containerForBackUpMetadataSegment.getId());

            // Get the container for segments inside back up metadata segment
            val container = containersMap.get(backUpMetadataSegmentEntry.getKey());

            // Make sure the backup segment is registered as a table segment.
            val bmsInfo = containerForBackUpMetadataSegment.getStreamSegmentInfo(backUpMetadataSegment, timeout)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (bmsInfo.getAttributes().getOrDefault(TableAttributes.INDEX_OFFSET, Attributes.NULL_ATTRIBUTE_VALUE) == Attributes.NULL_ATTRIBUTE_VALUE) {
                log.info("Back up container metadata segment name: {} does not have INDEX_OFFSET set; setting to 0 (forcing reindexing).", backUpMetadataSegment);
                containerForBackUpMetadataSegment.forSegment(backUpMetadataSegment, timeout)
                        .thenCompose(s -> s.updateAttributes(AttributeUpdateCollection.from(new AttributeUpdate(TableAttributes.INDEX_OFFSET, AttributeUpdateType.Replace, 0)), timeout))
                        .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                refreshDerivedProperties(backUpMetadataSegment, containerForBackUpMetadataSegment);
            }

            // Get the iterator to iterate through all segments in the back up metadata segment
            val tableExtension = containerForBackUpMetadataSegment.getExtension(ContainerTableExtension.class);
            val entryIterator = tableExtension.entryIterator(backUpMetadataSegment, args)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            val futures = new ArrayList<CompletableFuture<Void>>();

            // Iterating through all segments in the back up metadata segment
            entryIterator.forEachRemaining(item -> {
                for (val entry : item.getEntries()) {

                    val segmentInfo = MetadataStore.SegmentInfo.deserialize(entry.getValue());
                    val properties = segmentInfo.getProperties();

                    // skip, if this is original metadata segment
                    if (properties.getName().equals(metadataSegment)) {
                        continue;
                    }

                    // Get the attributes for the current segment
                    val attributeUpdates = properties.getAttributes().entrySet().stream()
                            .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.Replace, e.getValue()))
                            .collect(Collectors.toCollection(AttributeUpdateCollection::new));
                    log.info("Segment Name: {} Attributes Updates: {}", properties.getName(), attributeUpdates);

                    // Update attributes for the current segment
                    futures.add(Futures.exceptionallyExpecting(
                            container.updateAttributes(properties.getName(), attributeUpdates, timeout)
                                    .thenRun(() -> refreshDerivedProperties(properties.getName(), container)),
                            ex -> ex instanceof StreamSegmentNotExistsException, null));
                }
            }, executorService).get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            // Waiting for update attributes for all segments in each back up metadata segment.
            Futures.allOf(futures).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static void refreshDerivedProperties(String segmentName, DebugStreamSegmentContainer container) {
        val m = container.getMetadata().getStreamSegmentMetadata(
                container.getMetadata().getStreamSegmentId(segmentName, false));
        if (m != null) {
            m.refreshDerivedProperties();
        }
    }

    /**
     * Creates a target segment with the given name and copies the contents of the source segment to the target segment.
     * @param storage                   A storage instance to create the segment.
     * @param sourceSegment             The name of the source segment to copy the contents from.
     * @param targetSegment             The name of the segment to write the contents to.
     * @param executor                  A thread pool for execution.
     * @param timeout                   Timeout for the operation.
     * @return                          A CompletableFuture that, when completed normally, will indicate the operation
     * completed. If the operation failed, the future will be failed with the causing exception.
     */
    protected static CompletableFuture<Void> copySegment(Storage storage, String sourceSegment, String targetSegment, ExecutorService executor,
                                                         Duration timeout) {
        byte[] buffer = new byte[BUFFER_SIZE];
        return storage.create(targetSegment, timeout).thenComposeAsync(targetHandle -> {
            return storage.getStreamSegmentInfo(sourceSegment, timeout).thenComposeAsync(info -> {
                return storage.openRead(sourceSegment).thenComposeAsync(sourceHandle -> {
                    AtomicInteger offset = new AtomicInteger(0);
                    AtomicInteger bytesToRead = new AtomicInteger((int) info.getLength());
                    return Futures.loop(
                            () -> bytesToRead.get() > 0,
                            () -> {
                                return storage.read(sourceHandle, offset.get(), buffer, 0, Math.min(BUFFER_SIZE, bytesToRead.get()), timeout)
                                        .thenComposeAsync(size -> {
                                            return (size > 0) ? storage.write(targetHandle, offset.get(), new ByteArrayInputStream(buffer, 0, size), size, timeout)
                                                    .thenAcceptAsync(r -> {
                                                        bytesToRead.addAndGet(-size);
                                                        offset.addAndGet(size);
                                                        }, executor) : CompletableFuture.<Void>completedFuture(null);
                                            }, executor);
                                }, executor);
                    }, executor);
                }, executor);
            }, executor);
    }

    /**
     * This method creates a back up segment of container metadata segment and its attribute segment for each
     * container Id. The original metadata segments and its attribute segments are deleted and the back up copy
     * of original metadata segments are stored in a map and returned.
     *
     * @param storage                   A {@link Storage} instance to get the segments from.
     * @param containerCount            The number of containers for which renaming of container metadata segment and its
     *                                  attributes segment has to be performed.
     * @param executorService           A thread pool for execution.
     * @param timeout                   Timeout for the operation.
     * @return                          A CompletableFuture that, when completed normally, will have a Map of Container
     * Ids to new container metadata segment names.
     */
    public static synchronized CompletableFuture<Map<Integer, String>> createBackUpMetadataSegments(Storage storage,
                                                                                                    int containerCount,
                                                                                                    ExecutorService executorService,
                                                                                                    Duration timeout) {
        String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        Map<Integer, String> backUpMetadataSegments = new HashMap<>();

        val futures = new ArrayList<CompletableFuture<Void>>();

        for (int containerId = 0; containerId < containerCount; containerId++) {
            String backUpMetadataSegment = NameUtils.getMetadataSegmentName(containerId) + fileSuffix;
            String backUpAttributeSegment = NameUtils.getAttributeSegmentName(backUpMetadataSegment);
            log.debug("Created '{}' as a back of metadata segment of container Id '{}'", backUpMetadataSegment, containerId);

            val finalContainerId = containerId;
            futures.add(Futures.exceptionallyExpecting(
                    ContainerRecoveryUtils.backUpMetadataAndAttributeSegments(storage, containerId,
                            backUpMetadataSegment, backUpAttributeSegment, executorService, timeout)
                            .thenCompose(x -> ContainerRecoveryUtils.deleteMetadataAndAttributeSegments(storage, finalContainerId, timeout)),
                                    ex -> Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException, null));
            backUpMetadataSegments.put(finalContainerId, backUpMetadataSegment);
        }
        return Futures.allOf(futures).thenApply(v -> backUpMetadataSegments);
    }
}
