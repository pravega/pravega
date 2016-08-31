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

package com.emc.pravega.service.server.logs;

import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.OperationLog;
import com.emc.pravega.service.server.StreamSegmentNameUtils;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentMapper;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
import com.emc.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import com.emc.pravega.service.storage.Storage;
import com.google.common.collect.Iterators;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

/**
 * Helpers that aid in testing Log-based components, such as DurableLog or OperationQueueProcessor.
 */
public class LogTestHelpers {
    private static final int MAX_SEGMENT_COUNT = 1000 * 1000;

    /**
     * Updates the given Container Metadata to have a number of StreamSegments. All created StreamSegments will have
     * Ids from 0 to the value of streamSegmentCount.
     *
     * @param streamSegmentCount
     * @param containerMetadata
     * @return
     */
    public static HashSet<Long> createStreamSegmentsInMetadata(int streamSegmentCount, UpdateableContainerMetadata containerMetadata) {
        assert streamSegmentCount <= MAX_SEGMENT_COUNT : "cannot have more than " + MAX_SEGMENT_COUNT + " StreamSegments for this test.";
        HashSet<Long> result = new HashSet<>();
        for (long streamSegmentId = 0; streamSegmentId < streamSegmentCount; streamSegmentId++) {
            result.add(streamSegmentId);
            String name = getStreamSegmentName(streamSegmentId);
            UpdateableSegmentMetadata segmentMetadata = containerMetadata.mapStreamSegmentId(name, streamSegmentId);
            segmentMetadata.setDurableLogLength(0);
            segmentMetadata.setStorageLength(0);
        }

        return result;
    }

    /**
     * Creates a number of StreamSegments in the given Metadata and OperationLog.
     *
     * @param streamSegmentCount
     * @param containerMetadata
     * @param durableLog
     * @param storage
     * @return
     */
    public static HashSet<Long> createStreamSegmentsWithOperations(int streamSegmentCount, ContainerMetadata containerMetadata, OperationLog durableLog, Storage storage) {
        StreamSegmentMapper mapper = new StreamSegmentMapper(containerMetadata, durableLog, storage, ForkJoinPool.commonPool());
        HashSet<Long> result = new HashSet<>();
        for (int i = 0; i < streamSegmentCount; i++) {
            String name = getStreamSegmentName(i);
            long streamSegmentId = mapper
                    .createNewStreamSegment(name, Duration.ZERO)
                    .thenCompose((v) -> mapper.getOrAssignStreamSegmentId(name, Duration.ZERO)).join();
            result.add(streamSegmentId);
        }

        return result;
    }

    /**
     * Updates the given Container Metadata to have a number of Batches mapped to the given StreamSegment Ids.
     *
     * @param streamSegmentIds
     * @param batchesPerStreamSegment
     * @param containerMetadata
     * @return
     */
    public static AbstractMap<Long, Long> createBatchesInMetadata(HashSet<Long> streamSegmentIds, int batchesPerStreamSegment, UpdateableContainerMetadata containerMetadata) {
        assert batchesPerStreamSegment <= MAX_SEGMENT_COUNT : "cannot have more than " + MAX_SEGMENT_COUNT + " batches per StreamSegment for this test.";
        HashMap<Long, Long> result = new HashMap<>();
        for (long streamSegmentId : streamSegmentIds) {
            String streamSegmentName = containerMetadata.getStreamSegmentMetadata(streamSegmentId).getName();

            for (int i = 0; i < batchesPerStreamSegment; i++) {
                long batchId = getBatchId(streamSegmentId, i);
                assert result.put(batchId, streamSegmentId) == null : "duplicate BatchId generated: " + batchId;
                assert !streamSegmentIds.contains(batchId) : "duplicate StreamSegmentId (batch) generated: " + batchId;
                String batchName = StreamSegmentNameUtils.generateBatchStreamSegmentName(streamSegmentName);
                UpdateableSegmentMetadata batchMetadata = containerMetadata.mapStreamSegmentId(batchName, batchId, streamSegmentId);
                batchMetadata.setDurableLogLength(0);
                batchMetadata.setStorageLength(0);
            }
        }

        return result;
    }

    public static AbstractMap<Long, Long> createBatchesWithOperations(HashSet<Long> streamSegmentIds, int batchesPerStreamSegment, ContainerMetadata containerMetadata, OperationLog durableLog, Storage storage) {
        HashMap<Long, Long> result = new HashMap<>();
        StreamSegmentMapper mapper = new StreamSegmentMapper(containerMetadata, durableLog, storage, ForkJoinPool.commonPool());
        for (long streamSegmentId : streamSegmentIds) {
            String streamSegmentName = containerMetadata.getStreamSegmentMetadata(streamSegmentId).getName();

            for (int i = 0; i < batchesPerStreamSegment; i++) {
                long batchId = mapper
                        .createNewBatchStreamSegment(streamSegmentName, Duration.ZERO)
                        .thenCompose(v -> mapper.getOrAssignStreamSegmentId(v, Duration.ZERO)).join();
                result.put(batchId, streamSegmentId);
            }
        }

        return result;
    }

    /**
     * Generates a List of Log Operations that contains the following operations, in the "correct" order.
     * <ol>
     * <li> A set of StreamSegmentAppend Operations (based on the streamSegmentIds arg).
     * <li> A set of StreamSegmentSeal and MergeBatch Operations (based on the batchIds and mergeBatches arg).
     * <li> A set of StreamSegmentSeal Operations (based on the sealStreamSegments arg).
     * </ol>
     *
     * @param streamSegmentIds
     * @param batchIds
     * @param appendsPerStreamSegment
     * @param metadataCheckpointsEvery
     * @param mergeBatches
     * @param sealStreamSegments
     * @return
     */
    public static List<Operation> generateOperations(Collection<Long> streamSegmentIds, AbstractMap<Long, Long> batchIds, int appendsPerStreamSegment, int metadataCheckpointsEvery, boolean mergeBatches, boolean sealStreamSegments) {
        List<Operation> result = new ArrayList<>();

        // Add some appends.
        int appendId = 0;
        for (long streamSegmentId : streamSegmentIds) {
            for (int i = 0; i < appendsPerStreamSegment; i++) {
                result.add(new StreamSegmentAppendOperation(streamSegmentId, generateAppendData(appendId), new AppendContext(UUID.randomUUID(), i)));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
                appendId++;
            }
        }

        for (long batchId : batchIds.keySet()) {
            for (int i = 0; i < appendsPerStreamSegment; i++) {
                result.add(new StreamSegmentAppendOperation(batchId, generateAppendData(appendId), new AppendContext(UUID.randomUUID(), i)));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
                appendId++;
            }
        }

        // Merge batches.
        if (mergeBatches) {
            // Key = BatchId, Value = Parent Id.
            batchIds.entrySet().forEach(mapping -> {
                result.add(new StreamSegmentSealOperation(mapping.getKey()));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
                result.add(new MergeBatchOperation(mapping.getValue(), mapping.getKey()));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
            });
        }

        // Seal the StreamSegments.
        if (sealStreamSegments) {
            streamSegmentIds.forEach(streamSegmentId -> {
                result.add(new StreamSegmentSealOperation(streamSegmentId));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
            });
        }

        return result;
    }

    /**
     * Given a list of LogOperations, calculates the final lengths of the StreamSegments that are encountered, by inspecting
     * every StreamSegmentAppendOperation and MergeBatchOperation. All other types of Log Operations are ignored.
     *
     * @param operations
     * @return
     */
    public static AbstractMap<Long, Integer> getExpectedLengths(Collection<OperationWithCompletion> operations) {
        HashMap<Long, Integer> result = new HashMap<>();
        for (OperationWithCompletion o : operations) {
            Assert.assertTrue("Operation is not completed.", o.completion.isDone());
            if (o.completion.isCompletedExceptionally()) {
                // This is failed operation; ignore it.
                continue;
            }

            if (o.operation instanceof StreamSegmentAppendOperation) {
                StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) o.operation;
                result.put(
                        appendOperation.getStreamSegmentId(),
                        result.getOrDefault(appendOperation.getStreamSegmentId(), 0) + appendOperation.getData().length);
            } else if (o.operation instanceof MergeBatchOperation) {
                MergeBatchOperation mergeOperation = (MergeBatchOperation) o.operation;

                result.put(
                        mergeOperation.getStreamSegmentId(),
                        result.getOrDefault(mergeOperation.getStreamSegmentId(), 0) + result.getOrDefault(mergeOperation.getBatchStreamSegmentId(), 0));
                result.remove(mergeOperation.getBatchStreamSegmentId());
            }
        }

        return result;
    }

    /**
     * Given a list of Log Operations, generates an InputStream for each encountered StreamSegment that contains the final
     * contents of that StreamSegment. Only considers operations of type StreamSegmentAppendOperation and MergeBatchOperation.
     *
     * @param operations
     * @return
     */
    public static AbstractMap<Long, InputStream> getExpectedContents(Collection<OperationWithCompletion> operations) {
        HashMap<Long, List<ByteArrayInputStream>> partialContents = new HashMap<>();
        for (OperationWithCompletion o : operations) {
            Assert.assertTrue("Operation is not completed.", o.completion.isDone());
            if (o.completion.isCompletedExceptionally()) {
                // This is failed operation; ignore it.
                continue;
            }

            if (o.operation instanceof StreamSegmentAppendOperation) {
                StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) o.operation;
                List<ByteArrayInputStream> segmentContents = partialContents.get(appendOperation.getStreamSegmentId());
                if (segmentContents == null) {
                    segmentContents = new ArrayList<>();
                    partialContents.put(appendOperation.getStreamSegmentId(), segmentContents);
                }

                segmentContents.add(new ByteArrayInputStream(appendOperation.getData()));
            } else if (o.operation instanceof MergeBatchOperation) {
                MergeBatchOperation mergeOperation = (MergeBatchOperation) o.operation;
                List<ByteArrayInputStream> targetSegmentContents = partialContents.get(mergeOperation.getStreamSegmentId());
                if (targetSegmentContents == null) {
                    targetSegmentContents = new ArrayList<>();
                    partialContents.put(mergeOperation.getStreamSegmentId(), targetSegmentContents);
                }

                List<ByteArrayInputStream> sourceSegmentContents = partialContents.get(mergeOperation.getBatchStreamSegmentId());
                targetSegmentContents.addAll(sourceSegmentContents);
                partialContents.remove(mergeOperation.getBatchStreamSegmentId());
            }
        }

        // Construct final result.
        HashMap<Long, InputStream> result = new HashMap<>();
        for (Map.Entry<Long, List<ByteArrayInputStream>> e : partialContents.entrySet()) {
            result.put(e.getKey(), new SequenceInputStream(Iterators.asEnumeration(e.getValue().iterator())));
        }

        return result;
    }

    public static String getStreamSegmentName(long streamSegmentId) {
        return String.format("StreamSegment_%d", streamSegmentId);
    }

    public static long getBatchId(long streamSegmentId, int batchId) {
        return (streamSegmentId + 1) * MAX_SEGMENT_COUNT + batchId;
    }

    public static byte[] generateAppendData(int appendId) {
        return String.format("Append_%d", appendId).getBytes();
    }

    public static CompletableFuture<Void> allOf(Collection<OperationWithCompletion> operations) {
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        operations.forEach(oc -> futures.add(oc.completion));
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }

    private static void addCheckpointIfNeeded(List<Operation> operations, int metadataCheckpointsEvery) {
        if (metadataCheckpointsEvery > 0 && operations.size() % metadataCheckpointsEvery == 0) {
            operations.add(new MetadataCheckpointOperation());
        }
    }

    //region OperationWithCompletion

    public static class OperationWithCompletion {
        public final Operation operation;
        public final CompletableFuture<Long> completion;

        public OperationWithCompletion(Operation operation, CompletableFuture<Long> completion) {
            this.operation = operation;
            this.completion = completion;
        }

        @Override
        public String toString() {
            return String.format(
                    "(%s) %s",
                    this.completion.isDone() ? (this.completion.isCompletedExceptionally() ? "Error" : "Complete") : "Not Completed",
                    this.operation);
        }
    }

    //endregion
}
