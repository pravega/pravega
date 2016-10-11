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
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.concurrent.FutureCollectionHelper;
import com.emc.pravega.controller.store.stream.tables.Create;
import com.emc.pravega.controller.store.stream.tables.HistoryRecord;
import com.emc.pravega.controller.store.stream.tables.IndexRecord;
import com.emc.pravega.controller.store.stream.tables.Scale;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.controller.store.stream.tables.Task;
import com.emc.pravega.stream.StreamConfiguration;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractStreamBase implements Stream {
    private final String name;

    protected AbstractStreamBase(final String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    /***
     * Creates a new stream record in the stream store.
     * Create a new task of type Create.
     * If create task already exists, use that and bring it to completion
     * If no task exists, fall through all create steps. They are all idempotent
     * <p>
     * Create Steps:
     * 0. Take distributed mutex
     * 1. Create task/Fetch existing task
     * 2. Create a new znode to store configuration
     * 3. Create a new znode for segment table.
     * 4. Create a new znode for history table.
     * 5. Create a new znode for index
     * 6. delete task
     * 7. release mutex
     *
     * @param configuration stream configuration.
     * @return
     */
    @Override
    public CompletableFuture<Boolean> create(final StreamConfiguration configuration) {
        return acquireDistributedLock()
                .thenCompose(x -> getTaskDataIfExists())
                .thenCompose(taskData -> processCreateTask(configuration, taskData))
                        // Let create object fall through to all future callbacks as their own return values are uninteresting
                .thenCompose(create -> createConfiguration(configuration, create))
                .thenCompose(this::createSegmentTable)
                .thenCompose(this::createSegmentFile)
                .thenCompose(this::createHistoryTable)
                .thenCompose(this::createIndexTable)
                .thenCompose(x -> deleteTask())
                .handle((ok, ex) -> {
                    try {
                        releaseDistributedLock().get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    if (ex == null)
                        return true;
                    else {
                        throw new RuntimeException(ex);
                    }
                });
    }

    /**
     * Update configuration in zk at configurationPath
     *
     * @param configuration new stream configuration.
     * @return
     */
    @Override
    public CompletableFuture<Boolean> updateConfiguration(final StreamConfiguration configuration) {
        // replace the configurationPath zknode with new configurationPath
        return setConfigurationData(configuration).thenApply(x -> true);
    }

    /**
     * Fetch configuration from zk at configurationPath
     *
     * @return
     */
    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration() {
        return getConfigurationData();
    }

    /**
     * Compute correct znode name for the segment chunk that contains entry for this segment.
     * Fetch the segment table chunk and retrieve the segment
     *
     * @param number segment number.
     * @return
     */
    @Override
    public CompletableFuture<Segment> getSegment(final int number) {
        return getSegmentRow(number);

    }

    /**
     * Given segment number, find its successor candidates and then compute overlaps with its keyrange
     * to find successors
     *
     * @param number segment number.
     * @return
     */
    @Override
    public CompletableFuture<List<Integer>> getSuccessors(final int number) {

        final CompletableFuture[] futures = new CompletableFuture[3];

        futures[0] = getSegment(number);
        futures[1] = getIndexTable();
        futures[2] = getHistoryTable();

        return CompletableFuture.allOf(futures).thenCompose(x -> {

            try {
                final Segment segment = (Segment) futures[0].get();
                final byte[] indexTable = (byte[]) futures[1].get();
                final byte[] historyTable = (byte[]) futures[2].get();
                return FutureCollectionHelper.sequence(
                        TableHelper.findSegmentSuccessorCandidates(segment,
                                indexTable,
                                historyTable)
                                .stream()
                                .map(this::getSegment)
                                .collect(Collectors.toList()))
                        .thenApply(y -> new ImmutablePair<Segment, List<Segment>>(segment, y));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> TableHelper.getOverlaps(x.getKey(), x.getValue()));
    }

    /**
     * Find predecessor candidates and find overlaps with given segment's key range
     *
     * @param number segment number.
     * @return
     */
    @Override
    public CompletableFuture<List<Integer>> getPredecessors(final int number) {
        final CompletableFuture[] futures = new CompletableFuture[3];

        futures[0] = getSegment(number);
        futures[1] = getIndexTable();
        futures[2] = getHistoryTable();

        return CompletableFuture.allOf(futures).thenCompose(x -> {
            try {
                final Segment segment = (Segment) futures[0].get();
                final byte[] indexTable = (byte[]) futures[1].get();
                final byte[] historyTable = (byte[]) futures[2].get();
                return FutureCollectionHelper.sequence(
                        TableHelper.findSegmentPredecessorCandidates(segment,
                                indexTable,
                                historyTable)
                                .stream()
                                .map(this::getSegment)
                                .collect(Collectors.toList()))
                        .thenApply(y -> new ImmutablePair<Segment, List<Segment>>(segment, y));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> TableHelper.getOverlaps(x.getKey(), x.getValue()));
    }

    @Override
    public CompletableFuture<List<Integer>> getActiveSegments() {
        return getHistoryTable().thenApply(TableHelper::getActiveSegments);
    }

    /**
     * if timestamp is < create time of stream, we will return empty list
     * 1. perform binary searchIndex on index table to find timestamp
     * 2. fetch the record from history table for the pointer in index.
     * Note: index may be stale so we may need to fall through
     * 3. parse the row and return the list of integers
     *
     * @param timestamp point in time.
     * @return
     */
    @Override
    public CompletableFuture<List<Integer>> getActiveSegments(long timestamp) {
        final CompletableFuture<byte[]> indexFuture = getIndexTable();

        final CompletableFuture<byte[]> historyFuture = getHistoryTable();

        return indexFuture.thenCombine(historyFuture,
                (byte[] indexTable, byte[] historyTable) -> TableHelper.getActiveSegments(timestamp,
                        indexTable,
                        historyTable));
    }

    /**
     * Scale and create are two tasks where we update the table. For scale to be legitimate, it has to be
     * preceeded by create. Which means all appropriate tables exist.
     * Scale Steps:
     * 0. Take distributed mutex
     * 1. Scale task/Fetch existing task
     * 2. Verify if new scale input is same as existing scale task.
     * If not, existing takes precedence. TODO: Notify caller!
     * 3. Add new segment information in segment table.
     * Segments could spillover into a new chunk.
     * 4. Add entry into the history table.
     * 5. Add entry into the index table.
     * 6. delete task
     * 7. release mutex
     *
     * @param sealedSegments segments to be sealed
     * @param newRanges      key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @return
     */
    @Override
    public CompletableFuture<List<Segment>> scale(final List<Integer> sealedSegments,
                                                  final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                  final long scaleTimestamp) {
        // TODO Question: How do we know if we have lost the lock because of network issue. This lock implementation
        // is an ephemeral node in zk. So connection loss means auto lock release while we continue with our computation
        // following blog is interesting in suggesting solutions:
        // https://fpj.me/2016/02/10/note-on-fencing-and-distributed-locks/
        return acquireDistributedLock()
                .thenCompose(x -> getTaskDataIfExists())
                .thenCompose(x -> processScaleTask(sealedSegments, newRanges, scaleTimestamp, x))
                .thenCompose(scale -> {
                    // Note: we do not know the segment ids from the input. We need to compute the segment id while making
                    // sure we do not create duplicate entries.
                    // create new records in segment table if
                    // No new segment from requested 'newRanges' matches the range for last entry in segment table.
                    // (stronger check would be to check all new segments against last n segments where n is new segment count
                    return getSegmentChunks()
                            .thenCompose(this::getLatestChunk)
                            .thenCompose(latestSegmentData -> updateSegmentChunk(scale, latestSegmentData))
                            .thenApply(startingSegmentNumber -> new ImmutablePair<>(scale, startingSegmentNumber));
                })
                .thenCompose(pair -> {
                    // update history table if not already updated
                    // fetch last record from history table.
                    // if eventTime is different from scale.scaleTimeStamp do nothing, else create record
                    final Scale scale = pair.left;
                    final int startingSegmentNumber = pair.right;

                    return updateHistoryTable(sealedSegments, scale)
                            .thenApply(historyOffset -> new ImmutableTriple<>(scale, startingSegmentNumber, historyOffset));
                })
                .thenCompose(triple -> {
                    // update index if not already updated
                    // fetch last record from index table. if eventTime is same as scale.scaleTimeStamp do nothing,
                    // else create record
                    final Scale scale = triple.left;
                    final int historyOffset = triple.right;
                    final int startingSegmentNumber = triple.middle;

                    return updateIndexTable(scale, historyOffset, startingSegmentNumber);
                })
                .thenCompose(startingSegmentNumber -> deleteTask().thenApply(z -> startingSegmentNumber))
                .thenCompose(startingSegmentNumber -> getSegments(newRanges, startingSegmentNumber))
                .handle((ok, ex) -> {
                    try {
                        releaseDistributedLock().get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    if (ex == null) {
                        return ok;
                    } else {
                        throw new RuntimeException(ex);
                    }
                });
    }

    private CompletionStage<List<Segment>> getSegments(final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                       final Integer startingSegmentNumber) {
        final List<CompletableFuture<Segment>> segments = IntStream.range(startingSegmentNumber,
                startingSegmentNumber + newRanges.size())
                .boxed()
                .map(this::getSegment)
                .collect(Collectors.<CompletableFuture<Segment>>toList());
        return FutureCollectionHelper.sequence(segments);
    }

    private CompletionStage<Integer> updateIndexTable(final Scale scale,
                                                      final int historyOffset,
                                                      final int startingSegmentNumber) {
        return getIndexTable()
                .thenCompose(indexTable -> {
                    Optional<IndexRecord> lastRecord = IndexRecord.readLatestRecord(indexTable);
                    if (lastRecord.isPresent() && lastRecord.get().getEventTime() < scale.getScaleTimestamp()) {
                        final byte[] updated = TableHelper.updateIndexTable(indexTable,
                                scale.getScaleTimestamp(),
                                historyOffset);
                        return updateIndexTable(updated).thenApply(y -> startingSegmentNumber);
                    } else return new CompletableFuture<Integer>()
                            .thenApply(y -> startingSegmentNumber);
                });
    }

    private CompletableFuture<Integer> updateHistoryTable(final List<Integer> sealedSegments, final Scale scale) {
        return getHistoryTable()
                .thenCompose(historyTable -> {
                    final Optional<HistoryRecord> lastRecordOpt = HistoryRecord.readLatestRecord(historyTable);

                    // scale task is not allowed unless create is done which means at least one
                    // record in history table
                    assert lastRecordOpt.isPresent();

                    final HistoryRecord lastRecord = lastRecordOpt.get();

                    final int highestSegmentNumber = lastRecord
                            .getSegments()
                            .stream()
                            .max(Comparator.naturalOrder())
                            .get();
                    final List<Integer> newActiveSegments = lastRecord.getSegments();
                    newActiveSegments.removeAll(sealedSegments);
                    newActiveSegments.addAll(
                            IntStream.range(highestSegmentNumber + 1,
                                    highestSegmentNumber + scale.getNewRanges().size() + 1)
                                    .boxed()
                                    .collect(Collectors.toList()));
                    if (lastRecord.getEventTime() < scale.getScaleTimestamp()) {
                        final byte[] updated = TableHelper.updateHistoryTable(historyTable,
                                scale.getScaleTimestamp(),
                                newActiveSegments);
                        return updateHistoryTable(updated).thenApply(y -> historyTable.length);
                    } else {
                        return CompletableFuture.completedFuture(historyTable.length);
                    }
                });
    }

    private CompletableFuture<Integer> updateSegmentChunk(
            final Scale scale,
            final ImmutablePair<Integer, byte[]> currentSegmentData) {
        final int currentChunk = currentSegmentData.left;
        final byte[] currentChunkData = currentSegmentData.right;

        final int startingSegmentNumber = currentChunk * SegmentRecord.SEGMENT_CHUNK_SIZE +
                (currentChunkData.length / SegmentRecord.SEGMENT_RECORD_SIZE);

        final int maxSegmentNumberForChunk = (currentChunk + 1) * SegmentRecord.SEGMENT_CHUNK_SIZE;

        final int toCreate = Integer.min(maxSegmentNumberForChunk - startingSegmentNumber,
                scale.getNewRanges().size());

        final byte[] updatedChunkData = TableHelper.updateSegmentTable(startingSegmentNumber,
                currentChunkData,
                toCreate,
                scale.getNewRanges(),
                scale.getScaleTimestamp()
        );

        return setSegmentTableChunk(currentChunk, updatedChunkData)
                .thenCompose(y -> {
                    final int chunkNumber = (startingSegmentNumber + scale.getNewRanges().size()) / SegmentRecord.SEGMENT_RECORD_SIZE;
                    final int remaining = Integer.max(scale.getNewRanges().size() - toCreate, 0);

                    byte[] newChunk = TableHelper.updateSegmentTable(chunkNumber * SegmentRecord.SEGMENT_CHUNK_SIZE,
                            new byte[0],
                            remaining,
                            scale.getNewRanges(),
                            scale.getScaleTimestamp()
                    );

                    return createSegmentChunk(chunkNumber, newChunk);
                })
                .thenApply(x -> startingSegmentNumber);
    }

    private CompletionStage<ImmutablePair<Integer, byte[]>> getLatestChunk(final List<String> segmentChunks) {
        assert segmentChunks.size() > 0;

        final int latestChunk = segmentChunks.size() - 1;

        return getSegmentTableChunk(latestChunk)
                .thenApply(segmentTableChunk ->
                        new ImmutablePair<>(latestChunk, segmentTableChunk));
    }

    private CompletableFuture<Create> processCreateTask(final StreamConfiguration configuration, final byte[] taskData) {
        final Create create;
        if (taskData != null) {
            try {
                Task task = (Task) SerializationUtils.deserialize(taskData);
                // if the task is anything other than create, throw error and exit immediately
                assert task.getType().equals(Create.class);

                create = task.asCreate();
                // assert that configuration is same, else fail create attempt
                assert create.getConfiguration().equals(configuration);
                return CompletableFuture.completedFuture(create);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                // ensure that a previous create did not complete, if it did, we should throw an exception
                // TODO: throw appropriate exception to let the caller know that previous attempt to create
                // completed successfully. This is not a bad error and is recoverable.
                create = new Create(System.currentTimeMillis(), configuration);
                return createTask(SerializationUtils.serialize(create)).thenApply(x -> create);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private CompletableFuture<Scale> processScaleTask(final List<Integer> sealedSegments,
                                                      final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                      final long scaleTimestamp,
                                                      final byte[] taskData) {
        final Scale scale;
        if (taskData != null) {
            try {
                final Task task = (Task) SerializationUtils.deserialize(taskData);
                // if the task is anything other than scale, throw error and exit immediately
                assert task.getType().equals(Scale.class);

                scale = task.asScale();
                // If existing scale task is different from requested scale task, we will complete the partially
                // done scale task from the store and ignore the current task. We need to let the caller know
                // in case we ignore so that there are no surprises.
                assert scale.getSealedSegments().equals(sealedSegments) &&
                        scale.getNewRanges().equals(newRanges);
                return CompletableFuture.completedFuture(scale);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                scale = new Scale(sealedSegments, newRanges, scaleTimestamp);
                return createTask(SerializationUtils.serialize(scale)).thenApply(x -> scale);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    abstract CompletableFuture<Void> acquireDistributedLock();

    abstract CompletableFuture<Void> releaseDistributedLock();

    abstract CompletableFuture<Void> createTask(byte[] serialize);

    abstract CompletableFuture<byte[]> getTaskDataIfExists();

    abstract CompletableFuture<Void> deleteTask();

    abstract CompletionStage<Create> createConfiguration(StreamConfiguration configuration, Create create);

    abstract CompletableFuture<Void> setConfigurationData(StreamConfiguration configuration);

    abstract CompletableFuture<StreamConfiguration> getConfigurationData();

    abstract CompletableFuture<Create> createSegmentTable(Create create);

    abstract CompletableFuture<Void> createSegmentChunk(int chunkNumber, byte[] data);

    abstract CompletableFuture<List<String>> getSegmentChunks();

    abstract CompletableFuture<Segment> getSegmentRow(int number);

    abstract CompletableFuture<byte[]> getSegmentTableChunk(int chunkNumber);

    abstract CompletableFuture<Void> setSegmentTableChunk(int chunkNumber, byte[] data);

    abstract CompletionStage<String> createIndexTable(Create create);

    abstract CompletableFuture<byte[]> getIndexTable();

    abstract CompletableFuture<Void> updateIndexTable(byte[] updated);

    abstract CompletionStage<Create> createHistoryTable(Create create);

    abstract CompletableFuture<Void> updateHistoryTable(byte[] updated);

    abstract CompletableFuture<byte[]> getHistoryTable();

    abstract CompletionStage<Create> createSegmentFile(Create create);
}
