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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class PersistentStreamBase implements Stream {
    private final String name;

    protected PersistentStreamBase(final String name) {
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
     * @return : future of whether it was done or not
     */
    @Override
    public CompletableFuture<Boolean> create(final StreamConfiguration configuration) {
        return acquireDistributedLock()
                .thenCompose(x -> getTaskDataIfExists())
                .thenCompose(taskData -> processCreateTask(configuration, taskData))
                        // Let create object fall through to all future callbacks as their own return values are uninteresting
                .thenCompose(create -> createConfiguration(configuration, create).thenApply(x -> create))
                .thenCompose(create -> createSegmentTable(create).thenApply(x -> create))
                .thenCompose(create -> createSegmentFile(create).thenApply(x -> create))
                .thenCompose(create -> createHistoryTable(create).thenApply(x -> create))
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
     * @return : future of boolean
     */
    @Override
    public CompletableFuture<Boolean> updateConfiguration(final StreamConfiguration configuration) {
        // replace the configurationPath zknode with new configurationPath
        return setConfigurationData(configuration).thenApply(x -> true);
    }

    /**
     * Fetch configuration from zk at configurationPath
     *
     * @return : future of stream configuration
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
     * @return : future of segment
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
     * @return : future of list of successor segment numbers
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
                        .thenApply(successorCandidates -> new ImmutablePair<>(segment, successorCandidates));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> TableHelper.getOverlaps(x.getKey(), x.getValue()));
    }

    /**
     * Find predecessor candidates and find overlaps with given segment's key range
     *
     * @param number segment number.
     * @return : future of list of predecessor segment numbers
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
                        .thenApply(predecessorCandidates -> new ImmutablePair<>(segment, predecessorCandidates));
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
     * @return : list of active segment numbers at given time stamp
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
     * @return : list of newly created segments
     */
    @Override
    public CompletableFuture<List<Segment>> scale(final List<Integer> sealedSegments,
                                                  final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                  final long scaleTimestamp) {
        return acquireDistributedLock()
                .thenCompose(x -> getTaskDataIfExists())
                .thenCompose(taskData -> processScaleTask(sealedSegments, newRanges, scaleTimestamp, taskData))
                .thenCompose(scale -> getSegmentChunks()
                        .thenCompose(this::getLatestChunk)
                        .thenCompose(latestSegmentData -> addNewSegments(scale, latestSegmentData))
                        .thenApply(startingSegmentNumber -> new ImmutablePair<>(scale, startingSegmentNumber)))
                .thenCompose(pair -> {
                    final Scale scale = pair.left;
                    final int startingSegmentNumber = pair.right;

                    return addHistoryRecord(sealedSegments, scale, startingSegmentNumber)
                            .thenApply(historyOffset -> new ImmutableTriple<>(scale, startingSegmentNumber, historyOffset));
                })
                .thenCompose(triple -> {
                    final Scale scale = triple.left;
                    final int historyOffset = triple.right;
                    final int startingSegmentNumber = triple.middle;

                    return addIndexRecord(scale, historyOffset).thenApply(x -> startingSegmentNumber);
                })
                .thenCompose(startingSegmentNumber -> deleteTask().thenApply(x -> startingSegmentNumber))
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

    /**
     * @param scale
     * @param currentSegmentData
     * @return : return starting segment number
     */
    private CompletableFuture<Integer> addNewSegments(
            final Scale scale,
            final ImmutablePair<Integer, byte[]> currentSegmentData) {
        final int currentChunk = currentSegmentData.left;
        final byte[] currentChunkData = currentSegmentData.right;

        final int startingSegmentNumber = currentChunk * SegmentRecord.SEGMENT_CHUNK_SIZE +
                (currentChunkData.length / SegmentRecord.SEGMENT_RECORD_SIZE);

        // idempotent check
        final Segment lastSegment = TableHelper.getSegment(startingSegmentNumber - 1, currentChunkData);
        if (lastSegment.getStart() == scale.getScaleTimestamp()) {
            return CompletableFuture.completedFuture(lastSegment.getNumber() - scale.getNewRanges().size() + 1);
        }

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
                    final int chunkNumber = TableHelper.getSegmentChunkNumber(startingSegmentNumber + scale.getNewRanges().size());
                    final int remaining = Integer.max(scale.getNewRanges().size() - toCreate, 0);

                    byte[] newChunk = TableHelper.updateSegmentTable(chunkNumber * SegmentRecord.SEGMENT_CHUNK_SIZE,
                            new byte[0], // new chunk
                            remaining,
                            scale.getNewRanges(),
                            scale.getScaleTimestamp()
                    );

                    return createSegmentChunk(chunkNumber, newChunk);
                })
                .thenApply(x -> startingSegmentNumber);
    }

    /**
     * update history table if not already updated:
     * fetch last record from history table.
     * if eventTime is >= scale.scaleTimeStamp do nothing, else create record
     *
     * @param sealedSegments
     * @param scale
     * @return : future of history table offset for last entry
     */
    private CompletableFuture<Integer> addHistoryRecord(final List<Integer> sealedSegments,
                                                        final Scale scale,
                                                        final int startingSegmentNumber) {
        return getHistoryTable()
                .thenCompose(historyTable -> {
                    final Optional<HistoryRecord> lastRecordOpt = HistoryRecord.readLatestRecord(historyTable);

                    // scale task is not allowed unless create is done which means at least one
                    // record in history table
                    assert lastRecordOpt.isPresent();

                    final HistoryRecord lastRecord = lastRecordOpt.get();

                    // idempotent check
                    if (lastRecord.getEventTime() >= scale.getScaleTimestamp()) {
                        assert lastRecord.getSegments().contains(startingSegmentNumber);

                        return CompletableFuture.completedFuture(lastRecord.getStartOfRowPointer());
                    }

                    final List<Integer> newActiveSegments = getNewActiveSegments(sealedSegments,
                            scale,
                            startingSegmentNumber,
                            lastRecord);

                    final byte[] updated = TableHelper.updateHistoryTable(historyTable,
                            scale.getScaleTimestamp(),
                            newActiveSegments);

                    return updateHistoryTable(updated).thenApply(y -> lastRecord.getEndOfRowPointer() + 1);
                });
    }

    private List<Integer> getNewActiveSegments(List<Integer> sealedSegments,
                                               Scale scale,
                                               int startingSegmentNumber,
                                               HistoryRecord lastRecord) {
        final List<Integer> segments = lastRecord.getSegments();
        segments.removeAll(sealedSegments);
        segments.addAll(
                IntStream.range(startingSegmentNumber,
                        startingSegmentNumber + scale.getNewRanges().size())
                        .boxed()
                        .collect(Collectors.toList()));
        return segments;
    }

    private CompletionStage<Void> addIndexRecord(final Scale scale,
                                                 final int historyOffset) {
        return getIndexTable()
                .thenCompose(indexTable -> {
                    final Optional<IndexRecord> lastRecord = IndexRecord.readLatestRecord(indexTable);
                    // check idempotent
                    if (lastRecord.isPresent() && lastRecord.get().getEventTime() >= scale.getScaleTimestamp())
                        return CompletableFuture.completedFuture(null);

                    final byte[] updated = TableHelper.updateIndexTable(indexTable,
                            scale.getScaleTimestamp(),
                            historyOffset);
                    return updateIndexTable(updated);
                });
    }

    private CompletableFuture<Create> processCreateTask(final StreamConfiguration configuration, final byte[] taskData) {
        final Create create;
        if (taskData != null) {
            try {
                final Task task = (Task) SerializationUtils.deserialize(taskData);
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

    private CompletionStage<ImmutablePair<Integer, byte[]>> getLatestChunk(final List<String> segmentChunks) {
        assert segmentChunks.size() > 0;

        final int latestChunkNumber = segmentChunks.size() - 1;

        return getSegmentTableChunk(latestChunkNumber)
                .thenApply(segmentTableChunk -> new ImmutablePair<>(latestChunkNumber, segmentTableChunk));
    }

    abstract CompletableFuture<Void> acquireDistributedLock();

    abstract CompletableFuture<Void> releaseDistributedLock();

    abstract CompletableFuture<Void> createTask(byte[] serialize);

    abstract CompletableFuture<byte[]> getTaskDataIfExists();

    abstract CompletableFuture<Void> deleteTask();

    abstract CompletionStage<Void> createConfiguration(StreamConfiguration configuration, Create create);

    abstract CompletableFuture<Void> setConfigurationData(StreamConfiguration configuration);

    abstract CompletableFuture<StreamConfiguration> getConfigurationData();

    abstract CompletableFuture<Void> createSegmentTable(Create create);

    abstract CompletableFuture<Void> createSegmentChunk(int chunkNumber, byte[] data);

    abstract CompletableFuture<List<String>> getSegmentChunks();

    abstract CompletableFuture<Segment> getSegmentRow(int number);

    abstract CompletableFuture<byte[]> getSegmentTableChunk(int chunkNumber);

    abstract CompletableFuture<Void> setSegmentTableChunk(int chunkNumber, byte[] data);

    abstract CompletionStage<String> createIndexTable(Create create);

    abstract CompletableFuture<byte[]> getIndexTable();

    abstract CompletableFuture<Void> updateIndexTable(byte[] updated);

    abstract CompletionStage<Void> createHistoryTable(Create create);

    abstract CompletableFuture<Void> updateHistoryTable(byte[] updated);

    abstract CompletableFuture<byte[]> getHistoryTable();

    abstract CompletionStage<Void> createSegmentFile(Create create);
}
