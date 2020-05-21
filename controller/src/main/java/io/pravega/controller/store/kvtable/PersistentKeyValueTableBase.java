/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable;

import com.google.common.collect.ImmutableList;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.kvtable.records.KVTEpochRecord;
import io.pravega.controller.store.kvtable.records.KVTSegmentRecord;
import io.pravega.controller.store.kvtable.records.KVTConfigurationRecord;
import io.pravega.controller.store.kvtable.records.KVTStateRecord;
import io.pravega.controller.store.stream.*;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.groupingBy;

@Slf4j
public abstract class PersistentKeyValueTableBase implements KeyValueTable {
    private final String scope;
    private final String name;

    PersistentKeyValueTableBase(final String scope, final String name) {
        this.scope = scope;
        this.name = name;
    }

    @Override
    public String getScope() {
        return this.scope;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getScopeName() {
        return this.scope;
    }

    @Override
    public CompletableFuture<Void> updateState(final KVTableState state) {
        return getStateData(true)
                .thenCompose(currState -> {
                    VersionedMetadata<KVTableState> currentState = new VersionedMetadata<KVTableState>(currState.getObject().getState(), currState.getVersion());
                    return Futures.toVoid(updateVersionedState(currentState, state));
                });
    }

    @Override
    public CompletableFuture<VersionedMetadata<KVTableState>> getVersionedState() {
        return getStateData(true)
                .thenApply(x -> new VersionedMetadata<>(x.getObject().getState(), x.getVersion()));
    }

    @Override
    public CompletableFuture<VersionedMetadata<KVTableState>> updateVersionedState(final VersionedMetadata<KVTableState> previous, final KVTableState newState) {
        if (KVTableState.isTransitionAllowed(previous.getObject(), newState)) {
            return setStateData(new VersionedMetadata<>(KVTStateRecord.builder().state(newState).build(), previous.getVersion()))
                    .thenApply(updatedVersion -> new VersionedMetadata<>(newState, updatedVersion));
        } else {
            return Futures.failedFuture(StoreException.create(
                    StoreException.Type.OPERATION_NOT_ALLOWED,
                    "KeyValueTable: " + getName() + " State: " + newState.name() + " current state = " +
                            previous.getObject()));
        }
    }

    @Override
    public CompletableFuture<KVTableState> getState(boolean ignoreCached) {
        return getStateData(ignoreCached)
                .thenApply(x -> x.getObject().getState());
    }

    @Override
    public CompletableFuture<CreateKVTableResponse> create(final KeyValueTableConfiguration configuration, long createTimestamp, int startingSegmentNumber) {

        return checkKeyValueTableExists(configuration, createTimestamp, startingSegmentNumber)
                .thenCompose(createKVTResponse -> createKVTableMetadata()
                        .thenCompose((Void v) -> storeCreationTimeIfAbsent(createKVTResponse.getTimestamp()))
                        .thenCompose((Void v) -> createConfigurationIfAbsent(KVTConfigurationRecord.builder()
                                .scope(scope).kvtName(name).kvtConfiguration(configuration).build()))
                        .thenCompose((Void v) -> createStateIfAbsent(KVTStateRecord.builder().state(KVTableState.CREATING).build()))
                        .thenCompose((Void v) -> createHistoryRecords(startingSegmentNumber, createKVTResponse))
                        .thenApply((Void v) -> createKVTResponse));


    }

    private CompletionStage<Void> createHistoryRecords(int startingSegmentNumber, CreateKVTableResponse createKvtResponse) {
        final int numSegments = createKvtResponse.getConfiguration().getPartitionCount();
        // create epoch 0 record
        final double keyRangeChunk = 1.0 / numSegments;

        long creationTime = createKvtResponse.getTimestamp();
        final ImmutableList.Builder<KVTSegmentRecord> builder = ImmutableList.builder();

        IntStream.range(0, numSegments).boxed()
                .forEach(x -> builder.add(newSegmentRecord(0, startingSegmentNumber + x, creationTime,
                        x * keyRangeChunk, (x + 1) * keyRangeChunk)));

        KVTEpochRecord epoch0 = new KVTEpochRecord(0, builder.build(), creationTime);

        return createEpochRecord(epoch0).thenCompose(r -> createCurrentEpochRecordDataIfAbsent(epoch0));

    }

    private KVTSegmentRecord newSegmentRecord(int epoch, int segmentNumber, long time, Double low, Double high) {
        return KVTSegmentRecord.builder().creationEpoch(epoch).segmentNumber(segmentNumber).creationTime(time)
                .keyStart(low).keyEnd(high).build();
    }

    private CompletableFuture<Void> createEpochRecord(KVTEpochRecord epoch) {
        return createEpochRecordDataIfAbsent(epoch.getEpoch(), epoch);
    }
    /**
     * Fetch configuration at configurationPath.
     *
     * @return Future of stream configuration
     */

    @Override
    public CompletableFuture<KeyValueTableConfiguration> getConfiguration() {
        return getConfigurationData(false).thenApply(x -> x.getObject().getKvtConfiguration());
    }

    @Override
    public CompletableFuture<VersionedMetadata<KVTConfigurationRecord>> getVersionedConfigurationRecord() {
        return getConfigurationData(true)
                .thenApply(data -> new VersionedMetadata<>(data.getObject(), data.getVersion()));
    }

    // region state
    abstract CompletableFuture<Void> createStateIfAbsent(final KVTStateRecord state);

    abstract CompletableFuture<Version> setStateData(final VersionedMetadata<KVTStateRecord> state);

    abstract CompletableFuture<VersionedMetadata<KVTStateRecord>> getStateData(boolean ignoreCached);
    abstract CompletableFuture<CreateKVTableResponse> checkKeyValueTableExists(final KeyValueTableConfiguration configuration,
                                                                               final long creationTime,
                                                                               final int startingSegmentNumber);

    abstract CompletableFuture<Void> createKVTableMetadata();

    abstract CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime);
    abstract CompletableFuture<Version> setConfigurationData(final VersionedMetadata<KVTConfigurationRecord> configuration);

    abstract CompletableFuture<VersionedMetadata<KVTConfigurationRecord>> getConfigurationData(boolean ignoreCached);
    abstract CompletableFuture<Void> createConfigurationIfAbsent(final KVTConfigurationRecord data);
    abstract CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, KVTEpochRecord data);
    abstract CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(KVTEpochRecord data);

    /**
     * Fetches Segment metadata from the epoch in which segment was created.
     *
     * @param segmentId segment id.
     * @return : Future, which when complete contains segment object
     */
    /*
    @Override
    public CompletableFuture<StreamSegmentRecord> getSegment(final long segmentId) {
        // extract epoch from segment id.
        // fetch epoch record for the said epoch
        // extract segment record from it.
        int epoch = NameUtils.getEpoch(segmentId);
        return getEpochRecord(epoch)
                .thenApply(epochRecord -> {
                    Optional<StreamSegmentRecord> segmentRecord = epochRecord.getSegments().stream()
                                                                             .filter(x -> x.segmentId() == segmentId).findAny();
                    return segmentRecord
                            .orElseThrow(() -> StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                                    "segment not found in epoch"));
                });
    }

    @Override
    public CompletableFuture<Set<Long>> getAllSegmentIds() {
        CompletableFuture<Map<StreamSegmentRecord, Integer>> fromSpanFuture = getTruncationRecord()
                .thenCompose(truncationRecord -> {
                    if (truncationRecord.getObject().equals(StreamTruncationRecord.EMPTY)) {
                        return getEpochRecord(0)
                                .thenApply(this::convertToSpan);
                    } else {
                        return CompletableFuture.completedFuture(truncationRecord.getObject().getSpan());
                    }
                });
        CompletableFuture<Map<StreamSegmentRecord, Integer>> toSpanFuture = getActiveEpoch(true)
                .thenApply(this::convertToSpan);

        return CompletableFuture.allOf(fromSpanFuture, toSpanFuture)
                                .thenCompose(v -> {
                                    Map<StreamSegmentRecord, Integer> fromSpan = fromSpanFuture.join();
                                    Map<StreamSegmentRecord, Integer> toSpan = toSpanFuture.join();
                                    return segmentsBetweenStreamCutSpans(fromSpan, toSpan)
                                            .thenApply(x -> x.stream().map(StreamSegmentRecord::segmentId).collect(Collectors.toSet()));
                                });
    }



    private CompletableFuture<EpochRecord> getActiveEpochRecord(boolean ignoreCached) {
        return getCurrentEpochRecordData(ignoreCached).thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<List<StreamSegmentRecord>> getActiveSegments() {
        // read current epoch record
        return verifyLegalState()
                .thenCompose(v -> getActiveEpochRecord(true).thenApply(epochRecord -> epochRecord.getSegments()));
    }

    @Override
    public CompletableFuture<List<StreamSegmentRecord>> getSegmentsInEpoch(final int epoch) {
        return getEpochRecord(epoch)
                .thenApply(epochRecord -> epochRecord.getSegments());
    }

    @SneakyThrows
    private TxnStatus handleDataNotFoundException(Throwable ex) {
        if (Exceptions.unwrap(ex) instanceof DataNotFoundException) {
            return TxnStatus.UNKNOWN;
        } else {
            throw ex;
        }
    }
    
    @Override
    public CompletableFuture<EpochRecord> getActiveEpoch(boolean ignoreCached) {
        return getCurrentEpochRecordData(ignoreCached).thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<EpochRecord> getEpochRecord(int epoch) {
        return getEpochRecordData(epoch).thenApply(VersionedMetadata::getObject);
    }



    @Override
    public CompletableFuture<Void> createWaitingRequestIfAbsent(String processorName) {
        return createWaitingRequestNodeIfAbsent(processorName);
    }

    @Override
    public CompletableFuture<String> getWaitingRequestProcessor() {
        return getWaitingRequestNode()
                .handle((data, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof DataNotFoundException) {
                            return null;
                        } else {
                            throw new CompletionException(e);
                        }
                    } else {
                        return data;
                    }
                });
    }

    @Override
    public CompletableFuture<Void> deleteWaitingRequestConditionally(String processorName) {
        return getWaitingRequestProcessor()
                .thenCompose(waitingRequest -> {
                    if (waitingRequest != null && waitingRequest.equals(processorName)) {
                        return deleteWaitingRequestNode();
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }



    private CompletableFuture<Void> verifyLegalState() {
        return getState(false).thenApply(state -> {
            if (state == null || state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " State: " + state.name());
            }
            return null;
        });
    }

    private int getShardNumber(long segmentId) {
        return NameUtils.getEpoch(segmentId) / shardSize.get();
    }

    private ImmutableMap<StreamSegmentRecord, Integer> convertToSpan(EpochRecord epochRecord) {
        ImmutableMap.Builder<StreamSegmentRecord, Integer> builder = ImmutableMap.builder();
        epochRecord.getSegments().forEach(x -> builder.put(x, epochRecord.getEpoch()));
        return builder.build();
    }

    private Segment transform(StreamSegmentRecord segmentRecord) {
        return new Segment(segmentRecord.segmentId(), segmentRecord.getCreationTime(),
                segmentRecord.getKeyStart(), segmentRecord.getKeyEnd());
    }

    private List<Segment> transform(List<StreamSegmentRecord> segmentRecords) {
        return segmentRecords.stream().map(this::transform).collect(Collectors.toList());
    }
    
    @VisibleForTesting
    CompletableFuture<List<EpochRecord>> fetchEpochs(int fromEpoch, int toEpoch, boolean ignoreCache) {
        // fetch history time series chunk corresponding to from.
        // read entries till either last entry or till to
        // if to is not in this chunk fetch the next chunk and read till to
        // keep doing this until all records till to have been read.
        // keep computing history record from history time series by applying delta on previous.
        return getActiveEpochRecord(ignoreCache)
                .thenApply(currentEpoch -> currentEpoch.getEpoch() / historyChunkSize.get())
                .thenCompose(latestChunkNumber -> Futures.allOfWithResults(
                        IntStream.range(fromEpoch / historyChunkSize.get(), toEpoch / historyChunkSize.get() + 1)
                                 .mapToObj(i -> {
                                     int firstEpoch = i * historyChunkSize.get() > fromEpoch ? i * historyChunkSize.get() : fromEpoch;

                                     boolean ignoreCached = i >= latestChunkNumber;
                                     return getEpochsFromHistoryChunk(i, firstEpoch, toEpoch, ignoreCached);
                                 }).collect(Collectors.toList())))
                .thenApply(c -> c.stream().flatMap(Collection::stream).collect(Collectors.toList()));
    }

    private CompletableFuture<List<EpochRecord>> getEpochsFromHistoryChunk(int chunk, int firstEpoch, int toEpoch, boolean ignoreCached) {
        return getEpochRecord(firstEpoch)
                .thenCompose(first -> getHistoryTimeSeriesChunk(chunk, ignoreCached)
                        .thenCompose(x -> {
                            List<CompletableFuture<EpochRecord>> identity = new ArrayList<>();
                            identity.add(CompletableFuture.completedFuture(first));
                            return Futures.allOfWithResults(x.getHistoryRecords().stream()
                                                             .filter(r -> r.getEpoch() > firstEpoch && r.getEpoch() <= toEpoch)
                                                             .reduce(identity, (r, s) -> {
                                                                 CompletableFuture<EpochRecord> next = newEpochRecord(r.get(r.size() - 1),
                                                                         s.getEpoch(), s.getReferenceEpoch(), s.getSegmentsCreated(),
                                                                         s.getSegmentsSealed().stream().map(StreamSegmentRecord::segmentId)
                                                                          .collect(Collectors.toList()), s.getScaleTime());
                                                                 ArrayList<CompletableFuture<EpochRecord>> list = new ArrayList<>(r);
                                                                 list.add(next);
                                                                 return list;
                                                             }, (r, s) -> {
                                                                 ArrayList<CompletableFuture<EpochRecord>> list = new ArrayList<>(r);
                                                                 list.addAll(s);
                                                                 return list;
                                                             }));
                        }));
    }

    private CompletableFuture<EpochRecord> newEpochRecord(final CompletableFuture<EpochRecord> lastRecordFuture,
                                                          final int epoch, final int referenceEpoch,
                                                          final Collection<StreamSegmentRecord> createdSegments,
                                                          final Collection<Long> sealedSegments, final long time) {
        if (epoch == referenceEpoch) {
            return lastRecordFuture.thenApply(lastRecord -> {
                assert lastRecord.getEpoch() == epoch - 1;
                ImmutableList.Builder<StreamSegmentRecord> segmentsBuilder = ImmutableList.builder();
                lastRecord.getSegments().forEach(segment -> {
                   if (!sealedSegments.contains(segment.segmentId())) {
                       segmentsBuilder.add(segment);
                   }
                });
                segmentsBuilder.addAll(createdSegments);
                return new EpochRecord(epoch, referenceEpoch, segmentsBuilder.build(), time);
            });
        } else {
            return getEpochRecord(epoch);
        }
    }

    private StreamSegmentRecord newSegmentRecord(long segmentId, long time, Double low, Double high) {
        return newSegmentRecord(NameUtils.getEpoch(segmentId), NameUtils.getSegmentNumber(segmentId),
                time, low, high);
    }

    private StreamSegmentRecord newSegmentRecord(int epoch, int segmentNumber, long time, Double low, Double high) {
        return StreamSegmentRecord.builder().creationEpoch(epoch).segmentNumber(segmentNumber).creationTime(time)
                                  .keyStart(low).keyEnd(high).build();
    }

    @VisibleForTesting
    CompletableFuture<Integer> findEpochAtTime(long timestamp, boolean ignoreCached) {
        return getActiveEpoch(ignoreCached)
                .thenCompose(activeEpoch -> searchEpochAtTime(0, activeEpoch.getEpoch() / historyChunkSize.get(),
                        x -> x == activeEpoch.getEpoch() / historyChunkSize.get(), timestamp)
                        .thenApply(epoch -> {
                            if (epoch == -1) {
                                if (timestamp > activeEpoch.getCreationTime()) {
                                    return activeEpoch.getEpoch();
                                } else {
                                    return 0;
                                }

                            } else {
                                return epoch;
                            }
                        }));
    }

    private CompletableFuture<Integer> searchEpochAtTime(int lowest, int highest, Predicate<Integer> ignoreCached, long timestamp) {
        final int middle = (lowest + highest) / 2;

        if (lowest > highest) {
            // either return epoch 0 or latest epoch
            return CompletableFuture.completedFuture(-1);
        }

        return getHistoryTimeSeriesChunk(middle, ignoreCached.test(middle))
                .thenCompose(chunk -> {
                    List<HistoryTimeSeriesRecord> historyRecords = chunk.getHistoryRecords();
                    long rangeLow = historyRecords.get(0).getScaleTime();
                    long rangeHigh = historyRecords.get(historyRecords.size() - 1).getScaleTime();
                    if (timestamp >= rangeLow && timestamp <= rangeHigh) {
                        // found
                        int index = CollectionHelpers.findGreatestLowerBound(historyRecords, x -> Long.compare(timestamp, x.getScaleTime()));
                        assert index >= 0;
                        return CompletableFuture.completedFuture(historyRecords.get(index).getEpoch());
                    } else if (timestamp < rangeLow) {
                        return searchEpochAtTime(lowest, middle - 1, ignoreCached, timestamp);
                    } else {
                        return searchEpochAtTime(middle + 1, highest, ignoreCached, timestamp);
                    }
                });
    }

    @Override
    public CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(int chunkNumber) {
        return getHistoryTimeSeriesChunk(chunkNumber, true);
    }
    
    private CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(int chunkNumber, boolean ignoreCached) {
        return getHistoryTimeSeriesChunkData(chunkNumber, ignoreCached)
                .thenCompose(x -> {
                    HistoryTimeSeries timeSeries = x.getObject();
                    // we should only retrieve the chunk from cache once the chunk is full to capacity and hence immutable. 
                    if (!ignoreCached && timeSeries.getHistoryRecords().size() < historyChunkSize.get()) {
                        return getHistoryTimeSeriesChunk(chunkNumber, true);
                    }
                    return CompletableFuture.completedFuture(timeSeries);
                });
    }
*/
    /*
    abstract CompletableFuture<Void> deleteKeyValueTable();
    // endregion

    abstract CompletableFuture<VersionedMetadata<EpochRecord>> getCurrentEpochRecordData(boolean ignoreCached);

    abstract CompletableFuture<VersionedMetadata<EpochRecord>> getEpochRecordData(int epoch);

    // region processor
    abstract CompletableFuture<Void> createWaitingRequestNodeIfAbsent(String data);

    abstract CompletableFuture<String> getWaitingRequestNode();

    abstract CompletableFuture<Void> deleteWaitingRequestNode();
    // endregion
    */

}
