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
import io.pravega.controller.store.stream.StoreException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;

@Slf4j
public abstract class AbstractKVTableBase implements KeyValueTable {
    protected final String scopeName;
    protected final String name;

    AbstractKVTableBase(final String scope, final String name) {
        this.scopeName = scope;
        this.name = name;
    }

    @Override
    public String getScopeName() {
        return this.scopeName;
    }

    @Override
    public String getName() {
        return this.name;
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
                                                .scope(scopeName).kvtName(name).kvtConfiguration(configuration).build()))
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

    @Override
    public CompletableFuture<List<KVTSegmentRecord>> getActiveSegments() {
        // read current epoch record
        return verifyLegalState()
                .thenCompose(v -> getActiveEpochRecord(true).thenApply(epochRecord -> epochRecord.getSegments()));
    }

    @Override
    public CompletableFuture<KVTEpochRecord> getActiveEpochRecord(boolean ignoreCached) {
        return getCurrentEpochRecordData(ignoreCached).thenApply(VersionedMetadata::getObject);
    }

    private CompletableFuture<Void> verifyLegalState() {
        return getState(false).thenApply(state -> {
            if (state == null || state.equals(KVTableState.UNKNOWN) || state.equals(KVTableState.CREATING)) {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "KeyValueTable: " + getName() + " State: " + state.name());
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<KVTEpochRecord> getEpochRecord(int epoch) {
        log.info("getEpochRecord():: epoch number = {}", epoch);
        return getEpochRecordData(epoch).thenApply(VersionedMetadata::getObject);
    }

    /**
     * Fetch configuration at configurationPath.
     *
     * @return Future of kvtable configuration
     */
    @Override
    public CompletableFuture<KeyValueTableConfiguration> getConfiguration() {
        return getConfigurationData(false).thenApply(x -> x.getObject().getKvtConfiguration());
    }

    @Override
    public CompletableFuture<Set<Long>> getAllSegmentIds() {
        return getActiveEpochRecord(true)
                .thenApply(epochRecord -> epochRecord.getSegmentIds());
    }

    // region state
    abstract public CompletableFuture<String> getId();

    abstract CompletableFuture<Void> createStateIfAbsent(final KVTStateRecord state);

    abstract CompletableFuture<Version> setStateData(final VersionedMetadata<KVTStateRecord> state);

    abstract CompletableFuture<VersionedMetadata<KVTStateRecord>> getStateData(boolean ignoreCached);

    abstract CompletableFuture<CreateKVTableResponse> checkKeyValueTableExists(final KeyValueTableConfiguration configuration,
                                                                               final long creationTime,
                                                                               final int startingSegmentNumber);

    abstract CompletableFuture<Void> createKVTableMetadata();

    abstract CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime);

    abstract CompletableFuture<VersionedMetadata<KVTConfigurationRecord>> getConfigurationData(boolean ignoreCached);

    abstract CompletableFuture<Void> createConfigurationIfAbsent(final KVTConfigurationRecord data);

    abstract CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(KVTEpochRecord data);

    abstract CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, KVTEpochRecord data);

    abstract CompletableFuture<VersionedMetadata<KVTEpochRecord>> getCurrentEpochRecordData(boolean ignoreCached);

    abstract CompletableFuture<VersionedMetadata<KVTEpochRecord>> getEpochRecordData(int epoch);
}
