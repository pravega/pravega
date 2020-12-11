/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.StreamCut;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.RGStreamCutRecord;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;
import io.pravega.controller.store.stream.records.ReaderGroupStateRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * PravegaTables ReaderGroup.
 * This creates a top level metadata table for each readergroup.
 * All metadata records are stored in this metadata table.
 *
 * Each Reader Group is protected against recreation of another Reader Group with same name by attaching a UUID to the name.
 */
@Slf4j
class PravegaTablesReaderGroup extends AbstractReaderGroup {
    public static final String SEPARATOR = ".#.";
    private static final String READER_GROUPS_TABLE_IDENTIFIER = "_readergroups";
    private static final String METADATA_TABLE = "metadata" + SEPARATOR + "%s";
    // metadata keys
    private static final String CREATION_TIME_KEY = "creationTime";
    private static final String CONFIGURATION_KEY = "configuration";
    private static final String STATE_KEY = "state";

    private final PravegaTablesStoreHelper storeHelper;
    private final Supplier<CompletableFuture<String>> readerGroupsInScopeTableNameSupplier;
    private final AtomicReference<String> idRef;
    private final ScheduledExecutorService executor;

    @VisibleForTesting
    PravegaTablesReaderGroup(final String scopeName, final String rgName, PravegaTablesStoreHelper storeHelper,
                        Supplier<CompletableFuture<String>> rgInScopeTableNameSupplier,
                        ScheduledExecutorService executor) {
        super(scopeName, rgName);
        this.storeHelper = storeHelper;
        this.readerGroupsInScopeTableNameSupplier = rgInScopeTableNameSupplier;
        this.idRef = new AtomicReference<>(null);
        this.executor = executor;
    }

    private CompletableFuture<String> getId() {
        String id = idRef.get();
        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            return readerGroupsInScopeTableNameSupplier.get()
                    .thenCompose(streamsInScopeTable ->
                            storeHelper.getEntry(streamsInScopeTable, getName(),
                                    x -> BitConverter.readUUID(x, 0)))
                    .thenComposeAsync(data -> {
                        idRef.compareAndSet(null, data.getObject().toString());
                        return getId();
                    });
        }
    }

    private CompletableFuture<String> getMetadataTable() {
        return getId().thenApply(this::getMetadataTableName);
    }

    private String getMetadataTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), READER_GROUPS_TABLE_IDENTIFIER, getName(), String.format(METADATA_TABLE, id));
    }

    @Override
    CompletableFuture<Void> createMetadataTables() {
        return getId().thenCompose(id -> {
            String metadataTable = getMetadataTableName(id);
            return storeHelper.createTable(metadataTable)
                    .thenAccept(v -> log.debug("reader group {}/{} metadata table {} created", getScope(), getName(), metadataTable));
        });
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, creationTime);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CREATION_TIME_KEY, b)
                        .thenAccept(v -> storeHelper.invalidateCache(metadataTable, CREATION_TIME_KEY)));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final ReaderGroupConfig configuration) {
        Map<String, RGStreamCutRecord> startStreamCuts = configuration.getStartingStreamCuts().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getScopedName(),
                        e -> new RGStreamCutRecord(getStreamCutMap(e.getValue()))));

        Map<String, RGStreamCutRecord> endStreamCuts = configuration.getEndingStreamCuts().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getScopedName(),
                        e -> new RGStreamCutRecord(getStreamCutMap(e.getValue()))));

        ReaderGroupConfigRecord configRecord = ReaderGroupConfigRecord.builder()
                                        .groupRefreshTimeMillis(configuration.getGroupRefreshTimeMillis())
                                        .automaticCheckpointIntervalMillis(configuration.getAutomaticCheckpointIntervalMillis())
                                        .maxOutstandingCheckpointRequest(configuration.getMaxOutstandingCheckpointRequest())
                                        .retentionTypeOrdinal(configuration.getRetentionType().ordinal())
                                        .generation(0L)
                                        .startingStreamCuts(startStreamCuts)
                                        .endingStreamCuts(endStreamCuts)
                                        .build();

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CONFIGURATION_KEY, configRecord.toBytes())
                        .thenAccept(v -> storeHelper.invalidateCache(metadataTable, CONFIGURATION_KEY)));
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent() {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, STATE_KEY,
                        ReaderGroupStateRecord.builder().state(ReaderGroupState.CREATING).build().toBytes())));

    }

    @Override
    CompletableFuture<Version> setStateData(final VersionedMetadata<ReaderGroupStateRecord> state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, STATE_KEY,
                        state.getObject().toBytes(), state.getVersion())
                        .thenApply(r -> {
                            storeHelper.invalidateCache(metadataTable, STATE_KEY);
                            return r;
                        }));
    }

    @Override
    CompletableFuture<VersionedMetadata<ReaderGroupStateRecord>> getStateData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        return storeHelper.getEntry(metadataTable, STATE_KEY, ReaderGroupStateRecord::fromBytes);
                    }
                    return storeHelper.getCachedData(metadataTable, STATE_KEY, ReaderGroupStateRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getConfigurationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        return storeHelper.getEntry(metadataTable, CONFIGURATION_KEY, ReaderGroupConfigRecord::fromBytes);
                    }
                    return storeHelper.getCachedData(metadataTable, CONFIGURATION_KEY, ReaderGroupConfigRecord::fromBytes);
                });
    }

    @Override
    public CompletableFuture<Void> delete() {
        return getId().thenCompose(id -> Futures.toVoid(storeHelper.deleteTable(getMetadataTableName(id), false)));
    }

    @Override
    public void refresh() {
    }

    private ImmutableMap<Long, Long> getStreamCutMap(StreamCut streamCut) {
        ImmutableMap.Builder<Long, Long> mapBuilder = ImmutableMap.builder();
        return mapBuilder.putAll(streamCut.asImpl().getPositions().entrySet()
                .stream().collect(Collectors.toMap(x -> x.getKey().getSegmentId(), Map.Entry::getValue))).build();
    }
}
