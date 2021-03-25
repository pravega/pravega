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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.kvtable.records.KVTEpochRecord;
import io.pravega.controller.store.kvtable.records.KVTConfigurationRecord;
import io.pravega.controller.store.kvtable.records.KVTStateRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * Pravega KeyValueTable.
 * This creates a top level table per kvTable - metadataTable.
 * All metadata records are stored in metadata table.
 *
 * Each kvTable is protected against recreation of another kvTable/stream with same name by attaching a UUID to the name.
 */
@Slf4j
class PravegaTablesKVTable extends AbstractKVTableBase {
    public static final String PATH_SEPARATOR = ".#.";
    private static final String METADATA_TABLE = "metadata" + PATH_SEPARATOR + "%s";
    // metadata keys
    private static final String CREATION_TIME_KEY = "creationTime";
    private static final String CONFIGURATION_KEY = "configuration";
    private static final String STATE_KEY = "state";
    private static final String CURRENT_EPOCH_KEY = "currentEpochRecord";
    private static final String EPOCH_RECORD_KEY_FORMAT = "epochRecord-%d";

    private final PravegaTablesStoreHelper storeHelper;
    private final Supplier<CompletableFuture<String>> metadataTableName;
    private final AtomicReference<String> idRef;
    private final ScheduledExecutorService executor;

    @VisibleForTesting
    PravegaTablesKVTable(final String scopeName, final String kvtName, PravegaTablesStoreHelper storeHelper,
                         Supplier<CompletableFuture<String>> tableName,
                         ScheduledExecutorService executor) {
        super(scopeName, kvtName);
        this.storeHelper = storeHelper;
        this.metadataTableName = tableName;
        this.idRef = new AtomicReference<>(null);
        this.executor = executor;
    }

    public CompletableFuture<String> getId() {
        String id = idRef.get();

        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            return metadataTableName.get()
                    .thenCompose(kvtsInScopeTable -> storeHelper.getEntry(kvtsInScopeTable, getName(),
                                                          x -> BitConverter.readUUID(x, 0)))
                                                  .thenComposeAsync(data -> {
                                                      idRef.compareAndSet(null, data.getObject().toString());
                                                      return getId();
                                                  });
        }
    }

    @Override
    public void refresh() {
        String id = idRef.getAndSet(null);
        if (!Strings.isNullOrEmpty(id)) {
            // refresh all mutable records
            storeHelper.invalidateCache(getMetadataTableName(id), STATE_KEY);
            storeHelper.invalidateCache(getMetadataTableName(id), CONFIGURATION_KEY);
        }
    }

    @Override
    public CompletableFuture<Long> getCreationTime() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedData(metadataTable, CREATION_TIME_KEY,
                        data -> BitConverter.readLong(data, 0))).thenApply(VersionedMetadata::getObject);
    }

    private CompletableFuture<String> getMetadataTable() {
        return getId().thenApply(this::getMetadataTableName);
    }

    private String getMetadataTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScopeName(), getName(), String.format(METADATA_TABLE, id));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final KVTStateRecord state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, STATE_KEY, state.toBytes())));
    }

    @Override
    CompletableFuture<Version> setStateData(final VersionedMetadata<KVTStateRecord> state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, STATE_KEY,
                        state.getObject().toBytes(), state.getVersion())
                        .thenApply(r -> {
                            storeHelper.invalidateCache(metadataTable, STATE_KEY);
                            return r;
                        }));
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTStateRecord>> getStateData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        return storeHelper.getEntry(metadataTable, STATE_KEY, KVTStateRecord::fromBytes);
                    }
                    return storeHelper.getCachedData(metadataTable, STATE_KEY, KVTStateRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTConfigurationRecord>> getConfigurationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        return storeHelper.getEntry(metadataTable, CONFIGURATION_KEY, KVTConfigurationRecord::fromBytes);
                    }
                    return storeHelper.getCachedData(metadataTable, CONFIGURATION_KEY, KVTConfigurationRecord::fromBytes);
                });
    }

    @Override
    public CompletableFuture<CreateKVTableResponse> checkKeyValueTableExists(final KeyValueTableConfiguration configuration,
                                                                             final long creationTime,
                                                                             final int startingSegmentNumber) {
        // If kvtable exists, but is in a partially complete state, then fetch its creation time and configuration and any
        // metadata that is available from a previous run.
        // If the existing kvtable has already been created successfully earlier,
        return storeHelper.expectingDataNotFound(getCreationTime(), null)
                .thenCompose(storedCreationTime -> {
                    if (storedCreationTime == null) {
                        return CompletableFuture.completedFuture(new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.NEW,
                                configuration, creationTime, startingSegmentNumber));
                    } else {
                        return storeHelper.expectingDataNotFound(getConfiguration(), null)
                                .thenCompose(config -> {
                                    if (config != null) {
                                        return handleConfigExists(storedCreationTime, config, startingSegmentNumber,
                                                storedCreationTime == creationTime);
                                    } else {
                                        return CompletableFuture.completedFuture(
                                                new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.NEW,
                                                        configuration, storedCreationTime, startingSegmentNumber));
                                    }
                                });
                    }
                });
    }

    private CompletableFuture<CreateKVTableResponse> handleConfigExists(long creationTime, KeyValueTableConfiguration config,
                                                                       int startingSegmentNumber, boolean creationTimeMatched) {
        CreateKVTableResponse.CreateStatus status = creationTimeMatched ?
                CreateKVTableResponse.CreateStatus.NEW : CreateKVTableResponse.CreateStatus.EXISTS_CREATING;
        return storeHelper.expectingDataNotFound(getState(true), null)
                .thenApply(state -> {
                    if (state == null) {
                        return new CreateKVTableResponse(status, config, creationTime, startingSegmentNumber);
                    } else if (state.equals(KVTableState.UNKNOWN) || state.equals(KVTableState.CREATING)) {
                        return new CreateKVTableResponse(status, config, creationTime, startingSegmentNumber);
                    } else {
                        return new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.EXISTS_ACTIVE,
                                config, creationTime, startingSegmentNumber);
                    }
                });
    }

    @Override
    CompletableFuture<Void> createKVTableMetadata() {
        return getId().thenCompose(id -> storeHelper.createTable(getMetadataTableName(id)));
    }

    @Override
    public CompletableFuture<Void> delete() {
        return getId().thenCompose(id -> storeHelper.deleteTable(getMetadataTableName(id), false));
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
    public CompletableFuture<Void> createConfigurationIfAbsent(final KVTConfigurationRecord configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CONFIGURATION_KEY, configuration.toBytes())
                        .thenAccept(v -> storeHelper.invalidateCache(metadataTable, CONFIGURATION_KEY)));
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, KVTEpochRecord data) {
        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, key, data.toBytes())
                        .thenAccept(v -> storeHelper.invalidateCache(metadataTable, key)));
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(KVTEpochRecord data) {
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, data.getEpoch());
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CURRENT_EPOCH_KEY, epochData)
                       .thenAccept(v -> {
                           storeHelper.invalidateCache(metadataTable, CURRENT_EPOCH_KEY);
                       }));
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTEpochRecord>> getCurrentEpochRecordData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    CompletableFuture<VersionedMetadata<Integer>> future;
                    if (ignoreCached) {
                        future = storeHelper.getEntry(metadataTable, CURRENT_EPOCH_KEY, x -> BitConverter.readInt(x, 0));
                    } else {
                        future = storeHelper.getCachedData(metadataTable, CURRENT_EPOCH_KEY, x -> BitConverter.readInt(x, 0));
                    }
                    return future.thenCompose(versionedEpochNumber -> getEpochRecord(versionedEpochNumber.getObject())
                            .thenApply(epochRecord -> new VersionedMetadata<>(epochRecord, versionedEpochNumber.getVersion())));
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTEpochRecord>> getEpochRecordData(int epoch) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
                    return storeHelper.getCachedData(metadataTable, key, KVTEpochRecord::fromBytes);
                });
    }

}
