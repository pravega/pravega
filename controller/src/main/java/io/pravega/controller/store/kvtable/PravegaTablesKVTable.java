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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.kvtable.records.KVTEpochRecord;
import io.pravega.controller.store.kvtable.records.KVTConfigurationRecord;
import io.pravega.controller.store.kvtable.records.KVTStateRecord;
import io.pravega.controller.store.stream.OperationContext;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static io.pravega.controller.store.PravegaTablesStoreHelper.*;
import static io.pravega.controller.store.PravegaTablesStoreHelper.BYTES_TO_UUID_FUNCTION;
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
    private final BiFunction<Boolean, OperationContext, CompletableFuture<String>> metadataTableNameSupplier;
    private final AtomicReference<String> idRef;

    @VisibleForTesting
    PravegaTablesKVTable(final String scopeName, final String kvtName, PravegaTablesStoreHelper storeHelper,
                         BiFunction<Boolean, OperationContext, CompletableFuture<String>> tableName,
                         ScheduledExecutorService executor) {
        super(scopeName, kvtName);
        this.storeHelper = storeHelper;
        this.metadataTableNameSupplier = tableName;
        this.idRef = new AtomicReference<>(null);
    }

    @Override
    public CompletableFuture<String> getId(OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        String id = idRef.get();

        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            // first get the scope id from the cache.
            // if the cache does not contain scope id then we load it from the supplier. 
            // if cache contains the scope id then we load the kvtid. if not found, we load the scopeid first. 
            return storeHelper.loadFromTableHandleStaleTableName(metadataTableNameSupplier, getName(),
                    BYTES_TO_UUID_FUNCTION, context)
                              .thenComposeAsync(data -> {
                                  idRef.compareAndSet(null, data.getObject().toString());
                                  return getId(context);
                              });
        }
    }

    @Override
    public void refresh() {
        idRef.set(null);
    }

    @Override
    public CompletableFuture<Long> getCreationTime(OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.getCachedOrLoad(metadataTable, CREATION_TIME_KEY,
                        BYTES_TO_LONG_FUNCTION, context.getOperationStartTime(), context.getRequestId()))
                .thenApply(VersionedMetadata::getObject);
    }

    private CompletableFuture<String> getMetadataTable(OperationContext context) {
        return getId(context).thenApply(this::getMetadataTableName);
    }

    private String getMetadataTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScopeName(), getName(), String.format(METADATA_TABLE, id));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final KVTStateRecord state, OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, STATE_KEY, 
                        state, KVTStateRecord::toBytes, context.getRequestId())));
    }

    @Override
    CompletableFuture<Version> setStateData(final VersionedMetadata<KVTStateRecord> state, OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, STATE_KEY,
                        state.getObject(), KVTStateRecord::toBytes, state.getVersion(), context.getRequestId()));
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTStateRecord>> getStateData(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(metadataTable, STATE_KEY);
                    }
                    return storeHelper.getCachedOrLoad(metadataTable, STATE_KEY, KVTStateRecord::fromBytes, 
                            ignoreCached ? context.getOperationStartTime() : 0L, context.getRequestId());
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTConfigurationRecord>> getConfigurationData(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(metadataTable, CONFIGURATION_KEY);
                    }
                    return storeHelper.getCachedOrLoad(metadataTable, CONFIGURATION_KEY, KVTConfigurationRecord::fromBytes,
                            ignoreCached ? context.getOperationStartTime() : 0L, context.getRequestId());
                });
    }

    @Override
    public CompletableFuture<CreateKVTableResponse> checkKeyValueTableExists(final KeyValueTableConfiguration configuration,
                                                                             final long creationTime,
                                                                             final int startingSegmentNumber, 
                                                                             OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        // If kvtable exists, but is in a partially complete state, then fetch its creation time and configuration and any
        // metadata that is available from a previous run.
        // If the existing kvtable has already been created successfully earlier,
        return storeHelper.expectingDataNotFound(getCreationTime(context), null)
                .thenCompose(storedCreationTime -> {
                    if (storedCreationTime == null) {
                        return CompletableFuture.completedFuture(new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.NEW,
                                configuration, creationTime, startingSegmentNumber));
                    } else {
                        return storeHelper.expectingDataNotFound(getConfiguration(context), null)
                                .thenCompose(config -> {
                                    if (config != null) {
                                        return handleConfigExists(storedCreationTime, config, startingSegmentNumber,
                                                storedCreationTime == creationTime, context);
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
                                                                       int startingSegmentNumber, boolean creationTimeMatched, 
                                                                        OperationContext context) {
        CreateKVTableResponse.CreateStatus status = creationTimeMatched ?
                CreateKVTableResponse.CreateStatus.NEW : CreateKVTableResponse.CreateStatus.EXISTS_CREATING;
        return storeHelper.expectingDataNotFound(getState(true, context), null)
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
    CompletableFuture<Void> createKVTableMetadata(OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getId(context).thenCompose(id -> storeHelper.createTable(getMetadataTableName(id), context.getRequestId()));
    }

    @Override
    public CompletableFuture<Void> delete(OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getId(context).thenCompose(id -> storeHelper.deleteTable(getMetadataTableName(id), 
                false, context.getRequestId()));
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime, OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, 
                        CREATION_TIME_KEY, creationTime, LONG_TO_BYTES_FUNCTION, context.getRequestId())));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final KVTConfigurationRecord configuration, OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, 
                        CONFIGURATION_KEY, configuration, KVTConfigurationRecord::toBytes, context.getRequestId())));
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, KVTEpochRecord data, OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, key,
                        data, KVTEpochRecord::toBytes, context.getRequestId())));
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(KVTEpochRecord data, OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(
                        metadataTable, CURRENT_EPOCH_KEY, data.getEpoch(), INTEGER_TO_BYTES_FUNCTION, context.getRequestId())));
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTEpochRecord>> getCurrentEpochRecordData(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    CompletableFuture<VersionedMetadata<Integer>> future;
                    if (ignoreCached) {
                        storeHelper.invalidateCache(metadataTable, CURRENT_EPOCH_KEY);
                    }
                    future = storeHelper.getCachedOrLoad(metadataTable, CURRENT_EPOCH_KEY, BYTES_TO_INTEGER_FUNCTION, 
                            ignoreCached ? context.getOperationStartTime() : 0L, context.getRequestId());

                    return future.thenCompose(versionedEpochNumber -> getEpochRecord(versionedEpochNumber.getObject(), context)
                            .thenApply(epochRecord -> new VersionedMetadata<>(epochRecord, versionedEpochNumber.getVersion())));
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTEpochRecord>> getEpochRecordData(int epoch, OperationContext context) {
        Preconditions.checkNotNull(context, "context cannot be null");
        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
                    return storeHelper.getCachedOrLoad(metadataTable, key, KVTEpochRecord::fromBytes,
                            context.getOperationStartTime(), context.getRequestId());
                });
    }
}
