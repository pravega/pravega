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
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;
import io.pravega.controller.store.stream.records.ReaderGroupStateRecord;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static io.pravega.controller.store.PravegaTablesStoreHelper.BYTES_TO_UUID_FUNCTION;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * PravegaTables ReaderGroup.
 * This creates a top level metadata table for each readergroup.
 * All metadata records are stored in this metadata table.
 *
 * Each Reader Group is protected against recreation of another Reader Group with same name by attaching a UUID to the name.
 */
class PravegaTablesReaderGroup extends AbstractReaderGroup {
    public static final String SEPARATOR = ".#.";
    private static final String READER_GROUPS_TABLE_IDENTIFIER = "_readergroups";
    private static final String METADATA_TABLE = "metadata" + SEPARATOR + "%s";
    // metadata keys
    private static final String CREATION_TIME_KEY = "creationTime";
    private static final String CONFIGURATION_KEY = "configuration";
    private static final String STATE_KEY = "state";

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PravegaTablesReaderGroup.class));

    private final PravegaTablesStoreHelper storeHelper;
    private final BiFunction<Boolean, OperationContext, CompletableFuture<String>> readerGroupsInScopeTableNameSupplier;
    private final AtomicReference<String> idRef;

    @VisibleForTesting
    PravegaTablesReaderGroup(final String scopeName, final String rgName, PravegaTablesStoreHelper storeHelper,
                             BiFunction<Boolean, OperationContext, CompletableFuture<String>> rgInScopeTableNameSupplier,
                             ScheduledExecutorService executor) {
        super(scopeName, rgName);
        this.storeHelper = storeHelper;
        this.readerGroupsInScopeTableNameSupplier = rgInScopeTableNameSupplier;
        this.idRef = new AtomicReference<>(null);
    }

    private CompletableFuture<String> getId(OperationContext context) {
        String id = idRef.get();
        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            // first get the scope id from the cache.
            // if the cache does not contain scope id then we load it from the supplier. 
            // if cache contains the scope id then we load the readergroup id. if not found, we load the scopeid first.
            return storeHelper.loadFromTableHandleStaleTableName(readerGroupsInScopeTableNameSupplier, getName(),
                    BYTES_TO_UUID_FUNCTION, context)
                    .thenComposeAsync(data -> {
                        idRef.compareAndSet(null, data.getObject().toString());
                        return getId(context);
                    });
        }
    }

    private CompletableFuture<String> getMetadataTable(OperationContext context) {
        return getId(context).thenApply(this::getMetadataTableName);
    }

    private String getMetadataTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), READER_GROUPS_TABLE_IDENTIFIER, getName(), String.format(METADATA_TABLE, id));
    }

    @Override
    CompletableFuture<Void> createMetadataTables(OperationContext context) {
        return getId(context).thenCompose(id -> {
            String metadataTable = getMetadataTableName(id);
            return storeHelper.createTable(metadataTable, context.getRequestId())
                    .thenAccept(v -> log.debug("reader group {}/{} metadata table {} created", getScope(), getName(), metadataTable));
        });
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime, OperationContext context) {
        return Futures.toVoid(getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CREATION_TIME_KEY,
                        creationTime, PravegaTablesStoreHelper.LONG_TO_BYTES_FUNCTION, context.getRequestId())));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final ReaderGroupConfig configuration, OperationContext context) {
        ReaderGroupConfigRecord configRecord = ReaderGroupConfigRecord.update(configuration, 0L, false);
        return Futures.toVoid(getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CONFIGURATION_KEY,
                        configRecord, ReaderGroupConfigRecord::toBytes, context.getRequestId())));
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent(OperationContext context) {
        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, STATE_KEY,
                        ReaderGroupStateRecord.builder().state(ReaderGroupState.CREATING).build(), 
                        ReaderGroupStateRecord::toBytes, context.getRequestId())));

    }

    @Override
    CompletableFuture<Version> setStateData(final VersionedMetadata<ReaderGroupStateRecord> state, OperationContext context) {
        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, STATE_KEY, state.getObject(), 
                        ReaderGroupStateRecord::toBytes, state.getVersion(), context.getRequestId()));
    }

    @Override
    CompletableFuture<VersionedMetadata<ReaderGroupStateRecord>> getStateData(boolean ignoreCached, OperationContext context) {
        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(metadataTable, STATE_KEY);
                    }
                    return storeHelper.getCachedOrLoad(metadataTable, STATE_KEY, ReaderGroupStateRecord::fromBytes, 
                            ignoreCached ? context.getOperationStartTime() : 0L, context.getRequestId());
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getConfigurationData(boolean ignoreCached, OperationContext context) {
        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(metadataTable, CONFIGURATION_KEY);
                    }
                    return storeHelper.getCachedOrLoad(metadataTable, CONFIGURATION_KEY, ReaderGroupConfigRecord::fromBytes,
                            ignoreCached ? context.getOperationStartTime() : 0L, context.getRequestId());
                });
    }

    @Override
    public CompletableFuture<Void> delete(OperationContext context) {
        return getId(context).thenCompose(id -> storeHelper.deleteTable(getMetadataTableName(id), false, context.getRequestId())
        .thenCompose(v -> {
            this.idRef.set(null);
            return CompletableFuture.completedFuture(null);
        }));
    }

    @Override
    CompletableFuture<Version> setConfigurationData(final VersionedMetadata<ReaderGroupConfigRecord> configuration, OperationContext context) {
        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, CONFIGURATION_KEY, configuration.getObject(), 
                        ReaderGroupConfigRecord::toBytes, configuration.getVersion(), context.getRequestId()));
    }

    @Override
    public void refresh() {
        idRef.set(null);
    }
}
