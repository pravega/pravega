package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.PravegaTablesScope;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.records.readergroup.ReaderGroupConfigRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * PravegaTables ReaderGroup.
 * This creates a top level metadata table for each readergroup.
 * All metadata records are stored in metadata table.
 *
 * Each kvTable is protected against recreation of another readergroup with same name by attaching a UUID to the name.
 */
@Slf4j
class PravegaTablesReaderGroup extends AbstractReaderGroup {
    public static final String SEPARATOR = ".#.";
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
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), getName(), String.format(METADATA_TABLE, id));
    }

    @Override
    CompletableFuture<Void> createMetadataTables() {
        return getId().thenCompose(id -> {
            String metadataTable = getMetadataTableName(id);

            return storeHelper.createTable(metadataTable)
                    .thenAccept(v -> log.debug("reader group {}/{} metadata table {} created", getScope(), getName(), metadataTable);
        });
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(long creationTime) {
        return null;
    }

    @Override
    CompletableFuture<Void> createConfigurationIfAbsent(ReaderGroupConfigRecord data) {
        return null;
    }


    @Override
    public void refresh() {

    }
}
