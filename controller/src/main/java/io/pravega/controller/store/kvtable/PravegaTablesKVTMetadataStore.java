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
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.PravegaTablesScope;
import io.pravega.common.Exceptions;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.store.stream.StoreException;
import static io.pravega.shared.NameUtils.getQualifiedTableName;
import io.pravega.shared.NameUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Pravega Tables stream metadata store.
 */
@Slf4j
public class PravegaTablesKVTMetadataStore extends AbstractKVTableMetadataStore {
    static final String SCOPES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "scopes");
    static final String DELETED_KVTABLES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "deletedKVTables");

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final PravegaTablesStoreHelper storeHelper;
    private final ScheduledExecutorService executor;

    @VisibleForTesting
    PravegaTablesKVTMetadataStore(SegmentHelper segmentHelper, CuratorFramework curatorClient,
                                  ScheduledExecutorService executor, GrpcAuthHelper authHelper) {
        super(new ZKHostIndex(curatorClient, "/hostRequestIndex", executor));
        this.storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
        this.executor = executor;
    }

    @Override
    PravegaTablesKVTable newKeyValueTable(final String scope, final String name) {
        log.info("Fetching KV Table from PravegaTables store {}/{}", scope, name);
        return new PravegaTablesKVTable(scope, name, storeHelper,
                () -> ((PravegaTablesScope) getScope(scope)).getKVTablesInScopeTableName(), executor);
    }

    @Override
    public CompletableFuture<Void> deleteFromScope(final String scope,
                                                       final String name,
                                                       final KVTOperationContext context,
                                                       final Executor executor) {
        return Futures.completeOn(((PravegaTablesScope) getScope(scope)).removeKVTableFromScope(name),
                executor);
    }

    CompletableFuture<Void> recordLastKVTableSegment(final String scope, final String kvtable, int lastActiveSegment,
                                                     KVTOperationContext context, final Executor executor) {
        final String key = getScopedKVTName(scope, kvtable);
        byte[] maxSegmentNumberBytes = new byte[Integer.BYTES];
        BitConverter.writeInt(maxSegmentNumberBytes, 0, lastActiveSegment);
        return Futures.completeOn(storeHelper.createTable(DELETED_KVTABLES_TABLE)
                .thenCompose(created -> {
                    return storeHelper.expectingDataNotFound(storeHelper.getEntry(
                            DELETED_KVTABLES_TABLE, key, x -> BitConverter.readInt(x, 0)),
                            null)
                            .thenCompose(existing -> {
                                log.debug("Recording last segment {} for KeyValueTable {}/{} on deletion.", lastActiveSegment, scope, kvtable);
                                if (existing != null) {
                                    final int oldLastActiveSegment = existing.getObject();
                                    Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                            "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                            oldLastActiveSegment, scope, kvtable, lastActiveSegment);
                                    return Futures.toVoid(storeHelper.updateEntry(DELETED_KVTABLES_TABLE,
                                            key, maxSegmentNumberBytes, existing.getVersion()));
                                } else {
                                    return Futures.toVoid(storeHelper.addNewEntryIfAbsent(DELETED_KVTABLES_TABLE,
                                            key, maxSegmentNumberBytes));
                                }
                            });
                }), executor);
    }

    @Override
    public PravegaTablesScope newScope(final String scopeName) {
        return new PravegaTablesScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<Boolean> checkScopeExists(String scope) {
        return Futures.completeOn(storeHelper.expectingDataNotFound(
                storeHelper.getEntry(SCOPES_TABLE, scope, x -> x).thenApply(v -> true),
                false), executor);
    }

    @Override
    public CompletableFuture<Boolean> checkTableExists(String scopeName, String kvt) {
        return Futures.completeOn(((PravegaTablesScope) getScope(scopeName)).checkKeyValueTableExistsInScope(kvt), executor);
    }

    public CompletableFuture<Void> createEntryForKVTable(final String scopeName,
                                                         final String kvtName,
                                                         final byte[] id,
                                                         final Executor executor) {
        return Futures.completeOn(((PravegaTablesScope) getScope(scopeName)).addKVTableToScope(kvtName, id), executor);
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String kvtName) {
        return Futures.completeOn(storeHelper.getEntry(DELETED_KVTABLES_TABLE, getScopedKVTName(scopeName, kvtName),
                x -> BitConverter.readInt(x, 0))
                .handle((data, ex) -> {
                    if (ex == null) {
                        return data.getObject() + 1;
                    } else if (Exceptions.unwrap(ex) instanceof StoreException.DataNotFoundException) {
                        return 0;
                    } else {
                        log.error("Problem found while getting a safe starting segment number for {}.",
                                getScopedKVTName(scopeName, kvtName), ex);
                        throw new CompletionException(ex);
                    }
                }), executor);
    }

    @Override
    public void close() {
        // do nothing
    }
}
