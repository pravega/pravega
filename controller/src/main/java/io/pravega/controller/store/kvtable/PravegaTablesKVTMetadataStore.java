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
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.PravegaTablesScope;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.shared.NameUtils;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.controller.store.PravegaTablesStoreHelper.BYTES_TO_INTEGER_FUNCTION;
import static io.pravega.controller.store.PravegaTablesStoreHelper.INTEGER_TO_BYTES_FUNCTION;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * Pravega Tables stream metadata store.
 */
public class PravegaTablesKVTMetadataStore extends AbstractKVTableMetadataStore {
    static final String SCOPES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "scopes");
    static final String DELETED_KVTABLES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "deletedKVTables");

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PravegaTablesKVTMetadataStore.class));

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

    @VisibleForTesting
    public PravegaTablesKVTMetadataStore(CuratorFramework curatorClient, ScheduledExecutorService executor, PravegaTablesStoreHelper storeHelper) {
        super(new ZKHostIndex(curatorClient, "/hostRequestIndex", executor));
        this.storeHelper = storeHelper;
        this.executor = executor;
    }

    @Override
    PravegaTablesKVTable newKeyValueTable(final String scope, final String name) {
        log.debug("Fetching KV Table from PravegaTables store {}/{}", scope, name);
        return new PravegaTablesKVTable(scope, name, storeHelper,
                (x, y) -> ((PravegaTablesScope) getScope(scope, y)).getKVTablesInScopeTableName(y), executor);
    }

    @Override
    public CompletableFuture<Void> deleteFromScope(final String scope,
                                                   final String name,
                                                   final OperationContext ctx,
                                                   final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(((PravegaTablesScope) getScope(scope, context)).removeKVTableFromScope(name, context),
                executor);
    }

    @Override
    CompletableFuture<Void> recordLastKVTableSegment(final String scope, final String kvtable, int lastActiveSegment,
                                                     OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        final String key = getScopedKVTName(scope, kvtable);
        return Futures.completeOn(storeHelper.createTable(DELETED_KVTABLES_TABLE, context.getRequestId())
                .thenCompose(created -> {
                    return storeHelper.expectingDataNotFound(storeHelper.getCachedOrLoad(
                            DELETED_KVTABLES_TABLE, key, BYTES_TO_INTEGER_FUNCTION, System.currentTimeMillis(), context.getRequestId()),
                            null)
                            .thenCompose(existing -> {
                                log.debug(context.getRequestId(), "Recording last segment {} for KeyValueTable {}/{} on deletion.",
                                        lastActiveSegment, scope, kvtable);
                                if (existing != null) {
                                    final int oldLastActiveSegment = existing.getObject();
                                    Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                            "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                            oldLastActiveSegment, scope, kvtable, lastActiveSegment);
                                    return Futures.toVoid(storeHelper.updateEntry(DELETED_KVTABLES_TABLE,
                                            key, lastActiveSegment, INTEGER_TO_BYTES_FUNCTION, existing.getVersion(), context.getRequestId()));
                                } else {
                                    return Futures.toVoid(storeHelper.addNewEntryIfAbsent(DELETED_KVTABLES_TABLE,
                                            key, lastActiveSegment, INTEGER_TO_BYTES_FUNCTION, context.getRequestId()));
                                }
                            });
                }), executor);
    }

    @Override
    public PravegaTablesScope newScope(final String scopeName) {
        return new PravegaTablesScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<Boolean> checkScopeExists(String scope, OperationContext context, Executor executor) {
        long requestId = getOperationContext(context).getRequestId();

        return Futures.completeOn(storeHelper.expectingDataNotFound(
                storeHelper.getEntry(SCOPES_TABLE, scope, x -> x, requestId).thenApply(v -> true),
                false), executor);
    }

    @Override
    public CompletableFuture<Boolean> isScopeSealed(String scope, OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(((PravegaTablesScope) getScope(scope, context)).isScopeSealed(scope, context), executor);
    }

    @Override
    public CompletableFuture<Boolean> checkTableExists(String scopeName, String kvt, OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(((PravegaTablesScope) getScope(scopeName, context)).checkKeyValueTableExistsInScope(kvt, context), executor);
    }

    @Override
    public CompletableFuture<Void> createEntryForKVTable(final String scopeName,
                                                         final String kvtName,
                                                         final UUID id,
                                                         final OperationContext ctx, 
                                                         final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(((PravegaTablesScope) getScope(scopeName, context))
                .addKVTableToScope(kvtName, id, context), executor);
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String kvtName, 
                                                                      OperationContext context, Executor executor) {
        long requestId = getOperationContext(context).getRequestId();

        return Futures.completeOn(storeHelper.getEntry(DELETED_KVTABLES_TABLE, getScopedKVTName(scopeName, kvtName),
                BYTES_TO_INTEGER_FUNCTION, requestId)
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
