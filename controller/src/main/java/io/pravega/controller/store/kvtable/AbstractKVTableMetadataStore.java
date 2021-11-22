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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.kvtable.records.KVTSegmentRecord;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

@Slf4j
public abstract class AbstractKVTableMetadataStore implements KVTableMetadataStore {
    public static final Predicate<Throwable> DATA_NOT_FOUND_PREDICATE = e -> Exceptions.unwrap(e) instanceof 
            StoreException.DataNotFoundException;

    private final LoadingCache<String, Scope> scopeCache;
    private final LoadingCache<Pair<String, String>, KeyValueTable> cache;
    @Getter
    private final HostIndex hostTaskIndex;
    
    protected AbstractKVTableMetadataStore(HostIndex hostTaskIndex) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<Pair<String, String>, KeyValueTable>() {
                            @Override
                            @ParametersAreNonnullByDefault
                            public KeyValueTable load(Pair<String, String> input) {
                                try {
                                    return newKeyValueTable(input.getKey(), input.getValue());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

        scopeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, Scope>() {
                            @Override
                            @ParametersAreNonnullByDefault
                            public Scope load(String scopeName) {
                                try {
                                    return newScope(scopeName);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
        this.hostTaskIndex = hostTaskIndex;
    }

    public Scope getScope(final String scopeName, final OperationContext context) {
        if (context instanceof KVTOperationContext) {
            return ((KVTOperationContext) context).getScope();
        }
        Scope scope = scopeCache.getUnchecked(scopeName);
        scope.refresh();
        return scope;
    }

    abstract KeyValueTable newKeyValueTable(final String scope, final String kvTableName);

    @Override
    public KVTOperationContext createContext(String scopeName, String name, long requestId) {
        return new KVTOperationContext(getScope(scopeName, null), 
                getKVTable(scopeName, name, null), requestId);
    }

    @Override
    public KeyValueTable getKVTable(String scope, final String name, OperationContext context) {
        if (context instanceof KVTOperationContext) {
            return ((KVTOperationContext) context).getKvTable();
        }
        KeyValueTable kvt = cache.getUnchecked(new ImmutablePair<>(scope, name));
        log.debug("Got KVTable from cache: {}/{}", kvt.getScopeName(), kvt.getName());
        kvt.refresh();
        return kvt;
    }

    @Override
    public CompletableFuture<CreateKVTableResponse> createKeyValueTable(final String scope,
                                                                final String name,
                                                                final KeyValueTableConfiguration configuration,
                                                                final long createTimestamp,
                                                                final OperationContext ctx,
                                                                final Executor executor) {
        OperationContext context = getOperationContext(ctx);

        return Futures.completeOn(checkScopeExists(scope, context, executor)
                .thenCompose(exists -> {
                    if (exists) {
                        // Create kvtable may fail if scope is deleted as we attempt to create the table under scope.
                        return getSafeStartingSegmentNumberFor(scope, name, context, executor)
                                .thenCompose(startingSegmentNumber -> getKVTable(scope, name, context)
                                                .create(configuration, createTimestamp, startingSegmentNumber, context));
                    } else {
                        return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope does not exist"));
                    }
                }), executor);
    }

    OperationContext getOperationContext(OperationContext context) {
        return context != null ? context : new OperationContext() {
            private final long requestId = ControllerService.nextRequestId();
            private final long operationStartTime = System.currentTimeMillis();
            @Override
            public long getOperationStartTime() {
                return operationStartTime;
            }

            @Override
            public long getRequestId() {
                return requestId;
            }
        };
    }
    
    String getScopedKVTName(String scope, String name) {
        return String.format("%s/%s", scope, name);
    }

    @Override
    public CompletableFuture<Long> getCreationTime(final String scope,
                                                   final String name,
                                                   final OperationContext ctx,
                                                   final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getKVTable(scope, name, context).getCreationTime(context), executor);
    }

    @Override
    public CompletableFuture<Void> setState(final String scope, final String name,
                                            final KVTableState state, final OperationContext ctx,
                                            final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getKVTable(scope, name, context).updateState(state, context), executor);
    }

    @Override
    public CompletableFuture<KVTableState> getState(final String scope, final String name,
                                             final boolean ignoreCached,
                                             final OperationContext ctx,
                                             final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getKVTable(scope, name, context).getState(ignoreCached, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<KVTableState>> updateVersionedState(final String scope, final String name,
                                                                            final KVTableState state, final VersionedMetadata<KVTableState> previous,
                                                                            final OperationContext ctx,
                                                                            final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getKVTable(scope, name, context).updateVersionedState(previous, state, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<KVTableState>> getVersionedState(final String scope, final String name,
                                                                         final OperationContext ctx,
                                                                         final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getKVTable(scope, name, context).getVersionedState(context), executor);
    }

    @Override
    public CompletableFuture<List<KVTSegmentRecord>> getActiveSegments(final String scope, final String name, final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getKVTable(scope, name, context).getActiveSegments(context), executor);
    }

    @Override
    public CompletableFuture<KeyValueTableConfiguration> getConfiguration(final String scope,
                                                                          final String name,
                                                                          final OperationContext ctx, 
                                                                          final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getKVTable(scope, name, context).getConfiguration(context), executor);
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listKeyValueTables(final String scopeName, final String continuationToken,
                                                                            final int limit, final OperationContext ctx,
                                                                            final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getScope(scopeName, context).listKeyValueTables(limit, continuationToken, executor, context), executor);
    }

    @Override
    public CompletableFuture<Set<Long>> getAllSegmentIds(final String scope, final String name, final OperationContext ctx,
                                                         final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getKVTable(scope, name, context).getAllSegmentIds(context), executor);
    }

    @Override
    public CompletableFuture<Void> deleteKeyValueTable(final String scope,
                                                       final String name,
                                                       final OperationContext ctx,
                                                       final Executor executor) {
        OperationContext kvtContext = getOperationContext(ctx);
        KeyValueTable kvTable = getKVTable(scope, name, kvtContext);
        return Futures.completeOn(Futures.exceptionallyExpecting(
                kvTable.getActiveEpochRecord(true, kvtContext)
                       .thenApply(epoch -> epoch.getSegments().stream().map(KVTSegmentRecord::getSegmentNumber)
                                                .reduce(Integer::max).get())
                       .thenCompose(lastActiveSegment -> recordLastKVTableSegment(scope, name, lastActiveSegment,
                               kvtContext, executor)), DATA_NOT_FOUND_PREDICATE, null)
                                         .thenCompose(v -> kvTable.delete(kvtContext)), executor)
                      .thenCompose(v -> deleteFromScope(scope, name, kvtContext, executor));

    }

    /**
     * This method stores the last active segment for a stream upon its deletion. Persistently storing this value is
     * necessary in the case of a stream re-creation for picking an appropriate starting segment number.
     *
     * @param scope scope
     * @param kvtable kvtable
     * @param lastActiveSegment segment with highest number for a kvtable
     * @param ctx context
     * @param executor executor
     * @return CompletableFuture which indicates the task completion related to persistently store lastActiveSegment.
     */
    abstract CompletableFuture<Void> recordLastKVTableSegment(final String scope, final String kvtable, int lastActiveSegment,
                                                              final OperationContext ctx, final Executor executor);

    /**
     * This method retrieves a safe base segment number from which a stream's segment ids may start. In the case of a
     * new stream, this method will return 0 as a starting segment number (default). In the case that a stream with the
     * same name has been recently deleted, this method will provide as a safe starting segment number the last segment
     * number of the previously deleted stream + 1. This will avoid potential segment naming collisions with segments
     * being asynchronously deleted from the segment store.
     *
     * @param scopeName scope
     * @param kvtName KeyValueTable name
     * @return CompletableFuture with a safe starting segment number for this stream.
     */
    abstract CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String kvtName, 
                                                                        final OperationContext ctx, final Executor executor);
    
    abstract CompletableFuture<Void> deleteFromScope(final String scope,
                                                            final String name,
                                                            final OperationContext ctx,
                                                            final Executor executor);
}
