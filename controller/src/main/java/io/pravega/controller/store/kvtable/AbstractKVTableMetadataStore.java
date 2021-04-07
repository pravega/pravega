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
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.kvtable.records.KVTSegmentRecord;
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
    public static final Predicate<Throwable> DATA_NOT_FOUND_PREDICATE = e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException;

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

    protected Scope getScope(final String scopeName) {
        Scope scope = scopeCache.getUnchecked(scopeName);
        scope.refresh();
        return scope;
    }

    abstract KeyValueTable newKeyValueTable(final String scope, final String kvTableName);

    @Override
    public KVTOperationContext createContext(String scope, String name) {
        return new KVTOperationContext(getKVTable(scope, name, null));
    }

    public KeyValueTable getKVTable(String scope, final String name, KVTOperationContext context) {
        KeyValueTable kvt;
        if (context != null) {
            kvt = context.getKvTable();
            assert kvt.getScopeName().equals(scope);
            assert kvt.getName().equals(name);
        } else {
            kvt = cache.getUnchecked(new ImmutablePair<>(scope, name));
            log.debug("Got KVTable from cache: {}/{}", kvt.getScopeName(), kvt.getName());
            kvt.refresh();
        }
        return kvt;
    }

    @Override
    public CompletableFuture<CreateKVTableResponse> createKeyValueTable(final String scope,
                                                                final String name,
                                                                final KeyValueTableConfiguration configuration,
                                                                final long createTimestamp,
                                                                final KVTOperationContext context,
                                                                final Executor executor) {
        return Futures.completeOn(checkScopeExists(scope)
                .thenCompose(exists -> {
                    if (exists) {
                        // Create kvtable may fail if scope is deleted as we attempt to create the table under scope.
                        return getSafeStartingSegmentNumberFor(scope, name)
                                .thenCompose(startingSegmentNumber -> getKVTable(scope, name, context)
                                                .create(configuration, createTimestamp, startingSegmentNumber));
                    } else {
                        return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope does not exist"));
                    }
                }), executor);
    }

    String getScopedKVTName(String scope, String name) {
        return String.format("%s/%s", scope, name);
    }

    @Override
    public CompletableFuture<Long> getCreationTime(final String scope,
                                                   final String name,
                                                   final KVTOperationContext context,
                                                   final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getCreationTime(), executor);
    }

    @Override
    public CompletableFuture<Void> setState(final String scope, final String name,
                                            final KVTableState state, final KVTOperationContext context,
                                            final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).updateState(state), executor);
    }

    @Override
    public CompletableFuture<KVTableState> getState(final String scope, final String name,
                                             final boolean ignoreCached,
                                             final KVTOperationContext context,
                                             final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getState(ignoreCached), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<KVTableState>> updateVersionedState(final String scope, final String name,
                                                                            final KVTableState state, final VersionedMetadata<KVTableState> previous,
                                                                            final KVTOperationContext context,
                                                                            final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).updateVersionedState(previous, state), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<KVTableState>> getVersionedState(final String scope, final String name,
                                                                         final KVTOperationContext context,
                                                                         final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getVersionedState(), executor);
    }

    @Override
    public CompletableFuture<List<KVTSegmentRecord>> getActiveSegments(final String scope, final String name, final KVTOperationContext context, final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getActiveSegments(), executor);
    }

    @Override
    public CompletableFuture<KeyValueTableConfiguration> getConfiguration(final String scope,
                                                                   final String name,
                                                                   final KVTOperationContext context, final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getConfiguration(), executor);
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listKeyValueTables(String scopeName, String continuationToken,
                                                                    int limit, Executor executor) {
        return getScope(scopeName).listKeyValueTables(limit, continuationToken, executor);
    }

    @Override
    public CompletableFuture<Set<Long>> getAllSegmentIds(final String scope, final String name, final KVTOperationContext context, final Executor executor) {
        return Futures.completeOn(getKVTable(scope, name, context).getAllSegmentIds(), executor);
    }

    @Override
    public CompletableFuture<Void> deleteKeyValueTable(final String scope,
                                                final String name,
                                                final KVTOperationContext context,
                                                final Executor executor) {
        KVTOperationContext kvtContext = (context == null) ? createContext(scope, name) : context;
        return Futures.exceptionallyExpecting(CompletableFuture.completedFuture(getKVTable(scope, name, kvtContext))
                .thenCompose(kvt -> kvt.getActiveEpochRecord(true))
                            .thenApply(epoch -> epoch.getSegments().stream().map(KVTSegmentRecord::getSegmentNumber)
                                .reduce(Integer::max).get())
                        .thenCompose(lastActiveSegment -> recordLastKVTableSegment(scope, name, lastActiveSegment, context, executor)),
                DATA_NOT_FOUND_PREDICATE, null)
                .thenCompose(v -> deleteFromScope(scope, name, context, executor))
                .thenCompose(v -> Futures.completeOn(getKVTable(scope, name, kvtContext).delete(), executor))
                .thenAccept(v -> cache.invalidate(new ImmutablePair<>(scope, name)));
    }

    /**
     * This method stores the last active segment for a stream upon its deletion. Persistently storing this value is
     * necessary in the case of a stream re-creation for picking an appropriate starting segment number.
     *
     * @param scope scope
     * @param kvtable kvtable
     * @param lastActiveSegment segment with highest number for a kvtable
     * @param context context
     * @param executor executor
     * @return CompletableFuture which indicates the task completion related to persistently store lastActiveSegment.
     */
    abstract CompletableFuture<Void> recordLastKVTableSegment(final String scope, final String kvtable, int lastActiveSegment,
                                                             KVTOperationContext context, final Executor executor);

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
    abstract CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String kvtName);

    public abstract CompletableFuture<Boolean> checkScopeExists(String scope);

    public abstract CompletableFuture<Void> createEntryForKVTable(final String scopeName,
                                                                  final String kvtName,
                                                                  final byte[] id,
                                                                  final Executor executor);

    public abstract CompletableFuture<Void> deleteFromScope(final String scope,
                                                                   final String name,
                                                                   final KVTOperationContext context,
                                                                   final Executor executor);
}
