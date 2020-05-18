package io.pravega.controller.store.kvtable;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.kvtable.KVTOperationContext;
import io.pravega.controller.store.kvtable.KVTOperationContextImpl;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.kvtable.KeyValueTable;
import io.pravega.shared.controller.event.ControllerEventSerializer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public abstract class AbstractKVTableMetadataStore implements KVTableMetadataStore{
    public static final Predicate<Throwable> DATA_NOT_FOUND_PREDICATE = e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException;
    public static final Predicate<Throwable> DATA_NOT_EMPTY_PREDICATE = e -> Exceptions.unwrap(e) instanceof StoreException.DataNotEmptyException;

    private final static String RESOURCE_PART_SEPARATOR = "_%_";

    private final LoadingCache<String, Scope> scopeCache;
    private final LoadingCache<Pair<String, String>, KeyValueTable> cache;
    private final HostIndex hostTaskIndex;
    private final ControllerEventSerializer controllerEventSerializer;


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
        this.controllerEventSerializer = new ControllerEventSerializer();
    }

    protected Scope getScope(final String scopeName) {
        Scope scope = scopeCache.getUnchecked(scopeName);
        scope.refresh();
        return scope;
    }

    /**
     * Returns a Scope object from scope identifier.
     *
     * @param scopeName scope identifier is scopeName.
     * @return Scope object.
     */
    abstract Scope newScope(final String scopeName);

    abstract KeyValueTable newKeyValueTable(final String scope, final String kvTableName);

    @Override
    public KVTOperationContext createContext(String scope, String name) {
        return new KVTOperationContextImpl<>(getKVTable(scope, name, null));
    }

    protected KeyValueTable getKVTable(String scope, final String name, KVTOperationContext context) {
        KeyValueTable kvt;
        if (context != null) {
            kvt = context.getKeyValueTable();
            assert kvt.getScope().equals(scope);
            assert kvt.getName().equals(name);
        } else {
            kvt = cache.getUnchecked(new ImmutablePair<>(scope, name));
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
        return getSafeStartingSegmentNumberFor(scope, name)
                .thenCompose(startingSegmentNumber ->
                        withCompletion(checkScopeExists(scope)
                                .thenCompose(exists -> {
                                    if (exists) {
                                        // Create stream may fail if scope is deleted as we attempt to create the stream under scope.
                                        return getKVTable(scope, name, context)
                                                .create(configuration, createTimestamp, startingSegmentNumber);
                                    } else {
                                        return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope does not exist"));
                                    }
                                }), executor));
    }

    String getScopedKVTName(String scope, String stream) {
        return String.format("%s/%s", scope, stream);
    }

    @Override
    public CompletableFuture<Long> getCreationTime(final String scope,
                                                   final String name,
                                                   final KVTOperationContext context,
                                                   final Executor executor) {
        return withCompletion(getKVTable(scope, name, context).getCreationTime(), executor);
    }

    protected <T> CompletableFuture<T> withCompletion(CompletableFuture<T> future, final Executor executor) {

        // Following makes sure that the result future given out to caller is actually completed on
        // caller's executor. So any chaining, if done without specifying an executor, will either happen on
        // caller's executor or fork join pool but never on someone else's executor.

        CompletableFuture<T> result = new CompletableFuture<>();

        future.whenCompleteAsync((r, e) -> {
            if (e != null) {
                result.completeExceptionally(e);
            } else {
                result.complete(r);
            }
        }, executor);

        return result;
    }
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

    abstract CompletableFuture<Boolean> checkScopeExists(String scope);
}
