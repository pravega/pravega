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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.lang.Int96;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamCutReferenceRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.StreamSubscriber;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ControllerEventSerializer;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Abstract Stream metadata store. It implements various read queries using the Stream interface.
 * Implementation of create and update queries are delegated to the specific implementations of this abstract class.
 */
public abstract class AbstractStreamMetadataStore implements StreamMetadataStore {
    public static final Predicate<Throwable> DATA_NOT_FOUND_PREDICATE = e -> Exceptions.unwrap(e) 
            instanceof StoreException.DataNotFoundException;
    public static final Predicate<Throwable> DATA_NOT_EMPTY_PREDICATE = e -> Exceptions.unwrap(e) 
            instanceof StoreException.DataNotEmptyException;

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(AbstractStreamMetadataStore.class));
    private final static String RESOURCE_PART_SEPARATOR = "_%_";
    
    private final LoadingCache<String, Scope> scopeCache;
    private final LoadingCache<Pair<String, String>, Stream> cache;
    private final LoadingCache<Pair<String, String>, ReaderGroup> rgCache;
    private final HostIndex hostTxnIndex;
    @Getter
    private final HostIndex hostTaskIndex;
    private final ControllerEventSerializer controllerEventSerializer = new ControllerEventSerializer();

    protected AbstractStreamMetadataStore(HostIndex hostTxnIndex, HostIndex hostTaskIndex) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<Pair<String, String>, Stream>() {
                            @Override
                            @ParametersAreNonnullByDefault
                            public Stream load(Pair<String, String> input) {
                                try {
                                    return newStream(input.getKey(), input.getValue());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

        rgCache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<Pair<String, String>, ReaderGroup>() {
                            @Override
                            @ParametersAreNonnullByDefault
                            public ReaderGroup load(Pair<String, String> input) {
                                try {
                                    return newReaderGroup(input.getKey(), input.getValue());
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

        this.hostTxnIndex = hostTxnIndex;
        this.hostTaskIndex = hostTaskIndex;
    }

    /**
     * Returns a Scope object from scope identifier.
     *
     * @param scopeName scope identifier is scopeName.
     * @return Scope object.
     */
    abstract Scope newScope(final String scopeName);

    @Override
    public OperationContext createScopeContext(String scopeName, long requestId) {
        Scope scope = getScope(scopeName, null);
        return new ScopeOperationContext(scope, requestId);
    }

    @Override
    public OperationContext createStreamContext(String scopeName, String streamName, long requestId) {
        Scope scope = getScope(scopeName, null);
        Stream stream = getStream(scopeName, streamName, null);
        return new StreamOperationContext(scope, stream, requestId);
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(final String scope,
                                                   final String name,
                                                   final StreamConfiguration configuration,
                                                   final long createTimestamp,
                                                   final OperationContext ctx,
                                                   final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return getSafeStartingSegmentNumberFor(scope, name, context, executor)
                .thenCompose(startingSegmentNumber ->
                        Futures.completeOn(checkScopeExists(scope, context, executor)
                                .thenCompose(exists -> {
                                    if (exists) {
                                        // Create stream may fail if scope is deleted as we attempt to create the stream under scope.
                                        return getStream(scope, name, context)
                                                .create(configuration, createTimestamp, startingSegmentNumber, context);
                                    } else {
                                        return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope does not exist"));
                                    }
                                }), executor));
    }

    @Override
    public CompletableFuture<Void> deleteStream(final String scope,
                                                final String name,
                                                final OperationContext ctx,
                                                final Executor executor) {
        OperationContext context = getOperationContext(ctx);

        Stream s = getStream(scope, name, context);
        return Futures.exceptionallyExpecting(s.getActiveEpoch(true, context)
                .thenApply(epoch -> epoch.getSegments().stream().map(StreamSegmentRecord::getSegmentNumber)
                                                                    .reduce(Integer::max).get())
                .thenCompose(lastActiveSegment -> recordLastStreamSegment(scope, name, lastActiveSegment, context, executor)),
                DATA_NOT_FOUND_PREDICATE, null)
                .thenCompose(v -> Futures.completeOn(s.delete(context), executor))
                      .thenAccept(v -> cache.invalidate(new ImmutablePair<>(scope, name)));
    }

    @Override
    public CompletableFuture<Long> getCreationTime(final String scope,
                                                   final String name,
                                                   final OperationContext ctx,
                                                   final Executor executor) {
        OperationContext context = getOperationContext(ctx);

        return Futures.completeOn(getStream(scope, name, context).getCreationTime(context), executor);
    }

    @Override
    public CompletableFuture<Void> setState(final String scope, final String name,
                                               final State state, final OperationContext ctx,
                                               final Executor executor) {
        OperationContext context = getOperationContext(ctx);

        return Futures.completeOn(getStream(scope, name, context).updateState(state, context), executor);
    }

    @Override
    public CompletableFuture<State> getState(final String scope, final String name,
                                             final boolean ignoreCached,
                                             final OperationContext ctx,
                                             final Executor executor) {
        OperationContext context = getOperationContext(ctx);

        return Futures.completeOn(getStream(scope, name, context).getState(ignoreCached, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<State>> updateVersionedState(final String scope, final String name,
                                                                            final State state, 
                                                                            final VersionedMetadata<State> previous, 
                                                                            final OperationContext ctx,
                                                                            final Executor executor) {
        OperationContext context = getOperationContext(ctx);

        return Futures.completeOn(getStream(scope, name, context).updateVersionedState(previous, state, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<State>> getVersionedState(final String scope, final String name,
                                             final OperationContext ctx,
                                             final Executor executor) {
        OperationContext context = getOperationContext(ctx);

        return Futures.completeOn(getStream(scope, name, context).getVersionedState(context), executor);
    }

    /**
     * Create a scope with given name.
     *
     * @param scopeName Name of scope to created.
     * @return CreateScopeStatus future.
     */
    @Override
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName, final OperationContext ctx,
                                                            Executor executor) {
        OperationContext context = getOperationContext(ctx);

        Scope scope = getScope(scopeName, context);
        return Futures.completeOn(scope.isScopeSealed(scopeName, context).thenCompose(isScopeSealed -> {
            if (isScopeSealed) {
                return CompletableFuture.completedFuture(CreateScopeStatus.newBuilder().
                        setStatus(CreateScopeStatus.Status.SCOPE_EXISTS).build());
            }
                return scope.createScope(context).handle((result, ex) -> {
                    if (ex == null) {
                        return CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SUCCESS).build();
                    }
                    if (Exceptions.unwrap(ex) instanceof StoreException.DataExistsException) {
                        return CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SCOPE_EXISTS).build();
                    } else {
                        log.error(context.getRequestId(), "Create scope failed for scope {} due to ", scopeName, ex);
                        return CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.FAILURE).build();
                    }
                });
        }), executor);
    }

    /**
     * Delete a scope with given name.
     *
     * @param scopeName Name of scope to be deleted
     * @return DeleteScopeStatus future.
     */
    @Override
    public CompletableFuture<DeleteScopeStatus> deleteScope(final String scopeName, final OperationContext ctx,
                                                            Executor executor) {
        OperationContext context = getOperationContext(ctx);

        return Futures.completeOn(getScope(scopeName, context).deleteScope(context).handle((result, e) -> {
            Throwable ex = Exceptions.unwrap(e);
            final String message = "DeleteScope failed for scope";
            return getDeleteScopeStatus(scopeName, context, ex, message);
        }), executor);
    }

    private DeleteScopeStatus getDeleteScopeStatus(String scopeName, OperationContext context,
                                                   Throwable ex, String message) {
        if (ex == null) {
            return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SUCCESS).build();
        }
        if (ex instanceof StoreException.DataNotFoundException) {
            return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build();
        } else if (ex instanceof StoreException.DataNotEmptyException) {
            return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY).build();
        } else {
            log.error(context.getRequestId(), message + " {} due to {} ", scopeName, ex);
            return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.FAILURE).build();
        }
    }

    /**
     * Delete a scope recursively with given name.
     *
     * @param scopeName Name of scope to be deleted
     * @return DeleteScopeRecursiveStatus future.
     */
    @Override
    public CompletableFuture<DeleteScopeStatus> deleteScopeRecursive(final String scopeName, final OperationContext ctx,
                                                            Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getScope(scopeName, context).deleteScopeRecursive(context).handle((result, e) -> {
            Throwable ex = Exceptions.unwrap(e);
            final String message = "DeleteScopeRecursive failed for scope";
            return getDeleteScopeStatus(scopeName, context, ex, message);
        }), executor);
    }

    /**
     * List the streams in scope.
     *
     * @param scopeName Name of scope
     * @return A map of streams in scope to their configs.
     */
    @Override
    public CompletableFuture<Map<String, StreamConfiguration>> listStreamsInScope(final String scopeName, OperationContext ctx, 
                                                                                  Executor executor) {
        OperationContext context = getOperationContext(ctx);

        return Futures.completeOn(getScope(scopeName, context).listStreamsInScope(context).thenCompose(streams -> {
            HashMap<String, CompletableFuture<Optional<StreamConfiguration>>> result = new HashMap<>();
            for (String s : streams) {
                Stream stream = getStream(scopeName, s, null);
                CompletableFuture<StreamConfiguration> configurationFuture = stream.getConfiguration(context);
                CompletableFuture<State> stateFuture = stream.getState(true, context);
                // We are filtering out all streams for the configuration didn't exist. 
                // However if the stream is partially created (state < ACTIVE) we will throw data not found exception 
                // as well so that it is filtered out from the response.  
                CompletableFuture<StreamConfiguration> future = CompletableFuture
                        .allOf(configurationFuture, stateFuture)
                        .thenApply(v -> {
                            State state = stateFuture.join();
                            if (state.equals(State.CREATING) || state.equals(State.UNKNOWN)) {
                                throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, 
                                        "Partially created stream");
                            }
                            return configurationFuture.join();
                        });
                result.put(stream.getName(), 
                        Futures.exceptionallyExpecting(future, 
                                e -> e instanceof StoreException.DataNotFoundException, 
                                null)
                        .thenApply(Optional::ofNullable));
            }
            return Futures.allOfWithResults(result)
                    .thenApply(x -> {
                        return x.entrySet().stream().filter(y -> y.getValue().isPresent())
                         .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()));
                    });
        }), executor);
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStream(String scopeName, String continuationToken,
                                                                    int limit, Executor executor, OperationContext ctx) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getScope(scopeName, context).listStreams(limit, continuationToken, executor, context), executor);
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreamsForTag(String scopeName, String tag, String continuationToken,
                                                                           Executor executor, OperationContext ctx) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getScope(scopeName, context).listStreamsForTag(tag, continuationToken, executor, context), executor);
    }

    @Override
    public CompletableFuture<Void> startTruncation(final String scope,
                                                   final String name,
                                                   final Map<Long, Long> streamCut,
                                                   final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).startTruncation(streamCut, context), executor);
    }

    @Override
    public CompletableFuture<Void> completeTruncation(final String scope, final String name,
                                                      final VersionedMetadata<StreamTruncationRecord> record,
                                                      final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).completeTruncation(record, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationRecord(final String scope,
                                                                                            final String name,
                                                                                            final OperationContext ctx,
                                                                                            final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).getTruncationRecord(context), executor);
    }

    @Override
    public CompletableFuture<Void> startUpdateConfiguration(final String scope,
                                                            final String name,
                                                            final StreamConfiguration configuration,
                                                            final OperationContext ctx,
                                                            final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).startUpdateConfiguration(configuration, context), executor);
    }

    @Override
    public CompletableFuture<Void> completeUpdateConfiguration(final String scope, final String name,
                                                               final VersionedMetadata<StreamConfigurationRecord> existing,
                                                               final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).completeUpdateConfiguration(existing, context), executor);
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration(final String scope,
                                                                   final String name,
                                                                   final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).getConfiguration(context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getConfigurationRecord(final String scope,
                                                                                                  final String name,
                                                                                                  final OperationContext ctx,
                                                                                                  final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).getVersionedConfigurationRecord(context), executor);
    }

    @Override
    public CompletableFuture<Boolean> isSealed(final String scope, final String name, final OperationContext ctx, 
                                               final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).getState(true, context)
                                                                 .thenApply(state -> state.equals(State.SEALED)), executor);
    }

    @Override
    public CompletableFuture<Void> setSealed(final String scope, final String name, final OperationContext ctx, 
                                             final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).updateState(State.SEALED, context), executor);
    }


    @Override
    public CompletableFuture<StreamSegmentRecord> getSegment(final String scope, final String name, final long segmentId, 
                                                             final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).getSegment(segmentId, context), executor);
    }
    
    @Override
    public CompletableFuture<Set<Long>> getAllSegmentIds(final String scope, final String name, final OperationContext ctx, 
                                                         final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).getAllSegmentIds(context), executor);
    }

    @Override
    public CompletableFuture<Map<StreamSegmentRecord, Long>> getSegmentsAtHead(final String scope, final String name, 
                                                                               final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        final Stream stream = getStream(scope, name, context);
        return Futures.completeOn(stream.getSegmentsAtHead(context), executor);
    }

    @Override
    public CompletableFuture<List<StreamSegmentRecord>> getActiveSegments(final String scope, final String name, 
                                                                          final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        final Stream stream = getStream(scope, name, context);
        return Futures.completeOn(stream.getState(true, context)
                        .thenComposeAsync(state -> {
                            if (State.SEALED.equals(state)) {
                                return CompletableFuture.completedFuture(Collections.emptyList());
                            } else {
                                return stream.getActiveSegments(context);
                            }
                        }, executor),
                executor);
    }
    
    @Override
    public CompletableFuture<List<StreamSegmentRecord>> getSegmentsInEpoch(final String scope,
                                                              final String stream,
                                                              final int epoch,
                                                              final OperationContext ctx,
                                                              final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        final Stream streamObj = getStream(scope, stream, context);
        return Futures.completeOn(streamObj.getSegmentsInEpoch(epoch, context), executor);
    }

    @Override
    public CompletableFuture<Map<StreamSegmentRecord, List<Long>>> getSuccessors(final String scope, final String streamName,
                                                                                 final long segmentId, 
                                                                                 final OperationContext ctx,
                                                                                 final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scope, streamName, context);
        return Futures.completeOn(stream.getSuccessorsWithPredecessors(segmentId, context), executor);
    }

    @Override
    public CompletableFuture<List<StreamSegmentRecord>> getSegmentsBetweenStreamCuts(final String scope, 
                                                                                     final String streamName, 
                                                                                     final Map<Long, Long> from,
                                                                                     final Map<Long, Long> to, 
                                                                                     final OperationContext ctx, 
                                                                                     final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scope, streamName, context);
        return Futures.completeOn(stream.getSegmentsBetweenStreamCuts(from, to, context), executor);
    }

    @Override
    public CompletableFuture<Boolean> isStreamCutValid(final String scope,
                                                final String streamName,
                                                final Map<Long, Long> streamCut,
                                                final OperationContext ctx,
                                                final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scope, streamName, context);
        return Futures.completeOn(stream.isStreamCutValid(streamCut, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> submitScale(final String scope,
                                                                                   final String name,
                                                                                   final List<Long> sealedSegments,
                                                                                   final List<Map.Entry<Double, Double>> newRanges,
                                                                                   final long scaleTimestamp,
                                                                                   final VersionedMetadata<EpochTransitionRecord> record,
                                                                                   final OperationContext ctx,
                                                                                   final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).submitScale(sealedSegments, newRanges, scaleTimestamp, 
                record, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransition(String scope, String stream,
                                                                                          OperationContext ctx, 
                                                                                          ScheduledExecutorService executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).getEpochTransition(context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> resetEpochTransition(final String scope,
                                                 final String name,
                                                 final VersionedMetadata<EpochTransitionRecord> record,
                                                 final OperationContext context,
                                                 final Executor executor) {
        OperationContext ctx = getOperationContext(context);
        return Futures.completeOn(getStream(scope, name, context).resetEpochTransition(record, ctx), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> startScale(final String scope,
                                                          final String name,
                                                          final boolean isManualScale,
                                                          final VersionedMetadata<EpochTransitionRecord> record,
                                                          final VersionedMetadata<State> state,
                                                          final OperationContext ctx,
                                                          final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).startScale(isManualScale, record, state, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> scaleCreateNewEpochs(final String scope,
                                                          final String name,
                                                          final VersionedMetadata<EpochTransitionRecord> record,
                                                          final OperationContext ctx,
                                                          final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).scaleCreateNewEpoch(record, context), executor);
    }
    
    @Override
    public CompletableFuture<Void> scaleSegmentsSealed(final String scope,
                                                       final String name,
                                                       final Map<Long, Long> sealedSegmentSizes,
                                                       final VersionedMetadata<EpochTransitionRecord> record,
                                                       final OperationContext ctx,
                                                       final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        CompletableFuture<Void> future = Futures.completeOn(getStream(scope, name, context).scaleOldSegmentsSealed(
                sealedSegmentSizes, record, context), executor);

        future.thenCompose(result -> CompletableFuture.allOf(
                getActiveSegments(scope, name, context, executor).thenAccept(list ->
                        StreamMetrics.reportActiveSegments(scope, name, list.size())),
                findNumSplitsMerges(scope, name, context, executor).thenAccept(simpleEntry ->
                        StreamMetrics.reportSegmentSplitsAndMerges(scope, name, simpleEntry.getKey(), simpleEntry.getValue()))
        ));
        return future;
    }

    @Override
    public CompletableFuture<Void> completeScale(final String scope,
                                                 final String name,
                                                 final VersionedMetadata<EpochTransitionRecord> record,
                                                 final OperationContext ctx,
                                                 final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).completeScale(record, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> startRollingTxn(String scope, String stream,
                                                                                              int activeEpoch,
                                                                                              VersionedMetadata<CommittingTransactionsRecord> existing,
                                                                                              OperationContext ctx, 
                                                                                              ScheduledExecutorService executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).startRollingTxn(activeEpoch, existing, context), executor);
    }


    @Override
    public CompletableFuture<Void> rollingTxnCreateDuplicateEpochs(String scope, String name,
                        Map<Long, Long> sealedTxnEpochSegments, long time, VersionedMetadata<CommittingTransactionsRecord> record,
                        OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).rollingTxnCreateDuplicateEpochs(sealedTxnEpochSegments, 
                time, record, context), executor);
    }

    @Override
    public CompletableFuture<Void> completeRollingTxn(String scope, String name, Map<Long, Long> sealedActiveEpochSegments,
                                                      VersionedMetadata<CommittingTransactionsRecord> record, 
                                                      OperationContext ctx, Executor executor) {

        OperationContext context = getOperationContext(ctx);
        CompletableFuture<Void> future = Futures.completeOn(getStream(scope, name, context).completeRollingTxn(
                sealedActiveEpochSegments, 
                record, context), executor);

        future.thenCompose(result -> findNumSplitsMerges(scope, name, context, executor).thenAccept(simpleEntry ->
                StreamMetrics.reportSegmentSplitsAndMerges(scope, name, simpleEntry.getKey(), simpleEntry.getValue())));

        return future;
    }

    @Override
    public CompletableFuture<Void> addSubscriber(final String scopeName, final String streamName, String subscriber,
                                                 final long generation, final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scopeName, streamName, context);
        return Futures.completeOn(stream.addSubscriber(subscriber, generation, context), executor);
    }

    @Override
    public CompletableFuture<Void> deleteSubscriber(final String scope,
                                                    final String name,
                                                    final String subscriber,
                                                    final long generation,
                                                    final OperationContext ctx,
                                                    final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).deleteSubscriber(subscriber, generation, context), executor);
    }

    @Override
    public CompletableFuture<Void> updateSubscriberStreamCut(final String scope,
                                                            final String name,
                                                            final String subscriber,
                                                            final long generation,
                                                            final ImmutableMap<Long, Long> streamCut,
                                                            final VersionedMetadata<StreamSubscriber> previousRecord,
                                                            final OperationContext ctx,
                                                            final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context)
                .updateSubscriberStreamCut(previousRecord, subscriber, generation, streamCut, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamSubscriber>> getSubscriber(final String scope, final String name,
                                                                                              final String subscriber,
                                                                                              final OperationContext ctx,
                                                                                              final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).getSubscriberRecord(subscriber, context), executor);
    }

    @Override
    public CompletableFuture<List<String>> listSubscribers(final String scope, final String name,
                                                                                final OperationContext ctx,
                                                                                final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).listSubscribers(context), executor);
    }

    @Override
    public CompletableFuture<Void> addStreamCutToRetentionSet(final String scope, final String name, 
                                                              final StreamCutRecord streamCut,
                                                              final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scope, name, context);
        return Futures.completeOn(stream.addStreamCutToRetentionSet(streamCut, context), executor);
    }

    @Override
    public CompletableFuture<RetentionSet> getRetentionSet(final String scope, final String name,
                                                           final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scope, name, context);
        return Futures.completeOn(stream.getRetentionSet(context), executor);
    }

    @Override
    public CompletableFuture<StreamCutRecord> getStreamCutRecord(final String scope, final String name,
                                                          final StreamCutReferenceRecord reference,
                                                          final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scope, name, context);
        return Futures.completeOn(stream.getStreamCutRecord(reference, context), executor);
    }

    @Override
    public CompletableFuture<Void> deleteStreamCutBefore(final String scope, final String name, 
                                                         final StreamCutReferenceRecord streamCut,
                                                         final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scope, name, context);
        return Futures.completeOn(stream.deleteStreamCutBefore(streamCut, context), executor);
    }

    @Override
    public CompletableFuture<Long> getSizeTillStreamCut(final String scope, final String name, final Map<Long, Long> streamCut,
                                                        final Optional<StreamCutRecord> reference, final OperationContext ctx, 
                                                        final ScheduledExecutorService executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scope, name, context);
        return Futures.completeOn(stream.getSizeTillStreamCut(streamCut, reference, context), executor);
    }

    @Override
    public CompletableFuture<UUID> generateTransactionId(final String scopeName, final String streamName,
                                                             final OperationContext ctx,
                                                             final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scopeName, streamName, context);

        // This can throw write conflict exception
        CompletableFuture<Int96> nextFuture = getNextCounter();
        return Futures.completeOn(nextFuture.thenCompose(next -> stream.generateNewTxnId(next.getMsb(), next.getLsb(), 
                context)), executor);
    }

    @Override
    public CompletableFuture<VersionedTransactionData> createTransaction(final String scopeName,
                                                                         final String streamName,
                                                                         final UUID txnId,
                                                                         final long lease,
                                                                         final long maxExecutionTime,
                                                                         final OperationContext ctx,
                                                                         final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scopeName, streamName, context);
        return Futures.completeOn(stream.createTransaction(txnId, lease, maxExecutionTime, context), executor);
    }

    @Override
    public CompletableFuture<VersionedTransactionData> pingTransaction(final String scopeName, final String streamName,
                                                                       final VersionedTransactionData txData,
                                                                       final long lease,
                                                                       final OperationContext ctx,
                                                                       final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scopeName, streamName, context).pingTransaction(txData, lease, context), executor);
    }

    @Override
    public CompletableFuture<VersionedTransactionData> getTransactionData(final String scopeName,
                                                                          final String streamName,
                                                                          final UUID txId,
                                                                          final OperationContext ctx,
                                                                          final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scopeName, streamName, context).getTransactionData(txId, context), executor);
    }

    @Override
    public CompletableFuture<TxnStatus> transactionStatus(final String scopeName, final String streamName,
                                                          final UUID txId, final OperationContext ctx,
                                                          final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scopeName, streamName, context).checkTransactionStatus(txId, context), executor);
    }

    @VisibleForTesting
    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String streamName,
                                                          final UUID txId, final OperationContext ctx,
                                                          final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scope, streamName, context);
        return Futures.completeOn(((PersistentStreamBase) stream).commitTransaction(txId, context), executor);
    }

    @Override
    public CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealTransaction(final String scopeName,
                                                                              final String streamName,
                                                                              final UUID txId,
                                                                              final boolean commit,
                                                                              final Optional<Version> version,
                                                                              final String writerId,
                                                                              final long timestamp,
                                                                              final OperationContext ctx,
                                                                              final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scopeName, streamName, context)
                .sealTransaction(txId, commit, version, writerId, timestamp, context), executor);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final String scope, final String streamName,
                                                         final UUID txId, final OperationContext ctx,
                                                         final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        Stream stream = getStream(scope, streamName, context);
        return Futures.completeOn(stream.abortTransaction(txId, context), executor);
    }

    @Override
    public CompletableFuture<Void> addTxnToIndex(String hostId, TxnResource txn, Version version) {
        return hostTxnIndex.addEntity(hostId, getTxnResourceString(txn), Optional.ofNullable(version)
                                                                                 .orElse(getEmptyVersion()).toBytes());
    }

    @Override
    public CompletableFuture<Void> removeTxnFromIndex(String hostId, TxnResource txn, boolean deleteEmptyParent) {
        return hostTxnIndex.removeEntity(hostId, getTxnResourceString(txn), deleteEmptyParent);
    }

    @Override
    public CompletableFuture<Optional<TxnResource>> getRandomTxnFromIndex(final String hostId) {
        return hostTxnIndex.getEntities(hostId).thenApply(list -> list != null && list.size() > 0 ?
                Optional.of(this.getTxnResource(list.get(RandomFactory.create().nextInt(list.size())))) : Optional.empty());
    }

    @Override
    public CompletableFuture<Version> getTxnVersionFromIndex(final String hostId, final TxnResource resource) {
        return hostTxnIndex.getEntityData(hostId, getTxnResourceString(resource))
                .thenApply(data -> Optional.ofNullable(data).map(this::parseVersionData)
                                           .filter(x -> !x.equals(getEmptyVersion())).orElse(null));
    }

    @Override
    public CompletableFuture<Void> removeHostFromIndex(String hostId) {
        return hostTxnIndex.removeHost(hostId);
    }

    @Override
    public CompletableFuture<Set<String>> listHostsOwningTxn() {
        return hostTxnIndex.getHosts();
    }

    @Override
    public CompletableFuture<Map<String, ControllerEvent>> getPendingsTaskForHost(String hostId, int limit) {
        return hostTaskIndex.getEntities(hostId)
                .thenCompose(list ->
                        Futures.allOfWithResults(list.stream()
                                                     .limit(limit)
                                                     .collect(Collectors.toMap(id -> id,
                                                             id -> getControllerTask(hostId, id)))));
    }

    private CompletableFuture<ControllerEvent> getControllerTask(String hostId, String id) {
        return hostTaskIndex.getEntityData(hostId, id)
                          .thenApply(data -> controllerEventSerializer.fromByteBuffer(ByteBuffer.wrap(data)));
    }

    @Override
    public CompletableFuture<Void> removeHostFromTaskIndex(String hostId) {
        return hostTaskIndex.removeHost(hostId);
    }

    @Override
    public CompletableFuture<Set<String>> listHostsWithPendingTask() {
        return hostTaskIndex.getHosts();
    }

    @Override
    public CompletableFuture<Void> markCold(final String scope, final String stream, final long segmentId, final long timestamp,
                                            final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).setColdMarker(segmentId, timestamp, context), executor);
    }

    @Override
    public CompletableFuture<Boolean> isCold(final String scope, final String stream, final long segmentId,
                                             final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).getColdMarker(segmentId, context)
                .thenApply(marker -> marker != null && marker > System.currentTimeMillis()), executor);
    }

    @Override
    public CompletableFuture<Void> removeMarker(final String scope, final String stream, final long segmentId,
                                                final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).removeColdMarker(segmentId, context), executor);
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns(final String scope, final String stream,
                                                                       final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).getActiveTxns(context), executor);
    }

    @Override
    public CompletableFuture<Map<UUID, TxnStatus>> listCompletedTxns(String scope, String stream,
                                                                     OperationContext context, Executor executor) {
        OperationContext operationContext = getOperationContext(context);
        return  Futures.completeOn(getStream(scope, stream, operationContext).listCompletedTxns(operationContext), executor);
    }

    @Override
    public CompletableFuture<List<ScaleMetadata>> getScaleMetadata(final String scope, final String name, final long from, 
                                                                   final long to, final OperationContext ctx,
                                                                   final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, name, context).getScaleMetadata(from, to, context), executor);
    }

    @Override
    public CompletableFuture<EpochRecord>  getActiveEpoch(final String scope,
                                                            final String stream,
                                                            final OperationContext ctx,
                                                            final boolean ignoreCached,
                                                            final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).getActiveEpoch(ignoreCached, context), executor);
    }

    @Override
    public CompletableFuture<EpochRecord> getEpoch(final String scope,
                                                   final String stream,
                                                   final int epoch,
                                                   final OperationContext ctx,
                                                   final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).getEpochRecord(epoch, context), executor);
    }

    @Override
    public CompletableFuture<Map.Entry<VersionedMetadata<CommittingTransactionsRecord>, List<VersionedTransactionData>>> startCommitTransactions(
            String scope, String stream, int limit, OperationContext ctx, ScheduledExecutorService executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).startCommittingTransactions(limit, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getVersionedCommittingTransactionsRecord(
            String scope, String stream, OperationContext ctx, ScheduledExecutorService executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).getVersionedCommitTransactionsRecord(context), executor);
    }

    @Override
    public CompletableFuture<Void> completeCommitTransactions(String scope, String stream,
                                                              VersionedMetadata<CommittingTransactionsRecord> record,
                                                              OperationContext ctx, ScheduledExecutorService executor,
                                                              Map<String, TxnWriterMark> writerMarks) {
        OperationContext context = getOperationContext(ctx);
        Stream streamObj = getStream(scope, stream, context);
        return Futures.completeOn(streamObj.completeCommittingTransactions(record, context, writerMarks), executor)
                .thenAcceptAsync(result -> {
                    streamObj.getNumberOfOngoingTransactions(context).thenAccept(count ->
                            TransactionMetrics.reportOpenTransactions(scope, stream, count.intValue()));
                }, executor);
    }

    @Override
    public CompletableFuture<Void> createWaitingRequestIfAbsent(String scope, String stream, String processorName,
                                                                OperationContext ctx, ScheduledExecutorService executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).createWaitingRequestIfAbsent(processorName, context),
                executor);
    }

    @Override
    public CompletableFuture<String> getWaitingRequestProcessor(String scope, String stream, OperationContext ctx, 
                                                                ScheduledExecutorService executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).getWaitingRequestProcessor(context), executor);
    }

    @Override
    public CompletableFuture<Void> deleteWaitingRequestConditionally(String scope, String stream, String processorName,
                                                                     OperationContext ctx, ScheduledExecutorService executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).deleteWaitingRequestConditionally(processorName, context),
                executor);
    }

    @Override
    public CompletableFuture<WriterTimestampResponse> noteWriterMark(String scope, String stream, String writer,
                                                                     long timestamp, Map<Long, Long> position,
                                                                     OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).noteWriterMark(writer, timestamp, position, context),
                executor);
    }

    @Override
    public CompletableFuture<Void> shutdownWriter(String scope, String stream, String writer,
                                                       OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).shutdownWriter(writer, context), executor);
    }

    @Override
    public CompletableFuture<Void> removeWriter(String scope, String stream, String writer, WriterMark writerMark,
                                                OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).removeWriter(writer, writerMark, context), executor);
    }

    @Override
    public CompletableFuture<WriterMark> getWriterMark(String scope, String stream, String writer,
                                                       OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).getWriterMark(writer, context), executor);
    }

    @Override
    public CompletableFuture<Map<String, WriterMark>> getAllWriterMarks(String scope, String stream,
                                                                        OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, stream, context).getAllWriterMarks(context), executor);
    }


    @Override
    public CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(String scope, String streamName, int chunkNumber, 
                                                                          OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, streamName, context).getHistoryTimeSeriesChunk(chunkNumber, context), 
                executor);
    }

    @Override
    public CompletableFuture<SealedSegmentsMapShard> getSealedSegmentSizeMapShard(String scope, String streamName, 
                                                                                  int shardNumber, OperationContext ctx, 
                                                                                  Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, streamName, context).getSealedSegmentSizeMapShard(shardNumber, context), 
                executor);
    }

    @Override
    public CompletableFuture<Integer> getSegmentSealedEpoch(String scope, String streamName, long segmentId, 
                                                            OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, streamName, context).getSegmentSealedEpoch(segmentId, context), executor);
    }

    @Override
    public CompletableFuture<StreamCutComparison> compareStreamCut(String scope, String streamName, Map<Long, Long> streamCut1,
                                                       Map<Long, Long> streamCut2, OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, streamName, context).compareStreamCuts(streamCut1, streamCut2, context), 
                executor);
    }

    @Override
    public CompletableFuture<StreamCutReferenceRecord> findStreamCutReferenceRecordBefore(String scope, String streamName, 
                                                                                          Map<Long, Long> streamCut, 
                                                                                          final RetentionSet retentionSet, 
                                                                                          OperationContext ctx, 
                                                                                          Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getStream(scope, streamName, context).findStreamCutReferenceRecordBefore(streamCut, 
                retentionSet, context), executor);
    }

    protected Stream getStream(String scope, final String name, OperationContext context) {
        if (context instanceof StreamOperationContext) {
            return ((StreamOperationContext) context).getStream();
        } else {
            Stream stream = cache.getUnchecked(new ImmutablePair<>(scope, name));
            stream.refresh();
            return stream;
        }
    }

    @VisibleForTesting
    void setStream(Stream stream) {
        cache.put(new ImmutablePair<>(stream.getScope(), stream.getName()), stream);
    }

    public Scope getScope(final String scopeName, final OperationContext context) {
        if (context != null) {
            if (context instanceof StreamOperationContext) {
                return ((StreamOperationContext) context).getScope();
            } else if (context instanceof RGOperationContext) {
                return ((RGOperationContext) context).getScope();
            } 
        }
        Scope scope = scopeCache.getUnchecked(scopeName);
        scope.refresh();
        return scope;
    }

    @Override
    public CompletableFuture<UUID> getScopeId(final String scopeName, OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getScope(scopeName, context).getScopeId(scopeName, context), executor);
    }

    // region ReaderGroup
    @Override
    public CompletableFuture<UUID> getReaderGroupId(final String scopeName, final String rgName, 
                                                    OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getScope(scopeName, context).getReaderGroupId(rgName, context), executor);
    }

    @Override
    public CompletableFuture<Void> startRGConfigUpdate(final String scope, final String name,
                                                       final ReaderGroupConfig configuration,
                                                       final OperationContext ctx,
                                                       final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getReaderGroup(scope, name, context).startUpdateConfiguration(configuration, context), executor);
    }

    @Override
    public CompletableFuture<Void> completeRGConfigUpdate(final String scope, final String name,
                                                          final VersionedMetadata<ReaderGroupConfigRecord> existing,
                                                          final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getReaderGroup(scope, name, context).completeUpdateConfiguration(existing, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupState>> getVersionedReaderGroupState(final String scope, final String name,
                                                                                        final boolean ignoreCached,
                                                                                        final OperationContext ctx,
                                                                                        final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getReaderGroup(scope, name, context).getVersionedState(context), executor);
    }

    @Override
    public CompletableFuture<ReaderGroupState> getReaderGroupState(final String scope, final String name,
                                             final boolean ignoreCached,
                                             final OperationContext ctx,
                                             final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getReaderGroup(scope, name, context).getState(ignoreCached, context), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getReaderGroupConfigRecord(final String scope,
                                                                   final String name,
                                                                   final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getReaderGroup(scope, name, context).getVersionedConfigurationRecord(context), executor);
    }

    @Override
    public RGOperationContext createRGContext(String scope, String name, long requestId) {
        return new RGOperationContext(getScope(scope, null), 
                getReaderGroup(scope, name, null), requestId);
    }

    protected ReaderGroup getReaderGroup(String scope, final String name, OperationContext context) {
        ReaderGroup readerGroup;
        if (context instanceof RGOperationContext) {
            return ((RGOperationContext) context).getReaderGroup();
        }
        readerGroup = rgCache.getUnchecked(new ImmutablePair<>(scope, name));
        readerGroup.refresh();
        return readerGroup;
    }

    @Override
    public CompletableFuture<Void> createReaderGroup(final String scope,
                                                     final String rgName,
                                                     final ReaderGroupConfig configuration,
                                                     final long createTimestamp,
                                                     final OperationContext ctx,
                                                     final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(checkScopeExists(scope, context, executor)
                   .thenCompose(exists -> {
                   if (exists) {
                     // Create reader group may fail, if scope is deleted as we attempt to create the reader group under scope.
                     return getReaderGroup(scope, rgName, context)
                                    .create(configuration, createTimestamp, context);
                   } else {
                     return Futures.toVoid(Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope does not exist")));
                   }
                 }), executor);
    }

    @Override
    public CompletableFuture<Void> deleteReaderGroup(final String scopeName, final String rgName,
                                                     final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return getReaderGroup(scopeName, rgName, context).delete(context);
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupState>> updateReaderGroupVersionedState(
            final String scope, final String name, final ReaderGroupState state, final VersionedMetadata<ReaderGroupState> previous,
            final OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(getReaderGroup(scope, name, context).updateVersionedState(previous, state, context), executor);
    }

    //endregion

    private CompletableFuture<SimpleEntry<Long, Long>> findNumSplitsMerges(String scopeName, String streamName, 
                                                                           OperationContext ctx, Executor executor) {
            OperationContext context = getOperationContext(ctx);

            Stream stream = getStream(scopeName, streamName, context);
        return Futures.completeOn(stream.getActiveEpoch(true, context)
                                        .thenCompose(x -> stream.getSplitMergeCountsTillEpoch(x, context)), executor);
    }

    OperationContext getOperationContext(OperationContext context) {
        // if null context is supplied make sure we create a context with a new request id.
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

    /**
     * This method retrieves a safe base segment number from which a stream's segment ids may start. In the case of a
     * new stream, this method will return 0 as a starting segment number (default). In the case that a stream with the
     * same name has been recently deleted, this method will provide as a safe starting segment number the last segment
     * number of the previously deleted stream + 1. This will avoid potential segment naming collisions with segments
     * being asynchronously deleted from the segment store.
     *
     * @param scopeName scope
     * @param streamName stream
     * @return CompletableFuture with a safe starting segment number for this stream.
     */
    abstract CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName, 
                                                                        OperationContext ctx, Executor executor);

    /**
     * This method stores the last active segment for a stream upon its deletion. Persistently storing this value is
     * necessary in the case of a stream re-creation for picking an appropriate starting segment number.
     *
     * @param scope scope
     * @param stream stream
     * @param lastActiveSegment segment with highest number for a stream
     * @param ctx context
     * @param executor executor
     * @return CompletableFuture which indicates the task completion related to persistently store lastActiveSegment.
     */
    abstract CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, int lastActiveSegment,
                                                             OperationContext ctx, final Executor executor);

    abstract Stream newStream(final String scopeName, final String name);

    abstract CompletableFuture<Int96> getNextCounter();

    private String getTxnResourceString(TxnResource txn) {
        return txn.toString(RESOURCE_PART_SEPARATOR);
    }

    private TxnResource getTxnResource(String str) {
        return TxnResource.parse(str, RESOURCE_PART_SEPARATOR);
    }
    
    String getScopedStreamName(String scope, String stream) {
        return String.format("%s/%s", scope, stream);
    }

    abstract Version getEmptyVersion();

    abstract Version parseVersionData(byte[] data);

    // region reader group
    abstract ReaderGroup newReaderGroup(final String scope, final String name);

    //endregion
}

