/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.store.SegmentRecord;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.ScaleMetadata;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleStatusResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.AddSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.RemoveSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteKVTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.SubscribersResponse;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.shared.NameUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Stream controller RPC server implementation.
 */
@Getter
@AllArgsConstructor
@Slf4j
public class ControllerService {
    private final KVTableMetadataStore kvtMetadataStore;
    private final TableMetadataTasks kvtMetadataTasks;
    private final StreamMetadataStore streamStore;
    private final BucketStore bucketStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final SegmentHelper segmentHelper;
    private final Executor executor;
    private final Cluster cluster;

    public CompletableFuture<List<NodeUri>> getControllerServerList() {
        if (cluster == null) {
            return Futures.failedFuture(new IllegalStateException("Controller cluster not initialized"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                return cluster.getClusterMembers().stream()
                        .map(host -> NodeUri.newBuilder().setEndpoint(host.getIpAddr()).setPort(host.getPort()).build())
                        .collect(Collectors.toList());
            } catch (ClusterException e) {
                // cluster implementation throws checked exceptions which cannot be thrown inside completable futures.
                throw Exceptions.sneakyThrow(e);
            }
        }, executor);
    }

    public CompletableFuture<CreateKeyValueTableStatus> createKeyValueTable(String scope, String kvtName,
                                                                            final KeyValueTableConfiguration kvtConfig,
                                                                            final long createTimestamp) {
        Preconditions.checkNotNull(kvtConfig, "kvTableConfig");
        Preconditions.checkArgument(createTimestamp >= 0);
        Preconditions.checkArgument(kvtConfig.getPartitionCount() > 0);
        Timer timer = new Timer();
        try {
            NameUtils.validateUserKeyValueTableName(kvtName);
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn("Create KeyValueTable failed due to invalid name {}", kvtName);
            return CompletableFuture.completedFuture(
                    CreateKeyValueTableStatus.newBuilder().setStatus(CreateKeyValueTableStatus.Status.INVALID_TABLE_NAME).build());
        }
        return kvtMetadataTasks.createKeyValueTable(scope, kvtName, kvtConfig, createTimestamp)
                .thenApplyAsync(status -> {
                    reportCreateKVTableMetrics(scope, kvtName, kvtConfig.getPartitionCount(), status, timer.getElapsed());
                    return CreateKeyValueTableStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    public CompletableFuture<List<SegmentRange>> getCurrentSegmentsKeyValueTable(final String scope, final String kvtName) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(kvtName, "KeyValueTable");

        // Fetch active segments from segment store.
        return kvtMetadataStore.getActiveSegments(scope, kvtName, null, executor)
                .thenApplyAsync(activeSegments -> getSegmentRanges(activeSegments, scope, kvtName), executor);
    }

    /**
     * List existing KeyValueTables in specified scope.
     *
     * @param scope Name of the scope.
     * @param token continuation token
     * @param limit limit for number of KeyValueTables to return.
     * @return List of KeyValueTables in scope.
     */
    public CompletableFuture<Pair<List<String>, String>> listKeyValueTables(final String scope, final String token, final int limit) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        return kvtMetadataStore.listKeyValueTables(scope, token, limit, executor);
    }


    public CompletableFuture<DeleteKVTableStatus> deleteKeyValueTable(final String scope, final String kvtName) {
        Exceptions.checkNotNullOrEmpty(scope, "Scope Name");
        Exceptions.checkNotNullOrEmpty(kvtName, "KeyValueTable Name");
        Timer timer = new Timer();
        return kvtMetadataTasks.deleteKeyValueTable(scope, kvtName)
                .thenApplyAsync(status -> {
                    reportDeleteKVTableMetrics(scope, kvtName, status, timer.getElapsed());
                    return DeleteKVTableStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    public CompletableFuture<AddSubscriberStatus> addSubscriber(String scope, String stream, final String subscriber) {
        Preconditions.checkNotNull(scope, "scopeName is null");
        Preconditions.checkNotNull(stream, "streamName is null");
        Preconditions.checkNotNull(subscriber, "subscriber is null");
        Timer timer = new Timer();
        return streamMetadataTasks.addSubscriber(scope, stream, subscriber, null)
                .thenApplyAsync(status -> {
                    reportAddSubscriberMetrics(scope, stream, status, timer.getElapsed());
                    return AddSubscriberStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    public CompletableFuture<SubscribersResponse> listSubscribers(String scope, String stream) {
        Preconditions.checkNotNull(scope, "scopeName is null");
        Preconditions.checkNotNull(stream, "streamName is null");
        return streamMetadataTasks.listSubscribers(scope, stream, null);

    }

    public CompletableFuture<RemoveSubscriberStatus> removeSubscriber(String scope, String stream, final String subscriber) {
        Preconditions.checkNotNull(scope, "scopeName is null");
        Preconditions.checkNotNull(stream, "streamName is null");
        Preconditions.checkNotNull(subscriber, "subscriber is null");
        Timer timer = new Timer();
        return streamMetadataTasks.removeSubscriber(scope, stream, subscriber, null)
                .thenApplyAsync(status -> {
                    reportRemoveSubscriberMetrics(scope, stream, status, timer.getElapsed());
                    return RemoveSubscriberStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    public CompletableFuture<UpdateSubscriberStatus> updateTruncationStreamCut(String scope, String stream,
                                                                                 final String subscriber,
                                                                                 final ImmutableMap<Long, Long> truncationStreamCut) {
        Preconditions.checkNotNull(scope, "scopeName is null");
        Preconditions.checkNotNull(stream, "streamName is null");
        Preconditions.checkNotNull(subscriber, "subscriber is null");
        Preconditions.checkNotNull(truncationStreamCut, "Truncation StreamCut is null");
        Timer timer = new Timer();
        return streamMetadataTasks.updateSubscriberStreamCut(scope, stream, subscriber, truncationStreamCut, null)
                            .thenApplyAsync(status -> {
                                reportUpdateTruncationSCMetrics(scope, stream, status, timer.getElapsed());
                                return UpdateSubscriberStatus.newBuilder().setStatus(status).build();
                            }, executor);
    }

    public CompletableFuture<CreateStreamStatus> createStream(String scope, String stream, final StreamConfiguration streamConfig,
            final long createTimestamp) {
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        Preconditions.checkArgument(createTimestamp >= 0);
        Timer timer = new Timer();
        try {
            NameUtils.validateStreamName(stream);
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn("Create stream failed due to invalid stream name {}", stream);
            return CompletableFuture.completedFuture(
                    CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.INVALID_STREAM_NAME).build());
        }

        return Futures.exceptionallyExpecting(streamStore.getState(scope, stream, true, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, State.UNKNOWN)
                      .thenCompose(state -> {
                          if (state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                              return streamMetadataTasks.createStreamRetryOnLockFailure(scope,
                                      stream,
                                      streamConfig,
                                      createTimestamp, 10).thenApplyAsync(status -> {
                                  reportCreateStreamMetrics(scope, stream, streamConfig.getScalingPolicy().getMinNumSegments(), status,
                                          timer.getElapsed());
                                  return CreateStreamStatus.newBuilder().setStatus(status).build();
                              }, executor);
                          } else {
                              return CompletableFuture.completedFuture(
                                      CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.STREAM_EXISTS).build());
                          }
                      });

    }

    public CompletableFuture<UpdateStreamStatus> updateStream(String scope, String stream, final StreamConfiguration streamConfig) {
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        Timer timer = new Timer();
        return streamMetadataTasks.updateStream(scope, stream, streamConfig, null)
                  .thenApplyAsync(status -> {
                      reportUpdateStreamMetrics(scope, stream, status, timer.getElapsed());
                      return UpdateStreamStatus.newBuilder().setStatus(status).build();
                  }, executor);
    }

    public CompletableFuture<UpdateStreamStatus> truncateStream(final String scope, final String stream,
                                                                final Map<Long, Long> streamCut) {
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(streamCut, "streamCut");
        Timer timer = new Timer();
        return streamMetadataTasks.truncateStream(scope, stream, streamCut, null)
                .thenApplyAsync(status -> {
                    reportTruncateStreamMetrics(scope, stream, status, timer.getElapsed());
                    return UpdateStreamStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    public CompletableFuture<StreamConfiguration> getStream(final String scopeName, final String streamName) {
        return streamStore.getConfiguration(scopeName, streamName, null, executor);
    }

    public CompletableFuture<UpdateStreamStatus> sealStream(final String scope, final String stream) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Timer timer = new Timer();
        return streamMetadataTasks.sealStream(scope, stream, null)
                .thenApplyAsync(status -> {
                    reportSealStreamMetrics(scope, stream, status, timer.getElapsed());
                    return UpdateStreamStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    public CompletableFuture<DeleteStreamStatus> deleteStream(final String scope, final String stream) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Timer timer = new Timer();
        return streamMetadataTasks.deleteStream(scope, stream, null)
                .thenApplyAsync(status -> {
                    reportDeleteStreamMetrics(scope, stream, status, timer.getElapsed());
                    return DeleteStreamStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    public CompletableFuture<List<SegmentRange>> getCurrentSegments(final String scope, final String stream) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");

        // Fetch active segments from segment store.
        return streamStore.getActiveSegments(scope, stream, null, executor)
                .thenApplyAsync(activeSegments -> getSegmentRanges(activeSegments, scope, stream), executor);
    }

    public CompletableFuture<List<SegmentRange>> getEpochSegments(final String scope, final String stream, int epoch) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Exceptions.checkArgument(epoch >= 0, "epoch", "Epoch cannot be less than 0");
        OperationContext context = streamStore.createContext(scope, stream);
        return streamStore.getEpoch(scope, stream, epoch, context, executor)
                          .thenApplyAsync(epochRecord -> getSegmentRanges(epochRecord.getSegments(), scope, stream), executor);
    }

    public CompletableFuture<Map<SegmentId, Long>> getSegmentsAtHead(final String scope, final String stream) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");

        // First fetch segments active at specified timestamp from the specified stream.
        // Divide current segments in segmentFutures into at most count positions.
        return streamStore.getSegmentsAtHead(scope, stream, null, executor).thenApply(segments -> {
            return segments.entrySet().stream()
                           .collect(Collectors.toMap(entry -> ModelHelper.createSegmentId(scope, stream, entry.getKey().segmentId()),
                                   Map.Entry::getValue));
        });
    }

    public CompletableFuture<Map<SegmentRange, List<Long>>> getSegmentsImmediatelyFollowing(SegmentId segment) {
        Preconditions.checkNotNull(segment, "segment");
        OperationContext context = streamStore.createContext(segment.getStreamInfo().getScope(), segment
                .getStreamInfo().getStream());
        return streamStore.getSuccessors(segment.getStreamInfo().getScope(),
                segment.getStreamInfo().getStream(),
                segment.getSegmentId(),
                context,
                executor)
                .thenApply(successors -> successors.entrySet().stream()
                        .collect(Collectors.toMap(
                                entry -> ModelHelper.createSegmentRange(segment.getStreamInfo().getScope(),
                                                segment.getStreamInfo().getStream(), entry.getKey().segmentId(),
                                                entry.getKey().getKeyStart(),
                                                entry.getKey().getKeyEnd()),
                                Map.Entry::getValue)));
    }

    public CompletableFuture<List<StreamSegmentRecord>> getSegmentsBetweenStreamCuts(Controller.StreamCutRange range) {
        Preconditions.checkNotNull(range, "segment");
        Preconditions.checkArgument(!(range.getFromMap().isEmpty() && range.getToMap().isEmpty()));

        String scope = range.getStreamInfo().getScope();
        String stream = range.getStreamInfo().getStream();
        OperationContext context = streamStore.createContext(scope, stream);
        return streamStore.getSegmentsBetweenStreamCuts(scope,
                stream,
                range.getFromMap(),
                range.getToMap(),
                context,
                executor);
    }

    public CompletableFuture<ScaleResponse> scale(final String scope,
                                                  final String stream,
                                                  final List<Long> segmentsToSeal,
                                                  final Map<Double, Double> newKeyRanges,
                                                  final long scaleTimestamp) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(segmentsToSeal, "sealedSegments");
        Preconditions.checkNotNull(newKeyRanges, "newKeyRanges");

        return streamMetadataTasks.manualScale(scope,
                                         stream,
                                         segmentsToSeal,
                                         new ArrayList<>(ModelHelper.encode(newKeyRanges)),
                                         scaleTimestamp,
                                         null);
    }

    public CompletableFuture<ScaleStatusResponse> checkScale(final String scope, final String stream, final int epoch) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Exceptions.checkArgument(epoch >= 0, "epoch", "Epoch cannot be less than 0");

        return streamMetadataTasks.checkScale(scope, stream, epoch, null);
    }

    public CompletableFuture<List<ScaleMetadata>> getScaleRecords(final String scope, final String stream, final long from, final long to) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return streamStore.getScaleMetadata(scope, stream, from, to, null, executor);
    }

    public CompletableFuture<NodeUri> getURI(final SegmentId segment) {
        Preconditions.checkNotNull(segment, "segment");

        return CompletableFuture.completedFuture(
                segmentHelper.getSegmentUri(segment.getStreamInfo().getScope(), segment.getStreamInfo().getStream(),
                        segment.getSegmentId())
        );
    }

    private SegmentRange convert(final String scope,
                                 final String stream,
                                 final SegmentRecord segment) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(segment, "segment");
        return ModelHelper.createSegmentRange(
                scope, stream, segment.segmentId(), segment.getKeyStart(), segment.getKeyEnd());
    }

    public CompletableFuture<Boolean> isSegmentValid(final String scope,
                                                     final String stream,
                                                     final long segmentId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return streamStore.getActiveSegments(scope, stream, null, executor)
                .thenApplyAsync(x -> x.stream().anyMatch(z -> z.segmentId() == segmentId), executor);
    }

    public CompletableFuture<Boolean> isStreamCutValid(final String scope,
                                                     final String stream,
                                                     final Map<Long, Long> streamCut) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return streamStore.isStreamCutValid(scope, stream, streamCut, null, executor);
    }

    @SuppressWarnings("ReturnCount")
    public CompletableFuture<Pair<UUID, List<SegmentRange>>> createTransaction(final String scope, final String stream,
                                                                               final long lease) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Timer timer = new Timer();
        return streamTransactionMetadataTasks.createTxn(scope, stream, lease, null)
                .thenApply(pair -> {
                    VersionedTransactionData data = pair.getKey();
                    List<StreamSegmentRecord> segments = pair.getValue();
                    return new ImmutablePair<>(data.getId(), getSegmentRanges(segments, scope, stream));
                }).handle((result, ex) -> {
                    if (ex != null) {
                        TransactionMetrics.getInstance().createTransactionFailed(scope, stream);
                        throw new CompletionException(ex);
                    }
                    TransactionMetrics.getInstance().createTransaction(scope, stream, timer.getElapsed());
                    return result;
                });
    }

    private List<SegmentRange> getSegmentRanges(List<? extends SegmentRecord> activeSegments, String scope, String stream) {
        List<SegmentRange> listOfSegment = activeSegments
                .stream()
                .map(segment -> convert(scope, stream, segment))
                .collect(Collectors.toList());
        listOfSegment.sort(Comparator.comparingDouble(SegmentRange::getMinKey));
        return listOfSegment;
    }

    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final UUID txId,
                                                          final String writerId, final long timestamp) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txId, "txnId");
        Timer timer = new Timer();
        return streamTransactionMetadataTasks.commitTxn(scope, stream, txId, writerId, timestamp, null)
                .handle((ok, ex) -> {
                    if (ex != null) {
                        log.warn("Transaction commit failed", ex);
                        Throwable unwrap = getRealException(ex);
                        if (unwrap instanceof RetryableException) {
                            // if its a retryable exception (it could be either write conflict or store exception)
                            // let it be thrown and translated to appropriate error code so that the client
                            // retries upon failure.
                            throw new CompletionException(unwrap);
                        }
                        TransactionMetrics.getInstance().commitTransactionFailed(scope, stream, txId.toString());
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.FAILURE).build();
                    } else {
                        TransactionMetrics.getInstance().committingTransaction(timer.getElapsed());
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build();
                    }
                });
    }

    private Throwable getRealException(Throwable ex) {
        Throwable unwrap = Exceptions.unwrap(ex);
        if (unwrap instanceof RetriesExhaustedException) {
            unwrap = Exceptions.unwrap(unwrap.getCause());
        }
        return unwrap;
    }

    public CompletableFuture<TxnStatus> abortTransaction(final String scope, final String stream, final UUID txId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txId, "txnId");
        Timer timer = new Timer();
        return streamTransactionMetadataTasks.abortTxn(scope, stream, txId, null, null)
                .handle((ok, ex) -> {
                    if (ex != null) {
                        log.warn("Transaction abort failed", ex);
                        Throwable unwrap = getRealException(ex);
                        if (unwrap instanceof RetryableException) {
                            // if its a retryable exception (it could be either write conflict or store exception)
                            // let it be thrown and translated to appropriate error code so that the client
                            // retries upon failure.
                            throw new CompletionException(unwrap);
                        }
                        TransactionMetrics.getInstance().abortTransactionFailed(scope, stream, txId.toString());
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.FAILURE).build();
                    } else {
                        TransactionMetrics.getInstance().abortingTransaction(timer.getElapsed());
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build();
                    }
                });
    }

    public CompletableFuture<PingTxnStatus> pingTransaction(final String scope,
                                                            final String stream,
                                                            final UUID txId,
                                                            final long lease) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txId, "txnId");

        return streamTransactionMetadataTasks.pingTxn(scope, stream, txId, lease, null);
    }

    public CompletableFuture<TxnState> checkTransactionStatus(final String scope, final String stream,
            final UUID txnId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txnId, "txnId");
        return streamStore.transactionStatus(scope, stream, txnId, null, executor)
                .thenApplyAsync(res -> TxnState.newBuilder().setState(TxnState.State.valueOf(res.name())).build(), executor);
    }

    /**
     * Controller Service API to create scope.
     *
     * @param scope Name of scope to be created.
     * @return Status of create scope.
     */
    public CompletableFuture<CreateScopeStatus> createScope(final String scope) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Timer timer = new Timer();
        try {
            NameUtils.validateScopeName(scope);
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn("Create scope failed due to invalid scope name {}", scope);
            return CompletableFuture.completedFuture(CreateScopeStatus.newBuilder().setStatus(
                    CreateScopeStatus.Status.INVALID_SCOPE_NAME).build());
        }
        return streamStore.createScope(scope).thenApply(r -> reportCreateScopeMetrics(scope, r, timer.getElapsed()));
    }

    /**
     * Controller Service API to delete scope.
     *
     * @param scope Name of scope to be deleted.
     * @return Status of delete scope.
     */
    public CompletableFuture<DeleteScopeStatus> deleteScope(final String scope) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Timer timer = new Timer();
        return streamStore.deleteScope(scope).thenApply(r -> reportDeleteScopeMetrics(scope, r, timer.getElapsed()));
    }

    /**
     * List existing streams in scopes.
     *
     * @param scope Name of the scope.
     * @return List of streams in scope.
     */
    public CompletableFuture<Map<String, StreamConfiguration>> listStreamsInScope(final String scope) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        return streamStore.listStreamsInScope(scope);
    }

    /**
     * List existing streams in scopes.
     *
     * @param scope Name of the scope.
     * @param token continuation token
     * @param limit limit for number of streams to return.
     * @return List of streams in scope.
     */
    public CompletableFuture<Pair<List<String>, String>> listStreams(final String scope, final String token, final int limit) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        return streamStore.listStream(scope, token, limit, executor);
    }

    /**
     * List Scopes in cluster.
     *
     * @return List of scopes.
     */
    public CompletableFuture<List<String>> listScopes() {
        return streamStore.listScopes();
    }

    /**
     * List Scopes in cluster from continuation token and limit it to number of elements specified by limit parameter.
     *
     * @param token continuation token
     * @param limit number of elements to return.
     * @return List of scopes.
     */
    public CompletableFuture<Pair<List<String>, String>> listScopes(final String token, final int limit) {
        return streamStore.listScopes(token, limit, executor);
    }

    /**
     * Retrieve a scope.
     *
     * @param scopeName Name of Scope.
     * @return Scope if it exists.
     */
    public CompletableFuture<String> getScope(final String scopeName) {
        Preconditions.checkNotNull(scopeName);
        return streamStore.getScopeConfiguration(scopeName);
    }

    // Metrics reporting region
    private void reportCreateKVTableMetrics(String scope, String kvtName, int initialSegments, CreateKeyValueTableStatus.Status status,
                                           Duration latency) {
        if (status.equals(CreateKeyValueTableStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().createKeyValueTable(scope, kvtName, initialSegments, latency);
        } else if (status.equals(CreateKeyValueTableStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().createKeyValueTableFailed(scope, kvtName);
        }
    }

    private void reportDeleteKVTableMetrics(String scope, String kvtName, DeleteKVTableStatus.Status status, Duration latency) {
        if (status.equals(DeleteKVTableStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().deleteKeyValueTable(scope, kvtName, latency);
        } else if (status.equals(DeleteKVTableStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().deleteKeyValueTableFailed(scope, kvtName);
        }
    }

    private void reportCreateStreamMetrics(String scope, String streamName, int initialSegments, CreateStreamStatus.Status status,
                                           Duration latency) {
        if (status.equals(CreateStreamStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().createStream(scope, streamName, initialSegments, latency);
        } else if (status.equals(CreateStreamStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().createStreamFailed(scope, streamName);
        }
    }

    private CreateScopeStatus reportCreateScopeMetrics(String scope, CreateScopeStatus status, Duration latency) {
        if (status.getStatus().equals(CreateScopeStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().createScope(latency);
        } else if (status.getStatus().equals(CreateScopeStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().createScopeFailed(scope);
        }
        return status;
    }

    private void reportUpdateStreamMetrics(String scope, String streamName, UpdateStreamStatus.Status status, Duration latency) {
        if (status.equals(UpdateStreamStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().updateStream(scope, streamName, latency);
        } else if (status.equals(UpdateStreamStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().updateStreamFailed(scope, streamName);
        }
    }

    private void reportAddSubscriberMetrics(String scope, String streamName, AddSubscriberStatus.Status status, Duration latency) {
        if (status.equals(AddSubscriberStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().addSubscriber(scope, streamName, latency);
        } else if (status.equals(AddSubscriberStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().addSubscriberFailed(scope, streamName);
        }
    }

    private void reportRemoveSubscriberMetrics(String scope, String streamName, RemoveSubscriberStatus.Status status, Duration latency) {
        if (status.equals(RemoveSubscriberStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().removeSubscriber(scope, streamName, latency);
        } else if (status.equals(RemoveSubscriberStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().removeSubscriberFailed(scope, streamName);
        }
    }

    private void reportUpdateTruncationSCMetrics(String scope, String streamName, UpdateSubscriberStatus.Status status, Duration latency) {
        if (status.equals(UpdateSubscriberStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().updateTruncationSC(scope, streamName, latency);
        } else if (status.equals(UpdateSubscriberStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().updateTruncationSCFailed(scope, streamName);
        }
    }

    private void reportTruncateStreamMetrics(String scope, String streamName, UpdateStreamStatus.Status status, Duration latency) {
        if (status.equals(UpdateStreamStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().truncateStream(scope, streamName, latency);
        } else if (status.equals(UpdateStreamStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().truncateStreamFailed(scope, streamName);
        }
    }

    private void reportSealStreamMetrics(String scope, String streamName, UpdateStreamStatus.Status status, Duration latency) {
        if (status.equals(UpdateStreamStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().sealStream(scope, streamName, latency);
        } else if (status.equals(UpdateStreamStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().sealStreamFailed(scope, streamName);
        }
    }

    private void reportDeleteStreamMetrics(String scope, String streamName, DeleteStreamStatus.Status status, Duration latency) {
        if (status.equals(DeleteStreamStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().deleteStream(scope, streamName, latency);
        } else if (status.equals(DeleteStreamStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().deleteStreamFailed(scope, streamName);
        }
    }

    private DeleteScopeStatus reportDeleteScopeMetrics(String scope, DeleteScopeStatus status, Duration latency) {
        if (status.getStatus().equals(DeleteScopeStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().deleteScope(latency);
        } else if (status.getStatus().equals(DeleteScopeStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().deleteScopeFailed(scope);
        }
        return status;
    }

    public CompletableFuture<Controller.TimestampResponse> noteTimestampFromWriter(String scope, String stream, String writerId, long timestamp, Map<Long, Long> streamCut) {
        return bucketStore.addStreamToBucketStore(BucketStore.ServiceType.WatermarkingService, scope, stream, executor)
                          .thenCompose(v -> streamStore.noteWriterMark(scope, stream, writerId, timestamp, streamCut, null, executor))
                .thenApply(r -> {
                        Controller.TimestampResponse.Builder response = Controller.TimestampResponse.newBuilder();
                        switch (r) {
                            case SUCCESS:
                                response.setResult(Controller.TimestampResponse.Status.SUCCESS);
                                break;
                            case INVALID_TIME:
                                response.setResult(Controller.TimestampResponse.Status.INVALID_TIME);
                                break;
                            case INVALID_POSITION:
                                response.setResult(Controller.TimestampResponse.Status.INVALID_POSITION);
                                break;
                            default:
                                response.setResult(Controller.TimestampResponse.Status.INTERNAL_ERROR);
                                break;
                        }
                        return response.build();
                });
    }

    public CompletableFuture<Controller.RemoveWriterResponse> removeWriter(String scope, String stream, String writer) {
        return streamStore.shutdownWriter(scope, stream, writer, null, executor)
                .handle((r, e) -> {
                    Controller.RemoveWriterResponse.Builder response = Controller.RemoveWriterResponse.newBuilder();
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                            response.setResult(Controller.RemoveWriterResponse.Status.UNKNOWN_WRITER);
                        } else {
                            response.setResult(Controller.RemoveWriterResponse.Status.INTERNAL_ERROR);
                        }
                    } else {
                        response.setResult(Controller.RemoveWriterResponse.Status.SUCCESS);
                    }
                    return response.build();
                });
    }

    // End metrics reporting region
}
