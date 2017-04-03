/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.VersionedTransactionData;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.timeout.TimeoutService;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Stream controller RPC server implementation.
 */
@Getter
@AllArgsConstructor
public class ControllerService {

    private final StreamMetadataStore streamStore;
    private final HostControllerStore hostStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final TimeoutService timeoutService;
    private final SegmentHelper segmentHelper;
    private final Executor executor;
    private final Cluster cluster;

    public CompletableFuture<List<NodeUri>> getControllerServerList() {
        if (cluster == null) {
            return FutureHelpers.failedFuture(new IllegalStateException("Controller cluster not initialized"));
        }

        // TODO: This implementation will be updated once PR - https://github.com/pravega/pravega/pull/804 is complete.
        return CompletableFuture.supplyAsync(() -> {
            try {
                return cluster.getClusterMembers().stream()
                        .map(host -> NodeUri.newBuilder().setEndpoint(host.getIpAddr()).setPort(host.getPort()).build())
                        .collect(Collectors.toList());
            } catch (Exception e) {
                // cluster implementation throws checked exceptions which cannot be
                throw new CompletionException(e);
            }
        }, executor);
    }

    public CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig,
            final long createTimestamp) {
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        Preconditions.checkArgument(createTimestamp >= 0);
        return streamMetadataTasks.createStream(streamConfig.getScope(),
                                                streamConfig.getStreamName(),
                                                streamConfig,
                                                createTimestamp)
                .thenApplyAsync(status -> CreateStreamStatus.newBuilder().setStatus(status).build(), executor);
    }

    public CompletableFuture<UpdateStreamStatus> alterStream(final StreamConfiguration streamConfig) {
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        return streamMetadataTasks.alterStream(
                streamConfig.getScope(), streamConfig.getStreamName(), streamConfig, null)
                .thenApplyAsync(status -> UpdateStreamStatus.newBuilder().setStatus(status).build(), executor);
    }

    public CompletableFuture<StreamConfiguration> getStream(final String scopeName, final String streamName) {
        return streamStore.getConfiguration(scopeName, streamName, null, executor);
    }

    public CompletableFuture<UpdateStreamStatus> sealStream(final String scope, final String stream) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return streamMetadataTasks.sealStream(scope, stream, null)
                .thenApplyAsync(status -> UpdateStreamStatus.newBuilder().setStatus(status).build(), executor);
    }

    public CompletableFuture<DeleteStreamStatus> deleteStream(final String scope, final String stream) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return streamMetadataTasks.deleteStream(scope, stream, null)
                .thenApplyAsync(status -> DeleteStreamStatus.newBuilder().setStatus(status).build(), executor);
    }

    public CompletableFuture<List<SegmentRange>> getCurrentSegments(final String scope, final String stream) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");

        // Fetch active segments from segment store.
        return streamStore.getActiveSegments(scope, stream, null, executor)
                .thenApplyAsync(activeSegments -> getSegmentRanges(activeSegments, scope, stream), executor);
    }

    public CompletableFuture<Map<SegmentId, Long>> getSegmentsAtTime(final String scope, final String stream, final long timestamp) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");

        // First fetch segments active at specified timestamp from the specified stream.
        // Divide current segments in segmentFutures into at most count positions.
        return streamStore.getActiveSegments(scope, stream, timestamp, null, executor).thenApply(segments -> {
            return segments.stream()
                           .map(number -> ModelHelper.createSegmentId(scope, stream, number))
                           .collect(Collectors.toMap(id -> id, id -> 0L));
            //TODO: Implement https://github.com/pravega/pravega/issues/191  (Which will supply a value besides 0)
        });
    }

    public CompletableFuture<Map<SegmentId, List<Integer>>> getSegmentsImmediatlyFollowing(SegmentId segment) {
        Preconditions.checkNotNull(segment, "segment");
        return streamStore.getSuccessors(segment.getStreamInfo().getScope(),
                                         segment.getStreamInfo().getStream(),
                                         segment.getSegmentNumber(),
                                         null,
                                         executor)
                .thenApplyAsync(successors -> successors.entrySet().stream().collect(
                        Collectors.toMap(
                                entry -> ModelHelper.createSegmentId(segment.getStreamInfo().getScope(),
                                                                     segment.getStreamInfo().getStream(),
                                                                     entry.getKey()), Map.Entry::getValue)), executor);
    }

    public CompletableFuture<ScaleResponse> scale(final String scope,
                                                  final String stream,
                                                  final List<Integer> sealedSegments,
                                                  final Map<Double, Double> newKeyRanges,
                                                  final long scaleTimestamp) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(sealedSegments, "sealedSegments");
        Preconditions.checkNotNull(newKeyRanges, "newKeyRanges");

        return streamMetadataTasks.scale(scope,
                                         stream,
                                         new ArrayList<>(sealedSegments),
                                         new ArrayList<>(ModelHelper.encode(newKeyRanges)),
                                         scaleTimestamp,
                                         null);
    }

    public CompletableFuture<NodeUri> getURI(final SegmentId segment) {
        Preconditions.checkNotNull(segment, "segment");
        return CompletableFuture.completedFuture(
                segmentHelper.getSegmentUri(segment.getStreamInfo().getScope(), segment.getStreamInfo().getStream(),
                                            segment.getSegmentNumber(), hostStore)
        );
    }

    private SegmentRange convert(final String scope,
                                 final String stream,
                                 final Segment segment) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(segment, "segment");
        return ModelHelper.createSegmentRange(
                scope, stream, segment.getNumber(), segment.getKeyStart(), segment.getKeyEnd());
    }

    public CompletableFuture<Boolean> isSegmentValid(final String scope,
                                                     final String stream,
                                                     final int segmentNumber) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return streamStore.getActiveSegments(scope, stream, null, executor)
                .thenApplyAsync(x -> x.stream().anyMatch(z -> z.getNumber() == segmentNumber), executor);
    }

    public CompletableFuture<Pair<UUID, List<SegmentRange>>> createTransaction(final String scope, final String stream,
                                                                               final long lease,
                                                                               final long maxExecutionTime,
                                                                               final long scaleGracePeriod) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");

        // If scaleGracePeriod is larger than maxScaleGracePeriod return error
        if (scaleGracePeriod > timeoutService.getMaxScaleGracePeriod()) {
            return FutureHelpers.failedFuture(new IllegalArgumentException("scaleGracePeriod too large, max value is "
                                                                            + timeoutService.getMaxScaleGracePeriod()));
        }

        // If lease value is too large return error
        if (lease > scaleGracePeriod || lease > maxExecutionTime || lease > timeoutService.getMaxLeaseValue()) {
            return FutureHelpers.failedFuture(new IllegalArgumentException("lease value too large, max value is "
            + Math.min(scaleGracePeriod, Math.min(maxExecutionTime, timeoutService.getMaxLeaseValue()))));
        }

        return streamTransactionMetadataTasks.createTxn(scope, stream, lease, maxExecutionTime, scaleGracePeriod, null)
                .thenApply(pair -> {
                    VersionedTransactionData data = pair.getKey();
                    timeoutService.addTxn(scope, stream, data.getId(), data.getVersion(), lease,
                            data.getMaxExecutionExpiryTime(), data.getScaleGracePeriod());
                    return new ImmutablePair<>(data.getId(), getSegmentRanges(pair.getValue(), scope, stream));
                });
    }

    private List<SegmentRange> getSegmentRanges(List<Segment> activeSegments, String scope, String stream) {
        List<SegmentRange> listOfSegment = activeSegments
                .stream()
                .map(segment -> convert(scope, stream, segment))
                .collect(Collectors.toList());
        listOfSegment.sort(Comparator.comparingDouble(SegmentRange::getMinKey));
        return listOfSegment;
    }

    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final TxnId
            txnId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txnId, "txnId");

        UUID txId = ModelHelper.encode(txnId);
        return streamTransactionMetadataTasks.commitTxn(scope, stream, txId, null)
                .handle((ok, ex) -> {
                    if (ex != null) {
                        // TODO: return appropriate failures to user.
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.FAILURE).build();
                    } else {
                        timeoutService.removeTxn(scope, stream, txId);
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build();
                    }
                });
    }

    public CompletableFuture<TxnStatus> abortTransaction(final String scope, final String stream, final TxnId txnId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txnId, "txnId");
        UUID txId = ModelHelper.encode(txnId);
        return streamTransactionMetadataTasks.abortTxn(scope, stream, txId, Optional.<Integer>empty(), null)
                .handle((ok, ex) -> {
                    if (ex != null) {
                        // TODO: return appropriate failures to user.
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.FAILURE).build();
                    } else {
                        timeoutService.removeTxn(scope, stream, txId);
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build();
                    }
                });
    }

    public CompletableFuture<PingTxnStatus> pingTransaction(final String scope, final String stream, final TxnId txnId,
                                                         final long lease) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txnId, "txnId");
        UUID txId = ModelHelper.encode(txnId);

        if (!timeoutService.isRunning()) {
            return CompletableFuture.completedFuture(
                    PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.DISCONNECTED).build());
        }

        if (timeoutService.containsTxn(scope, stream, txId)) {
            // If timeout service knows about this transaction, try to increase its lease.
            PingTxnStatus status = timeoutService.pingTxn(scope, stream, txId, lease);

            return CompletableFuture.completedFuture(status);
        }
        // Otherwise, first check whether lease value is within necessary bounds, and then
        // start owning the transaction timeout management by updating the txn node data in the store,
        // thus updating its version.
        // Pass this transaction metadata along with its version to timeout service, and ask timeout service to
        // start managing timeout for this transaction.
        return streamStore.getTransactionData(scope, stream, txId, null, executor)
                .thenApply(txnData -> {
                    // sanity check for lease value
                    if (lease > txnData.getScaleGracePeriod() || lease > timeoutService.getMaxLeaseValue()) {
                        return PingTxnStatus.Status.LEASE_TOO_LARGE;
                    } else if (lease + System.currentTimeMillis() > txnData.getMaxExecutionExpiryTime()) {
                        return PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED;
                    } else {
                        return PingTxnStatus.Status.OK;
                    }
                })
                .thenCompose(status -> {
                    if (status == PingTxnStatus.Status.OK) {
                        // If lease value if within necessary bounds, update the transaction node data, thus
                        // updating its version.
                        return streamTransactionMetadataTasks.pingTxn(scope, stream, txId, lease, null)
                                .thenApply(data -> {
                                    // Let timeout service start managing timeout for the transaction.
                                    timeoutService.addTxn(scope, stream, txId, data.getVersion(), lease,
                                            data.getMaxExecutionExpiryTime(), data.getScaleGracePeriod());

                                    return PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.OK).build();
                                });
                    } else {
                        return CompletableFuture.completedFuture(
                                PingTxnStatus.newBuilder().setStatus(status).build());
                    }
                });
    }

    public CompletableFuture<TxnState> checkTransactionStatus(final String scope, final String stream,
            final TxnId txnId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txnId, "txnId");
        return streamStore.transactionStatus(scope, stream, ModelHelper.encode(txnId), null, executor)
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
        return streamStore.createScope(scope);
    }

    /**
     * Controller Service API to delete scope.
     *
     * @param scope Name of scope to be deleted.
     * @return Status of delete scope.
     */
    public CompletableFuture<DeleteScopeStatus> deleteScope(final String scope) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        return streamStore.deleteScope(scope);
    }

    /**
     * List existing streams in scopes.
     *
     * @param scope Name of the scope.
     * @return List of streams in scope.
     */
    public CompletableFuture<List<StreamConfiguration>> listStreamsInScope(final String scope) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        return streamStore.listStreamsInScope(scope);
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
     * Retrieve a scope.
     *
     * @param scopeName Name of Scope.
     * @return Scope if it exists.
     */
    public CompletableFuture<String> getScope(final String scopeName) {
        Preconditions.checkNotNull(scopeName);
        return streamStore.getScopeConfiguration(scopeName);
    }
}
