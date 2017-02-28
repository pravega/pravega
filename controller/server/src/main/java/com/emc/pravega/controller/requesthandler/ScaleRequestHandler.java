/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.controller.retryable.RetryableException;
import com.emc.pravega.controller.store.stream.OperationContext;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.task.ConflictingTaskException;
import com.emc.pravega.controller.store.task.LockFailedException;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.ScaleStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Request handler for scale requests in scale-request-stream.
 */
@Slf4j
public class ScaleRequestHandler implements RequestHandler<ScaleRequest> {

    private static final long RETRY_INITIAL_DELAY = 100;
    private static final int RETRY_MULTIPLIER = 2;
    private static final int RETRY_MAX_ATTEMPTS = 100;
    private static final long RETRY_MAX_DELAY = Duration.ofSeconds(10).toMillis();

    private static final long REQUEST_VALIDITY_PERIOD = Duration.ofMinutes(10).toMillis();
    private static final Retry.RetryAndThrowConditionally<RuntimeException> RETRY = Retry.withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
            .retryWhen(RetryableException::isRetryable)
            .throwingOn(RuntimeException.class);

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final StreamTransactionMetadataTasks streamTxMetadataTasks;
    private final ScheduledExecutorService executor;

    public ScaleRequestHandler(final StreamMetadataTasks streamMetadataTasks,
                               final StreamMetadataStore streamMetadataStore,
                               final StreamTransactionMetadataTasks streamTxMetadataTasks,
                               final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(streamMetadataTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamMetadataStore = streamMetadataStore;
        this.streamTxMetadataTasks = streamTxMetadataTasks;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> process(final ScaleRequest request) {
        if (!(request.getTimestamp() + REQUEST_VALIDITY_PERIOD > System.currentTimeMillis())) {
            // request no longer valid. Ignore.
            // log, because a request was fetched from the stream after its validity expired.
            // This should be a rare occurrence. Either the request was unable to acquire lock for a long time. Or
            // we are processing at much slower rate than the message ingestion rate into the stream. We should scale up.
            // Either way, logging this helps us know how often this is happening.

            log.debug(String.format("Scale Request for stream %s/%s expired", request.getScope(), request.getStream()));
            return CompletableFuture.completedFuture(null);
        }

        final OperationContext context = streamMetadataStore.createContext(request.getScope(), request.getStream());

        return RETRY.runAsync(() -> {
            final CompletableFuture<ScalingPolicy> policyFuture = streamMetadataStore
                    .getConfiguration(request.getScope(), request.getStream(), context, executor)
                    .thenApply(StreamConfiguration::getScalingPolicy);

            if (request.getDirection() == ScaleRequest.UP) {
                return policyFuture.thenComposeAsync(policy -> processScaleUp(request, policy, context), executor);
            } else {
                return policyFuture.thenComposeAsync(policy -> processScaleDown(request, policy, context), executor);
            }
        }, executor);
    }

    private CompletableFuture<Void> processScaleUp(final ScaleRequest request, final ScalingPolicy policy, final OperationContext context) {
        log.debug("scale up request received for stream {} segment {}", request.getStream(), request.getSegmentNumber());
        if (policy.getType().equals(ScalingPolicy.Type.FIXED_NUM_SEGMENTS)) {
            return CompletableFuture.completedFuture(null);
        }
        return streamMetadataStore.getSegment(request.getScope(), request.getStream(), request.getSegmentNumber(), context, executor)
                .thenComposeAsync(segment -> {
                    // do not go above scale factor. Minimum scale factor is 2 though.
                    int numOfSplits = Math.min(request.getNumOfSplits(), Math.max(2, policy.getScaleFactor()));
                    double delta = (segment.getKeyEnd() - segment.getKeyStart()) / numOfSplits;

                    final ArrayList<AbstractMap.SimpleEntry<Double, Double>> simpleEntries = new ArrayList<>();
                    for (int i = 0; i < numOfSplits; i++) {
                        simpleEntries.add(new AbstractMap.SimpleEntry<>(segment.getKeyStart() + delta * i,
                                segment.getKeyStart() + (delta * (i + 1))));
                    }
                    return executeScaleTask(request, Lists.newArrayList(request.getSegmentNumber()), simpleEntries, context);
                }, executor);
    }

    private CompletableFuture<Void> processScaleDown(final ScaleRequest request, final ScalingPolicy policy, final OperationContext context) {
        log.debug("scale down request received for stream {} segment {}", request.getStream(), request.getSegmentNumber());
        if (policy.getType().equals(ScalingPolicy.Type.FIXED_NUM_SEGMENTS)) {
            return CompletableFuture.completedFuture(null);
        }

        return streamMetadataStore.markCold(request.getScope(),
                request.getStream(),
                request.getSegmentNumber(),
                request.isSilent() ? Long.MAX_VALUE : request.getTimestamp() + REQUEST_VALIDITY_PERIOD,
                context, executor)
                .thenCompose(x -> streamMetadataStore.getActiveSegments(request.getScope(), request.getStream(), context, executor))
                .thenApply(activeSegments -> {
                    assert activeSegments != null;
                    final Optional<Segment> currentOpt = activeSegments.stream()
                            .filter(y -> y.getNumber() == request.getSegmentNumber()).findAny();
                    if (!currentOpt.isPresent() || activeSegments.size() == policy.getMinNumSegments()) {
                        // if we are already at min-number of segments, we cant scale down, we have put the marker,
                        // we should simply return and do nothing.
                        return null;
                    } else {
                        final List<Segment> candidates = activeSegments.stream().filter(z -> z.getKeyEnd() == currentOpt.get().getKeyStart() ||
                                z.getKeyStart() == currentOpt.get().getKeyEnd() || z.getNumber() == request.getSegmentNumber())
                                .sorted(Comparator.comparingDouble(Segment::getKeyStart))
                                .collect(Collectors.toList());
                        return new ImmutablePair<>(candidates, activeSegments.size() - policy.getMinNumSegments());
                    }
                })
                .thenCompose(input -> {
                    if (input != null && input.getLeft().size() > 1) {
                        final List<Segment> candidates = input.getLeft();
                        final int maxScaleDownFactor = input.getRight();

                        // fetch their cold status for all candidates
                        return FutureHelpers.filter(candidates,
                                candidate -> streamMetadataStore.isCold(request.getScope(),
                                        request.getStream(),
                                        candidate.getNumber(),
                                        context, executor))
                                .thenApply(segments -> {
                                    if (maxScaleDownFactor == 1 && segments.size() == 3) {
                                        // Note: sorted by keystart so just pick first two.
                                        return Lists.newArrayList(segments.get(0), segments.get(1));
                                    } else {
                                        return segments;
                                    }
                                });
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .thenCompose(toMerge -> {
                    if (toMerge != null && toMerge.size() > 1) {
                        toMerge.forEach(x -> {
                            log.debug("merging stream {}: segment {} ", request.getStream(), x.getNumber());
                        });

                        final ArrayList<AbstractMap.SimpleEntry<Double, Double>> simpleEntries = new ArrayList<>();
                        double min = toMerge.stream().mapToDouble(Segment::getKeyStart).min().getAsDouble();
                        double max = toMerge.stream().mapToDouble(Segment::getKeyEnd).max().getAsDouble();
                        simpleEntries.add(new AbstractMap.SimpleEntry<>(min, max));
                        final ArrayList<Integer> segments = new ArrayList<>();
                        toMerge.forEach(segment -> segments.add(segment.getNumber()));
                        return executeScaleTask(request, segments, simpleEntries, context);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    /**
     * Scale tasks exceptions are absorbed.
     *
     * @param request   incoming request from request stream.
     * @param segments  segments to seal
     * @param newRanges new ranges for segments to create
     * @param context   operation context
     * @return CompletableFuture
     */
    private CompletableFuture<Void> executeScaleTask(final ScaleRequest request, final ArrayList<Integer> segments,
                                                     final ArrayList<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                     final OperationContext context) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        CompletableFuture<ScaleResponse> scaleFuture = streamMetadataStore.blockTransactions(request.getScope(), request.getStream(), context, executor)
                .thenCompose(x -> streamMetadataTasks.scale(request.getScope(),
                        request.getStream(),
                        segments,
                        newRanges,
                        System.currentTimeMillis(),
                        context)
                        .whenCompleteAsync((res, e) -> {
                            if (e != null) {
                                log.warn("Scale failed for request {}/{}/{} with exception {}", request.getScope(), request.getStream(), request.getSegmentNumber(), e);
                                Throwable cause = ExceptionHelpers.getRealException(e);
                                if (cause instanceof LockFailedException) {
                                    throw (LockFailedException) cause;
                                } else {
                                    throw new RuntimeException(e);
                                }
                            } else if (res.getStatus().equals(ScaleStreamStatus.TXN_CONFLICT)) {
                                // transactions were running, throw a retryable exception.
                                throw new ConflictingTaskException(request.getStream());
                            } else {
                                // completed - either successfully or with pre-condition-failure. Clear markers on all scaled segments.
                                clearMarkers(request.getScope(), request.getStream(), segments, context);
                            }
                        }, executor));

        scaleFuture.whenComplete((r, ex) -> {
            if (ex != null && ExceptionHelpers.getRealException(ex) instanceof LockFailedException) {
                // lock failure, throw an exception here,
                // and that will result in several retries exhausting which the request will be put back
                // into request stream.
                blockTxCreationAndSweepTimedout(request, context); // block and sweep returns a future, but we dont need any callbacks linked with it.
            } else {
                streamMetadataStore.unblockTransactions(request.getScope(), request.getStream(), context, executor);
            }
        });

        scaleFuture.whenComplete((res, ex) -> {
            // if it is retryable exception, do not unblock creation of txn and let scale be attempted again.
            // However, if its either completed successfully or failed with non-retryable, we need to unblock
            // creation of transactions.
            if (ex != null) {
                log.error("scale failed for request {}/{}/{} with exception", request.getScope(), request.getStream(), request.getSegmentNumber(), ex.getMessage());
                result.completeExceptionally(ex);
            } else {
                log.error("scale done for {}/{}/{}", request.getScope(), request.getStream(), request.getSegmentNumber());
                result.complete(null);
            }
        });

        return result;
    }

    /**
     * Block creation of new transactions for limited period while scale will attempt to acquire lock.
     * It may still not be able to acquire the lock immediately as there could be ongoing transactions.
     * But by blocking creation of new transactions, it increase probability of scale to be processed
     * after existing transactions complete.
     * Note: there could be existing transactions that may have timed out
     * but their timed clean up may not have been scheduled successfully. So if such a txn failed, we need to
     * opportunistically sweep and clean them.
     *
     * @param request scale request
     * @param context stream store context
     */
    private CompletableFuture<Void> blockTxCreationAndSweepTimedout(ScaleRequest request, OperationContext context) {
        return streamMetadataStore.blockTransactions(request.getScope(), request.getStream(), context, executor)
                .thenCompose(x -> streamMetadataStore.getActiveTxns(request.getScope(), request.getStream(), context, executor))
                .thenCompose(x -> FutureHelpers.allOfWithResults(x.entrySet().stream().filter(y ->
                        System.currentTimeMillis() - y.getValue().getTxCreationTimestamp() > Config.TXN_TIMEOUT_IN_SECONDS)
                        .map(z -> streamTxMetadataTasks.abortTx(request.getScope(), request.getStream(), z.getKey(), null))
                        .collect(Collectors.toList())))
                .handle((res, ex) -> {
                    if (ex != null) {
                        throw new RuntimeException(ex);
                    }
                    return null;
                });
    }

    private CompletableFuture<List<Void>> clearMarkers(final String scope, final String stream, final ArrayList<Integer> segments, final OperationContext context) {
        return FutureHelpers.allOfWithResults(segments.stream().parallel().map(x -> streamMetadataStore.removeMarker(scope, stream, x, context, executor)).collect(Collectors.toList()));
    }
}
