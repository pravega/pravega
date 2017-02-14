/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.RetryableException;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.controller.store.stream.OperationContext;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.task.ConflictingTaskException;
import com.emc.pravega.controller.store.task.LockFailedException;
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

        final CompletableFuture<Void> result = new CompletableFuture<>();

        // Wrapping the processing in Retry.
        // Upon trying a scale operation, if it fails with a retryable exception we will retry the operation.
        // If the operation continues to fail after all retries have been exhausted, while we still are dealing with
        // a retryable exception, we will put it back in the request Stream.
        // Creating a duplicate entry in the request stream will have following drawbacks:
        // a) it increases the traffic in request stream
        // b) upon controller failover, means wasted compute cycles and affecting other requests.
        // So we should be conservative in creating duplicate entries. We should only create it after having retried
        // sufficient number of times.
        // However, we should eventually create a duplicate entry if we are not able to process a scale task. This is because
        // otherwise we will stall progression of checkpoint. Also, finishing this processing frees up compute resources which
        // could be used to run other requests while this can be retried later.

        // Any Retryable exception is typically because of intermittent network issues.
        // While running a scale task we make network calls into metadata store and pravega hosts.
        //
        // Scale-task has retries built in for each of its steps. So if a scale task fails after exhausting its retries
        // and throws an exception we change it to non-retryable.
        //
        // The following retry block is for retrying other steps involved in computing scale tasks's input (metadata store calls) and
        // locking or conflict failures of scale-task.
        // ProcessScaleUp and processScaleDown functions are responsible for creating input for scale tasks.
        Retry.withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
                .retryingOn(RetryableException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> {
                    final CompletableFuture<ScalingPolicy> policyFuture = streamMetadataStore
                            .getConfiguration(request.getScope(), request.getStream(), context, executor)
                            .thenApply(StreamConfiguration::getScalingPolicy);

                    if (request.getDirection() == ScaleRequest.UP) {
                        return policyFuture.thenComposeAsync(policy -> processScaleUp(request, policy, context), executor)
                                .whenComplete((res, ex) -> {
                                    if (ex != null) {
                                        log.error("ScaleRequestHandler process scaleUp for segment {}/{}/{} threw exception", request.getScope(), request.getStream(), request.getSegmentNumber(), ex.getMessage());

                                        result.completeExceptionally(ex);
                                    } else {
                                        result.complete(res);
                                    }
                                });
                    } else {
                        return policyFuture.thenComposeAsync(policy -> processScaleDown(request, policy, context), executor)
                                .whenComplete((res, ex) -> {
                                    if (ex != null) {
                                        log.error("ScaleRequestHandler process scaleDown for segment {}/{}/{} threw exception", request.getScope(), request.getStream(), request.getSegmentNumber(), ex.getMessage());

                                        result.completeExceptionally(ex);
                                    } else {
                                        result.complete(res);
                                    }
                                });
                    }
                }, executor);

        return result;
    }

    /**
     * Helper method to say if an operation requested at given timestamp is still valid.
     *
     * @param timestamp timestamp when the operation was requested.
     * @return true if validity period has not elapsed since timestamp, else false.
     */
    private boolean isValid(long timestamp) {
        return timestamp > System.currentTimeMillis();
    }

    private CompletableFuture<Void> processScaleUp(final ScaleRequest request, final ScalingPolicy policy, final OperationContext context) {
        return streamMetadataStore.getSegment(request.getScope(), request.getStream(), request.getSegmentNumber(), context, executor)
                .thenComposeAsync(segment -> {
                    if (!policy.getType().equals(ScalingPolicy.Type.FIXED_NUM_SEGMENTS)) {
                        // do not go above scale factor. Minimum scale factor is 2 though.
                        int numOfSplits = Math.min(request.getNumOfSplits(), Math.max(2, policy.getScaleFactor()));
                        double delta = (segment.getKeyEnd() - segment.getKeyStart()) / numOfSplits;

                        final ArrayList<AbstractMap.SimpleEntry<Double, Double>> simpleEntries = new ArrayList<>();
                        for (int i = 0; i < numOfSplits; i++) {
                            simpleEntries.add(new AbstractMap.SimpleEntry<>(segment.getKeyStart() + delta * i,
                                    segment.getKeyStart() + (delta * (i + 1))));
                        }
                        return executeScaleTask(request, Lists.newArrayList(request.getSegmentNumber()), simpleEntries, context);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }, executor);
    }

    private CompletableFuture<Void> processScaleDown(final ScaleRequest request, final ScalingPolicy policy, final OperationContext context) {
        if (!policy.getType().equals(ScalingPolicy.Type.FIXED_NUM_SEGMENTS)) {
            return streamMetadataStore.setMarker(request.getScope(),
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
                                    candidate -> streamMetadataStore.getMarker(request.getScope(),
                                            request.getStream(),
                                            candidate.getNumber(),
                                            context, executor).thenApply(x -> x.map(this::isValid).orElse(false)))
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
                            final ArrayList<AbstractMap.SimpleEntry<Double, Double>> simpleEntries = new ArrayList<>();
                            simpleEntries.add(new AbstractMap.SimpleEntry<Double, Double>(toMerge.get(0).getKeyStart(), toMerge.get(toMerge.size() - 1).getKeyEnd()));
                            final ArrayList<Integer> segments = new ArrayList<>();
                            toMerge.forEach(segment -> segments.add(segment.getNumber()));
                            return executeScaleTask(request, segments, simpleEntries, context);
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Scale tasks exceptions are absorbed.
     *
     * @param request   incoming request from request stream.
     * @param segments  segments to seal
     * @param newRanges new ranges for segments to create
     * @param context
     * @return CompletableFuture
     */
    private CompletableFuture<Void> executeScaleTask(final ScaleRequest request, final ArrayList<Integer> segments,
                                                     final ArrayList<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                     final OperationContext context) {
        return streamMetadataStore.blockTransactions(request.getScope(), request.getStream(), context, executor)
                .thenCompose(x -> streamMetadataTasks.scale(request.getScope(),
                        request.getStream(),
                        segments,
                        newRanges,
                        System.currentTimeMillis(),
                        context)
                        .whenCompleteAsync((result, e) -> {
                            if (e != null) {
                                log.warn("Scale failed for request {}/{}/{} with exception", request.getScope(), request.getStream(), request.getSegmentNumber(), e.getMessage());

                                if (e instanceof LockFailedException || e.getCause() instanceof LockFailedException) {
                                    // lock failure, throw an exception here,
                                    // and that will result in several retries exhausting which the request will be put back
                                    // into request stream.
                                    // Note: We do not want to put the request back in the request stream very quickly either as it
                                    // can lead to flooding of request stream with duplicate messages for this request. This is
                                    // particularly bad during controller instance failover recovery and can also impact other
                                    // requests.
                                    blockTxCreationAndSweepTimedout(request, context); // block and sweep returns a future, but we dont need any callbacks linked with it.
                                    throw e instanceof LockFailedException ? (LockFailedException) e : (LockFailedException) e.getCause();
                                } else {
                                    // We could be here because of two reasons:
                                    // 1. Its a non-retryable Exception
                                    // 2. Its a retryable Exception
                                    //
                                    // Non-retryable: We cant retry the task. We should just fail.
                                    // Note: scale operation may have partially completed whereby we maybe in inconsistent state.
                                    // If we are here scale task failed at some intermediate step with a RuntimeException or a known
                                    // non-retryable exception.
                                    // RuntimeExceptions are thrown when we dont understand the error that has occurred.
                                    // Non-retryable Exceptions are thrown when we know definitely that the error will reoccur
                                    // even if we retry the operation.

                                    // Retryable:
                                    //
                                    // Scale operation is partially complete, but since this is a retryable, we would have
                                    // retried the failing step prescribed number of times and all our retries would have exhausted.
                                    // Note: with current defaults, we would have attempted operation about 100 times with up to 10 sec gaps
                                    // This translates to approximately 10 minutes of retrying. No need to keep retrying.
                                    //
                                    // Ideally we would want to keep retrying indefinitely as eventually this should succeed.
                                    // But that will lead to wasting compute cycles and stalling checkpoint for other requests.
                                    // We could be hitting this error case because of complete cluster failure/ store failure/ network failure
                                    // OR code bug leading to repeated failures or cascading failures.
                                    // We should notify the administrator about the partial completion of scale and let them fix it.
                                    // Ideally we want to complete a scale task once started and hence have large number of retries.
                                    //
                                    // We also DO NOT want scale task being retried by putting it back into Request stream as
                                    // that will result in a new scale task being created while this one has not
                                    // finished. Idempotency of steps, esp pre-condition check is based on the "scaleTimestamp".
                                    // So new request will simply fail at precondition if in previous iteration metadata store
                                    // had been updated.
                                    // So as far as processing here is concerned, we will throw non-retryable exception and stop processing.
                                    //
                                    // Note: a stream's metadata may be in inconsistent state because we were not able to complete the scale task.
                                    // An admin needs to be notified. Also, we need to prevent other scale operations on this stream until the
                                    // inconsistency is resolved/fixed.
                                    // TODO: have a mechanism to prevent any scale operations on this stream until potential inconsistency is taken care of.
                                    // Ideally we should do the above before releasing the lock. So this mechanism should be built as part of task framework.
                                    // As 'thing-do-on-task-failure-before-releasing-lock'
                                    throw new RuntimeException(e);
                                }
                            } else if (result.getStatus().equals(ScaleStreamStatus.TXN_CONFLICT)) {
                                // transactions were running, throw a retryable exception.
                                throw new ConflictingTaskException(request.getStream());
                            } else {
                                // completed - either successfully or with pre-condition-failure. Clear markers on all scaled segments.
                                clearMarkers(request.getScope(), request.getStream(), segments, context);
                            }
                        }, executor))
                .handle((res, ex) -> {
                    // if it is retryable exception, do not unblock creation of txn and let scale be attempted again.
                    // However, if its either completed successfully or failed with non-retryable, we need to unblock
                    // creation of transactions.

                    if (ex != null) {
                        log.error("scale failed for request {}/{}/{} with exception", request.getScope(), request.getStream(), request.getSegmentNumber(), ex.getMessage());

                        RetryableException.throwRetryableOrElseRuntime(ex);
                        return ex;
                    } else {
                        return null;
                    }
                })
                .thenCompose(ex -> streamMetadataStore.unblockTransactions(request.getScope(), request.getStream(), context, executor)
                        .exceptionally(e -> {
                            if (ex != null) {
                                // if scale operation had failed, we should throw the original exception. This has to be non-retryable as
                                // scale task has exhausted all its lives
                                RetryableException.throwRetryableOrElseRuntime(ex);
                            } else {
                                // unblock failed with retryable.
                                log.warn("Failed to unblock txn creation for stream {}/{}", request.getScope(), request.getStream());
                                throw new RuntimeException(e);
                            }
                            return null;
                        }));
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
