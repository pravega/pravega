/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import io.pravega.common.concurrent.Futures;
import io.pravega.shared.controller.event.AutoScaleEvent;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.segment.StreamSegmentNameUtils;
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

import static io.pravega.controller.eventProcessor.impl.EventProcessorHelper.withRetries;

/**
 * Request handler for scale requests in scale-request-stream.
 */
@Slf4j
public class AutoScaleTask {

    private static final long REQUEST_VALIDITY_PERIOD = Duration.ofMinutes(10).toMillis();

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public AutoScaleTask(final StreamMetadataTasks streamMetadataTasks,
                         final StreamMetadataStore streamMetadataStore,
                         final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(streamMetadataTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamMetadataStore = streamMetadataStore;
        this.executor = executor;
    }

    public CompletableFuture<Void> execute(final AutoScaleEvent request) {
        if (!(request.getTimestamp() + REQUEST_VALIDITY_PERIOD > System.currentTimeMillis())) {
            // request no longer valid. Ignore.
            // log, because a request was fetched from the stream after its validity expired.
            // This should be a rare occurrence. Either the request was unable to acquire lock for a long time. Or
            // we are processing at much slower rate than the message ingestion rate into the stream. We should scale up.
            // Either way, logging this helps us know how often this is happening.

            log.info(String.format("[requestId={}] Scale Request for stream %s/%s expired", request.getTimestamp(),
                    request.getScope(), request.getStream()));
            return CompletableFuture.completedFuture(null);
        }

        final OperationContext context = streamMetadataStore.createContext(request.getScope(), request.getStream());

        return withRetries(() -> {
            final CompletableFuture<ScalingPolicy> policyFuture = streamMetadataStore
                    .getConfiguration(request.getScope(), request.getStream(), context, executor)
                    .thenApply(StreamConfiguration::getScalingPolicy);

            if (request.getDirection() == AutoScaleEvent.UP) {
                return policyFuture.thenComposeAsync(policy -> processScaleUp(request, policy, context), executor);
            } else {
                return policyFuture.thenComposeAsync(policy -> processScaleDown(request, policy, context), executor);
            }
        }, executor);
    }

    private CompletableFuture<Void> processScaleUp(final AutoScaleEvent request, final ScalingPolicy policy, final OperationContext context) {
        String qualifiedName = StreamSegmentNameUtils.getQualifiedStreamSegmentName(request.getScope(), request.getStream(), request.getSegmentId());
        log.info("[requestId={}] Scale up request received for stream segment {}", request.getTimestamp(), qualifiedName);
        if (policy.getScaleType().equals(ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS)) {
            return CompletableFuture.completedFuture(null);
        }
        return streamMetadataStore.getSegment(request.getScope(), request.getStream(), request.getSegmentId(), context, executor)
                .thenComposeAsync(segment -> {
                    // do not go above scale factor. Minimum scale factor is 2 though.
                    int numOfSplits = Math.min(Math.max(2, request.getNumOfSplits()), Math.max(2, policy.getScaleFactor()));
                    double delta = (segment.getKeyEnd() - segment.getKeyStart()) / numOfSplits;

                    final ArrayList<AbstractMap.SimpleEntry<Double, Double>> simpleEntries = new ArrayList<>();
                    for (int i = 0; i < numOfSplits - 1; i++) {
                        simpleEntries.add(new AbstractMap.SimpleEntry<>(segment.getKeyStart() + delta * i,
                                segment.getKeyStart() + (delta * (i + 1))));
                    }
                    // add the last entry such that is key end matches original segments key end.
                    // This is because of doubles precision which may mean `start + n * ((end - start) / n)` may not equal `end`.
                    simpleEntries.add(new AbstractMap.SimpleEntry<>(segment.getKeyStart() + delta * (numOfSplits -1),
                            segment.getKeyEnd()));

                    return postScaleRequest(request, Lists.newArrayList(request.getSegmentId()), simpleEntries, request.getTimestamp());
                }, executor);
    }

    private CompletableFuture<Void> processScaleDown(final AutoScaleEvent request, final ScalingPolicy policy, final OperationContext context) {
        String qualifiedName = StreamSegmentNameUtils.getQualifiedStreamSegmentName(request.getScope(), request.getStream(), request.getSegmentId());
        log.info("[requestId={}] Scale down request received for stream segment {}", request.getTimestamp(), qualifiedName);
        if (policy.getScaleType().equals(ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS)) {
            return CompletableFuture.completedFuture(null);
        }

        return streamMetadataStore.markCold(request.getScope(),
                request.getStream(),
                request.getSegmentId(),
                request.isSilent() ? Long.MAX_VALUE : request.getTimestamp() + REQUEST_VALIDITY_PERIOD,
                context, executor)
                .thenCompose(x -> streamMetadataStore.getActiveSegments(request.getScope(), request.getStream(), context, executor))
                .thenApply(activeSegments -> {
                    assert activeSegments != null;
                    final Optional<Segment> currentOpt = activeSegments.stream()
                            .filter(y -> y.segmentId() == request.getSegmentId()).findAny();
                    if (!currentOpt.isPresent() || activeSegments.size() == policy.getMinNumSegments()) {
                        // if we are already at min-number of segments, we cant scale down, we have put the marker,
                        // we should simply return and do nothing.
                        return null;
                    } else {
                        final List<Segment> candidates = activeSegments.stream().filter(z -> z.getKeyEnd() == currentOpt.get().getKeyStart() ||
                                z.getKeyStart() == currentOpt.get().getKeyEnd() || z.segmentId() == request.getSegmentId())
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
                        return Futures.filter(candidates,
                                candidate -> streamMetadataStore.isCold(request.getScope(),
                                        request.getStream(),
                                        candidate.segmentId(),
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
                            String segmentName = StreamSegmentNameUtils.getQualifiedStreamSegmentName(request.getScope(), request.getStream(), x.segmentId());

                            log.debug("[requestId={}] Merging stream segment {} ", request.getTimestamp(), segmentName);
                        });

                        final ArrayList<AbstractMap.SimpleEntry<Double, Double>> simpleEntries = new ArrayList<>();
                        double min = toMerge.stream().mapToDouble(Segment::getKeyStart).min().getAsDouble();
                        double max = toMerge.stream().mapToDouble(Segment::getKeyEnd).max().getAsDouble();
                        simpleEntries.add(new AbstractMap.SimpleEntry<>(min, max));
                        final ArrayList<Long> segments = new ArrayList<>();
                        toMerge.forEach(segment -> segments.add(segment.segmentId()));
                        return postScaleRequest(request, segments, simpleEntries, request.getTimestamp());
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
     * @return CompletableFuture
     */
    private CompletableFuture<Void> postScaleRequest(final AutoScaleEvent request, final ArrayList<Long> segments,
                                                     final ArrayList<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                     final long timestamp) {
        ScaleOpEvent event = new ScaleOpEvent(request.getScope(),
                request.getStream(),
                segments,
                newRanges,
                false,
                timestamp);

        return streamMetadataTasks.writeEvent(event);
    }
}
