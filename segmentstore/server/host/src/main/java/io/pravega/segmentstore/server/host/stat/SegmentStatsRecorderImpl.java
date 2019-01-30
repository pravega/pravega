/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.stat;

import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SegmentStatsRecorderImpl implements SegmentStatsRecorder {
    private static final long TWO_MINUTES = Duration.ofMinutes(2).toMillis();
    private static final long TWENTY_MINUTES = Duration.ofMinutes(20).toMillis();
    private static final int INITIAL_CAPACITY = 1000;
    private static final int MAX_CACHE_SIZE = 100000; // 100k segment records in memory.
    // At 100k * with each aggregate approximately ~80 bytes = 8 Mb of memory foot print.
    // Assuming 32 bytes for streamSegmentName used as the key in the cache = 3Mb
    // So this can handle 100k concurrently active stream segments with about 11-12 Mb footprint.
    // If cache overflows beyond this, entries will be evicted in order of last accessed.
    // So we will lose relevant traffic history if we have 100k active 'stream segments' across containers
    // where traffic is flowing concurrently.

    private static final Duration TIMEOUT = Duration.ofMinutes(1);

    private final Set<String> pendingCacheLoads;
    private final Cache<String, SegmentAggregates> cache;
    private final long reportingDuration;
    private final AutoScaleProcessor reporter;
    private final StreamSegmentStore store;
    private final Executor executor;

    SegmentStatsRecorderImpl(AutoScaleProcessor reporter, StreamSegmentStore store,
                             ExecutorService executor, ScheduledExecutorService maintenanceExecutor) {
        this(reporter, store, TWO_MINUTES, TWENTY_MINUTES, TimeUnit.MINUTES, executor, maintenanceExecutor);
    }

    @VisibleForTesting
    SegmentStatsRecorderImpl(AutoScaleProcessor reporter, StreamSegmentStore store,
                             long reportingDuration, long expiryDuration, TimeUnit timeUnit,
                             ExecutorService executor, ScheduledExecutorService maintenanceExecutor) {
        Preconditions.checkNotNull(executor);
        Preconditions.checkNotNull(maintenanceExecutor);
        this.executor = executor;
        this.pendingCacheLoads = Collections.synchronizedSet(new HashSet<>());

        this.cache = CacheBuilder.newBuilder()
                .initialCapacity(INITIAL_CAPACITY)
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterAccess(expiryDuration, timeUnit)
                // We may want to store some traffic related information in attributes
                // and reconstruct while loading from it for evicted segments.
                // .removalListener((RemovalListener<String, SegmentAggregates>) notification -> {
                //    if (notification.getCause().equals(RemovalCause.SIZE)) {
                //        store.updateAttributes(notification.getKey(), Collections.emptyList(), TIMEOUT);
                //    }
                //notification.getCause().equals(RemovalCause.EXPIRED)
                // expired will happen if there is no traffic for expiry duration
                // so no need to update attributes in the store
                //})
                .build();

        // Dedicated thread for cache clean up scheduled periodically. This ensures that read and write
        // on cache are not used for cache maintenance activities.
        maintenanceExecutor.scheduleAtFixedRate(cache::cleanUp, Duration.ofMinutes(2).toMillis(), 2, TimeUnit.MINUTES);
        this.reportingDuration = reportingDuration;
        this.store = store;
        this.reporter = reporter;
    }

    private SegmentAggregates getSegmentAggregate(String streamSegmentName) {
        SegmentAggregates aggregates = cache.getIfPresent(streamSegmentName);

        if (aggregates == null &&
                !StreamSegmentNameUtils.isTransactionSegment(streamSegmentName)) {
            loadAsynchronously(streamSegmentName);
        }

        return aggregates;
    }

    private void loadAsynchronously(String streamSegmentName) {
        if (!pendingCacheLoads.contains(streamSegmentName)) {
            pendingCacheLoads.add(streamSegmentName);
            if (store != null) {
                store.getStreamSegmentInfo(streamSegmentName, TIMEOUT)
                        .thenAcceptAsync(prop -> {
                            if (prop != null &&
                                    prop.getAttributes().containsKey(Attributes.SCALE_POLICY_TYPE) &&
                                    prop.getAttributes().containsKey(Attributes.SCALE_POLICY_RATE)) {
                                byte type = prop.getAttributes().get(Attributes.SCALE_POLICY_TYPE).byteValue();
                                int rate = prop.getAttributes().get(Attributes.SCALE_POLICY_RATE).intValue();
                                cache.put(streamSegmentName, new SegmentAggregates(type, rate));
                            }
                            pendingCacheLoads.remove(streamSegmentName);
                        }, executor);
            }
        }
    }

    @Override
    public void createSegment(String streamSegmentName, byte type, int targetRate) {
        cache.put(streamSegmentName, new SegmentAggregates(type, targetRate));
        reporter.notifyCreated(streamSegmentName, type, targetRate);
    }

    @Override
    public void sealSegment(String streamSegmentName) {
        if (getSegmentAggregate(streamSegmentName) != null) {
            cache.invalidate(streamSegmentName);
            reporter.notifySealed(streamSegmentName);
        }
    }

    @Override
    public void policyUpdate(String streamSegmentName, byte type, int targetRate) {
        SegmentAggregates aggregates = getSegmentAggregate(streamSegmentName);
        if (aggregates != null) {
            // if there is a scale type change, discard the old object and create a new object
            if (aggregates.getScaleType() != type) {
                cache.put(streamSegmentName, new SegmentAggregates(type, targetRate));
            } else {
                aggregates.setTargetRate(targetRate);
            }
        }
    }

    /**
     * Updates segment specific aggregates.
     * Then if two minutes have elapsed between last report
     * of aggregates for this segment, send a new update to the monitor.
     * This update to the monitor is processed by monitor asynchronously.
     *
     * @param streamSegmentName stream segment name
     * @param dataLength        length of data that was written
     * @param numOfEvents       number of events that were written
     */
    @Override
    public void record(String streamSegmentName, long dataLength, int numOfEvents) {
        try {
            SegmentAggregates aggregates = getSegmentAggregate(streamSegmentName);
            // Note: we could get stats for a transaction segment. We will simply ignore this as we
            // do not maintain intermittent txn segment stats. Txn stats will be accounted for
            // only upon txn commit. This is done via merge method. So here we can get a txn which
            // we do not know about and hence we can get null and ignore.

            if (aggregates != null) {
                if (aggregates.getScaleType() != WireCommands.CreateSegment.NO_SCALE) {
                    aggregates.update(dataLength, numOfEvents);
                    report(streamSegmentName, aggregates);
                }
            }
        } catch (Exception e) {
            log.warn("Record statistic for {} for data: {} and events:{} threw exception", streamSegmentName, dataLength, numOfEvents, e);
        }
    }

    /**
     * Method called with txn stats whenever a txn is committed.
     *
     * @param streamSegmentName parent segment name
     * @param dataLength        length of data written in txn
     * @param numOfEvents       number of events written in txn
     * @param txnCreationTime   time when txn was created
     */
    @Override
    public void merge(String streamSegmentName, long dataLength, int numOfEvents, long txnCreationTime) {
        SegmentAggregates aggregates = getSegmentAggregate(streamSegmentName);
        if (aggregates != null) {
            aggregates.updateTx(dataLength, numOfEvents, txnCreationTime);
            report(streamSegmentName, aggregates);
        }
    }

    private long report(String streamSegmentName, SegmentAggregates aggregates) {
        return aggregates.lastReportedTime.getAndUpdate(prev -> {
            if (System.currentTimeMillis() - prev > reportingDuration) {
                reporter.report(streamSegmentName,
                        aggregates.getTargetRate(), aggregates.getScaleType(), aggregates.getStartTime(),
                        aggregates.getTwoMinuteRate(), aggregates.getFiveMinuteRate(),
                        aggregates.getTenMinuteRate(), aggregates.getTwentyMinuteRate());
                return System.currentTimeMillis();
            }

            return prev;
        });
    }

    @VisibleForTesting
    SegmentAggregates getIfPresent(String streamSegmentName) {
        return cache.getIfPresent(streamSegmentName);
    }
}
