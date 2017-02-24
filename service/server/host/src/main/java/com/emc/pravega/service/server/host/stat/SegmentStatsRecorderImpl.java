/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.server.host.stat;

import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.common.segment.StreamSegmentNameUtils;
import com.emc.pravega.service.contracts.Attributes;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SegmentStatsRecorderImpl implements SegmentStatsRecorder {
    private static final long TWO_MINUTES = Duration.ofMinutes(2).toMillis();
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
        this(reporter, store, TWO_MINUTES, executor, maintenanceExecutor);
    }

    @VisibleForTesting
    SegmentStatsRecorderImpl(AutoScaleProcessor reporter, StreamSegmentStore store,
                             long reportingDuration, ExecutorService executor, ScheduledExecutorService maintenanceExecutor) {
        Preconditions.checkNotNull(executor);
        Preconditions.checkNotNull(maintenanceExecutor);
        this.executor = executor;
        this.pendingCacheLoads = new HashSet<>();

        this.cache = CacheBuilder.newBuilder()
                .initialCapacity(INITIAL_CAPACITY)
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterAccess(20, TimeUnit.MINUTES)
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

    @VisibleForTesting
    SegmentAggregates getSegmentAggregate(String streamSegmentName) {
        SegmentAggregates aggregates = cache.getIfPresent(streamSegmentName);

        if (aggregates == null &&
                StreamSegmentNameUtils.getParentStreamSegmentName(streamSegmentName) != null) {
            loadAsynchronously(streamSegmentName);
        }

        return aggregates;
    }

    private void loadAsynchronously(String streamSegmentName) {

        if (!pendingCacheLoads.contains(streamSegmentName)) {
            pendingCacheLoads.add(streamSegmentName);
            Optional.ofNullable(store).ifPresent(s -> s.getStreamSegmentInfo(streamSegmentName, false, TIMEOUT)
                    .thenAcceptAsync(prop -> {
                        if (prop != null &&
                                prop.getAttributes().containsKey(Attributes.SCALE_POLICY_TYPE) &&
                                prop.getAttributes().containsKey(Attributes.SCALE_POLICY_RATE)) {
                            byte type = prop.getAttributes().getOrDefault(Attributes.SCALE_POLICY_TYPE, 0L).byteValue();
                            int rate = prop.getAttributes().get(Attributes.SCALE_POLICY_TYPE).intValue();
                            cache.put(streamSegmentName, new SegmentAggregates(type, rate));
                        }
                        pendingCacheLoads.remove(streamSegmentName);
                    }, executor));
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

                    if (System.currentTimeMillis() - aggregates.getLastReportedTime() > reportingDuration) {
                        try {
                            executor.execute(() -> reporter.report(streamSegmentName,
                                    aggregates.getTargetRate(), aggregates.getScaleType(), aggregates.getStartTime(),
                                    aggregates.getTwoMinuteRate(), aggregates.getFiveMinuteRate(),
                                    aggregates.getTenMinuteRate(), aggregates.getTwentyMinuteRate()));
                            aggregates.setLastReportedTime(System.currentTimeMillis());
                        } catch (RejectedExecutionException e) {
                            // We will not keep posting indefinitely and let the queue grow. We will only post optimistically
                            // and ignore any rejected execution exceptions.
                            log.error("Executor queue full. We will report this when pending work clears up. StreamSegmentName: {}",
                                    streamSegmentName);
                        }
                    }
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
        // This should never be null as a txn commit cannot happen on a sealed segment.
        // However, if a diskbacked cache is not present, this may be null as it may have been evicted from cache.
        if (aggregates != null) {
            aggregates.updateTx(dataLength, numOfEvents, txnCreationTime);
        }
    }
}
