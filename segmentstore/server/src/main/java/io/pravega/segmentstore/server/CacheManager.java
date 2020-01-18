/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.segmentstore.storage.cache.CacheState;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages the lifecycle of Cache Entries. Decides which entries are to be kept in memory and which are eligible for
 * removal.
 *
 * Entry Management is indirect, and doesn't deal with them directly. Also, the management needs to be done across multiple
 * CacheManager Clients, and a common scheme needs to be used to instruct each such client when it's time to evict unused entries.
 *
 * The CacheManager holds two reference numbers: the current generation and the oldest generation. Every new Cache Entry
 * (in the clients) that is generated or updated gets assigned the current generation. As the CacheManager determines that
 * there are too many Cache Entries or that the maximum size has been exceeded, it will increment the oldest generation.
 * The CacheManager Clients can use this information to evict those Cache Entries that have a generation below the oldest generation number.
 */
@Slf4j
@ThreadSafe
public class CacheManager extends AbstractScheduledService implements AutoCloseable {
    //region Members

    private static final String TRACE_OBJECT_ID = "CacheManager";
    @GuardedBy("lock")
    private final Collection<Client> clients;
    private final ScheduledExecutorService executorService;
    @GuardedBy("lock")
    private int currentGeneration;
    @GuardedBy("lock")
    private int oldestGeneration;
    @GuardedBy("lock")
    private CacheState lastCacheState;
    private final CachePolicy policy;
    private final AtomicBoolean closed;
    private final SegmentStoreMetrics.CacheManager metrics;
    @Getter
    private final CacheStorage cacheStorage;
    @Getter
    private final CacheUtilizationProvider utilizationProvider;
    private final Object lock = new Object();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the CacheManager class.
     *
     * @param policy          The policy to use with this CacheManager.
     * @param executorService An executorService to use for scheduled tasks.
     */
    public CacheManager(CachePolicy policy, ScheduledExecutorService executorService) {
        this(policy, new DirectMemoryCache(policy.getMaxSize()), executorService);
    }

    /**
     * Creates a new instance of the CacheManager class.
     *
     * @param policy          The policy to use with this CacheManager.
     * @param cacheStorage       The CacheStorage to maintain.
     * @param executorService An executorService to use for scheduled tasks.
     */
    @VisibleForTesting
    public CacheManager(CachePolicy policy, CacheStorage cacheStorage, ScheduledExecutorService executorService) {
        this.policy = Preconditions.checkNotNull(policy, "policy");
        this.executorService = Preconditions.checkNotNull(executorService, "executorService");
        this.cacheStorage = Preconditions.checkNotNull(cacheStorage, "cacheStorage");
        this.cacheStorage.setCacheFullCallback(this::cacheFullCallback);
        this.clients = new HashSet<>();
        this.oldestGeneration = 0;
        this.currentGeneration = 0;
        this.closed = new AtomicBoolean();
        this.lastCacheState = this.cacheStorage.getState();
        this.metrics = new SegmentStoreMetrics.CacheManager();
        this.utilizationProvider = new CacheUtilizationProvider(this.policy, this::getStoredBytes);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (state() == State.RUNNING) {
                Futures.await(Services.stopAsync(this, this.executorService));
            }

            synchronized (this.lock) {
                this.clients.clear();
            }

            this.cacheStorage.close();
            long pendingBytes = this.utilizationProvider.getPendingBytes();
            if (pendingBytes > 0) {
                log.error("{}: Closing with {} outstanding bytes. This indicates a leak somewhere.",
                        TRACE_OBJECT_ID, pendingBytes);

                assert false : "CacheManager closed with " + pendingBytes + " outstanding bytes."; // This will fail any unit tests.
            }
            log.info("{} Closed.", TRACE_OBJECT_ID);
        }
    }

    //endregion

    //region AbstractScheduledService Implementation

    @Override
    protected ScheduledExecutorService executor() {
        return this.executorService;
    }

    @Override
    protected void runOneIteration() {
        boolean anythingEvicted = applyCachePolicy();
        if (anythingEvicted) {
            this.utilizationProvider.notifyCleanupListeners();
        }
    }

    @Override
    protected Scheduler scheduler() {
        long millis = this.policy.getGenerationDuration().toMillis();
        return Scheduler.newFixedDelaySchedule(millis, millis, TimeUnit.MILLISECONDS);
    }

    //endregion

    //region Client Registration

    /**
     * Registers the given client to this CacheManager.
     *
     * @param client The client to register.
     */
    public void register(Client client) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkNotNull(client, "client");
        synchronized (this.lock) {
            if (!this.clients.contains(client)) {
                this.clients.add(client);
                client.updateGenerations(this.currentGeneration, this.oldestGeneration);
            }
        }

        log.info("{} Registered {}.", TRACE_OBJECT_ID, client);
    }

    /**
     * Unregisters the given client from this CacheManager.
     *
     * @param client The client to unregister.
     */
    public void unregister(Client client) {
        if (this.closed.get()) {
            // We are done. Nothing to do here.
            return;
        }

        Preconditions.checkNotNull(client, "client");
        synchronized (this.lock) {
            this.clients.remove(client);
        }

        log.info("{} Unregistered {}.", TRACE_OBJECT_ID, client);
    }

    //endregion

    //region Helpers

    private boolean cacheFullCallback() {
        log.info("{}: Cache full. Forcing cache policy.", TRACE_OBJECT_ID);
        return applyCachePolicy();
    }

    /**
     * Same as {@link #applyCachePolicyInternal()}, but this is safe for concurrent invocation and handles all exceptions by
     * logging them.
     *
     * We must ensure that no two invocations of the {@link #applyCachePolicyInternal()) execute at the same time. It performs
     * a good amount of state checking and updating, and concurrent calls would corrupt the internal state of the {@link CacheManager).
     *
     * Under normal operating conditions, the only method invoking this is {@link #runOneIteration()} which is guaranteed
     * to execute only once at a time. Concurrent invocations come from {@link #cacheFullCallback()} which are triggered
     * by the {@link CacheStorage} becoming full while inserting into it, which can come (at the same time) from different
     * requesting threads.
     *
     * @return True if anything changed, false otherwise.
     */
    @VisibleForTesting
    protected boolean applyCachePolicy() {
        if (this.closed.get()) {
            // We are done.
            return false;
        }

        try {
            synchronized (this.lock) {
                return applyCachePolicyInternal();
            }
        } catch (Throwable ex) {
            if (Exceptions.mustRethrow(ex)) {
                throw ex;
            }

            log.error("{}: Error while applying cache policy.", TRACE_OBJECT_ID, ex);
            return false;
        }
    }

    /**
     * Same as {@link #applyCachePolicy()}, but it is unsynchronized and does not handle any errors. This method should
     * only be invoked by {@link #applyCachePolicy()} as concurrent invocations from different threads may result
     * in state corruption.
     *
     * @return True if anything changed, false otherwise.
     */
    @GuardedBy("lock")
    private boolean applyCachePolicyInternal() {
        // Run through all the active clients and gather status.
        CacheStatus currentStatus = collectStatus();
        fetchCacheState();
        if (currentStatus == null || this.lastCacheState.getStoredBytes() == 0) {
            // We either have no clients or we have clients and they do not have any data stored.
            return false;
        }

        // Increment current generation (if needed).
        boolean currentChanged = adjustCurrentGeneration(currentStatus);

        // Increment oldest generation (if needed and if possible).
        boolean oldestChanged = adjustOldestGeneration(currentStatus);

        if (!currentChanged && !oldestChanged) {
            // Nothing changed, nothing to do.
            return false;
        }

        // Notify clients that something changed (if any of the above got changed). Run in a loop, until either we can't
        // adjust the oldest anymore or we are unable to trigger any changes to the clients.
        boolean reducedInIteration;
        boolean reducedOverall = false;
        do {
            reducedInIteration = updateClients();
            if (reducedInIteration) {
                fetchCacheState();
                logCurrentStatus(currentStatus);
                oldestChanged = adjustOldestGeneration(currentStatus);
                reducedOverall = true;
            }
        } while (reducedInIteration && oldestChanged);
        this.metrics.report(this.lastCacheState, currentStatus.getNewestGeneration() - currentStatus.getOldestGeneration());
        return reducedOverall;
    }

    @GuardedBy("lock")
    private CacheStatus collectStatus() {
        int cg = this.currentGeneration;
        int minGeneration = cg;
        int maxGeneration = 0;
        ArrayList<Client> toUnregister = new ArrayList<>();
        for (Client c : this.clients) {
            CacheStatus clientStatus;
            try {
                clientStatus = c.getCacheStatus();
                if (clientStatus.isEmpty()) {
                    continue; // Nothing useful for this one.
                }
            } catch (ObjectClosedException ex) {
                // This object was closed but it was not unregistered. Do it now.
                log.warn("{} Detected closed client {}.", TRACE_OBJECT_ID, c);
                toUnregister.add(c);
                continue;
            }

            if (clientStatus.oldestGeneration > cg || clientStatus.newestGeneration > cg) {
                log.warn("{} Client {} returned status that is out of bounds {}. CurrentGeneration = {}, OldestGeneration = {}.",
                        TRACE_OBJECT_ID, c, clientStatus, this.currentGeneration, this.oldestGeneration);
            }

            minGeneration = Math.min(minGeneration, clientStatus.oldestGeneration);
            maxGeneration = Math.max(maxGeneration, clientStatus.newestGeneration);
        }

        toUnregister.forEach(this::unregister);
        if (minGeneration > maxGeneration) {
            // Either no clients or clients are empty.
            return null;
        }

        return new CacheStatus(minGeneration, maxGeneration);
    }

    @GuardedBy("lock")
    private void fetchCacheState() {
        this.lastCacheState = this.cacheStorage.getState();
    }

    @GuardedBy("lock")
    private boolean updateClients() {
        boolean reduced = false;
        int cg = this.currentGeneration;
        int og = this.oldestGeneration;
        ArrayList<Client> toUnregister = new ArrayList<>();
        for (Client c : this.clients) {
            try {
                reduced = c.updateGenerations(cg, og) | reduced;
            } catch (ObjectClosedException ex) {
                // This object was closed but it was not unregistered. Do it now.
                log.warn("{} Detected closed client {}.", TRACE_OBJECT_ID, c);
                toUnregister.add(c);
            } catch (Throwable ex) {
                if (Exceptions.mustRethrow(ex)) {
                    throw ex;
                }

                log.warn("{} Unable to update client {}.", TRACE_OBJECT_ID, c, ex);
            }
        }

        toUnregister.forEach(this::unregister);
        return reduced;
    }

    @GuardedBy("lock")
    private boolean adjustCurrentGeneration(CacheStatus currentStatus) {
        // We need to increment if at least one of the following happened:
        // 1. We had any activity in the current generation. This can be determined by comparing the current generation
        // with the newest generation from the retrieved status.
        // 2. We are currently exceeding the eviction threshold. It is possible that even with no activity, some entries
        // may have recently become eligible for eviction, in which case we should try to evict them.
        boolean shouldIncrement = currentStatus.getNewestGeneration() >= this.currentGeneration || exceedsEvictionThreshold();
        if (shouldIncrement) {
            this.currentGeneration++;
        }

        return shouldIncrement;
    }

    @GuardedBy("lock")
    private boolean adjustOldestGeneration(CacheStatus currentStatus) {
        // Figure out if we exceed the policy criteria.
        int newOldestGeneration = this.oldestGeneration;
        if (exceedsPolicy(currentStatus)) {
            // Start by setting the new value to the smallest reported value, and increment by one.
            newOldestGeneration = Math.max(newOldestGeneration, currentStatus.oldestGeneration) + 1;

            // Then factor in the oldest permissible generation.
            newOldestGeneration = Math.max(newOldestGeneration, getOldestPermissibleGeneration());

            // Then make sure we don't exceed the current generation.
            newOldestGeneration = Math.min(newOldestGeneration, this.currentGeneration);
        }

        boolean isAdjusted = newOldestGeneration > this.oldestGeneration;
        if (isAdjusted) {
            this.oldestGeneration = newOldestGeneration;
        }

        return isAdjusted;
    }

    @GuardedBy("lock")
    private boolean exceedsPolicy(CacheStatus currentStatus) {
        // We need to increment the OldestGeneration only if any of the following conditions occurred:
        // 1. We currently exceed the maximum usable size as defined by the cache policy.
        // 2. The oldest generation reported by the clients is older than the oldest permissible generation.
        return exceedsEvictionThreshold()
                || currentStatus.getOldestGeneration() < getOldestPermissibleGeneration();
    }

    @GuardedBy("lock")
    private boolean exceedsEvictionThreshold() {
        return this.lastCacheState.getUsedBytes() > this.policy.getEvictionThreshold();
    }

    @GuardedBy("lock")
    private int getOldestPermissibleGeneration() {
        return this.currentGeneration - this.policy.getMaxGenerations() + 1;
    }

    @GuardedBy("lock")
    private void logCurrentStatus(CacheStatus status) {
        log.info("{} Gen: {}-{}, Clients: {}, Cache: {}.", TRACE_OBJECT_ID, this.currentGeneration, this.oldestGeneration, this.clients.size(), this.lastCacheState);
    }

    private long getStoredBytes() {
        synchronized (this.lock) {
            return this.lastCacheState.getStoredBytes();
        }
    }

    //endregion

    //region Client

    /**
     * Defines a Client that subscribes to the CacheManager.
     */
    public interface Client {
        /**
         * Gets the current Cache Status.
         * @return The current Cache status.
         */
        CacheStatus getCacheStatus();

        /**
         * Called by the CacheManager to notify when there is a generation change (either current or oldest).
         *
         * @param currentGeneration The value of the current generation.
         * @param oldestGeneration  The value of the oldest generation. This is the cutoff for which entries can still
         *                          exist in the cache.
         * @return If any cache data was trimmed with this update.
         */
        boolean updateGenerations(int currentGeneration, int oldestGeneration);
    }

    //endregion

    //region CacheStatus

    /**
     * Represents the current status of the cache for a particular client.
     */
    public static class CacheStatus {
        static final int EMPTY_VALUE = Integer.MAX_VALUE;
        /**
         * The oldest generation found in any cache entry. This value is irrelevant if {@link #isEmpty()} is true.
         */
        @Getter
        private final int oldestGeneration;
        /**
         * The newest generation found in any cache entry. This value is irrelevant if {@link #isEmpty()} is true.
         */
        @Getter
        private final int newestGeneration;

        /**
         * Creates a new instance of the CacheStatus class.
         *
         * @param oldestGeneration The oldest generation found in any cache entry.
         * @param newestGeneration The newest generation found in any cache entry.
         */
        CacheStatus(int oldestGeneration, int newestGeneration) {
            Preconditions.checkArgument(oldestGeneration >= 0, "oldestGeneration must be a non-negative number");
            Preconditions.checkArgument(newestGeneration >= oldestGeneration, "newestGeneration must be larger than or equal to oldestGeneration");
            this.oldestGeneration = oldestGeneration;
            this.newestGeneration = newestGeneration;
        }

        /**
         * Creates a new {@link CacheStatus} instance from the given generations.
         *
         * @param generations An {@link Iterator} containing generations of {@link Client} instances.
         * @return A new {@link CacheStatus} instance having {@link #getOldestGeneration()} and {@link #getNewestGeneration()}
         * set to the minimum value and maximum value, respectively, from `generations`. If `generations` is empty, returns
         * an instance with {@link #isEmpty()} set to true.
         */
        public static CacheStatus fromGenerations(Iterator<Integer> generations) {
            if (!generations.hasNext()) {
                return new CacheStatus(EMPTY_VALUE, EMPTY_VALUE);
            }

            int minGen = EMPTY_VALUE;
            int maxGen = 0;
            while (generations.hasNext()) {
                int g = generations.next();
                minGen = Math.min(minGen, g);
                maxGen = Math.max(maxGen, g);
            }

            return new CacheManager.CacheStatus(minGen, maxGen);
        }

        /**
         * Creates a new {@link CacheStatus} instance from the given {@link CacheStatus} instances.
         *
         * @param cacheStates An {@link Iterator} containing {@link CacheStatus} instances.
         * @return A new {@link CacheStatus} instance having {@link #getOldestGeneration()} set to the minimum value
         * of all {@link #getOldestGeneration()} from `cacheStates` and {@link #getNewestGeneration()} set to the maximum
         * of all {@link #getNewestGeneration()} from `cacheStates`. If `cacheStates` is empty, returns an instance with
         * {@link #isEmpty()} set to true.
         */
        public static CacheStatus combine(Iterator<CacheStatus> cacheStates) {
            if (!cacheStates.hasNext()) {
                return new CacheStatus(EMPTY_VALUE, EMPTY_VALUE);
            }

            int minGen = EMPTY_VALUE;
            int maxGen = 0;
            while (cacheStates.hasNext()) {
                CacheStatus cs = cacheStates.next();
                minGen = Math.min(minGen, cs.getOldestGeneration());
                maxGen = Math.max(maxGen, cs.getNewestGeneration());
            }

            return new CacheManager.CacheStatus(minGen, maxGen);
        }

        /**
         * Gets a value indicating whether this instance contains no useful information (the {@link Client} that generated
         * it has no data in the cache.
         *
         * @return True or false.
         */
        public boolean isEmpty() {
            return this.oldestGeneration == EMPTY_VALUE;
        }

        @Override
        public String toString() {
            return isEmpty() ? "<EMPTY>" : String.format("OG-NG = %d-%d", this.oldestGeneration, this.newestGeneration);
        }
    }

    //endregion
}
