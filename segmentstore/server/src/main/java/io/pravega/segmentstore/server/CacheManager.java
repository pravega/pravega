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
import io.pravega.segmentstore.storage.cache.CacheSnapshot;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
public class CacheManager extends AbstractScheduledService implements AutoCloseable, CacheUtilizationProvider {
    //region Members

    private static final String TRACE_OBJECT_ID = "CacheManager";
    @GuardedBy("clients")
    private final Collection<Client> clients;
    private final ScheduledExecutorService executorService;
    private final AtomicInteger currentGeneration;
    private final AtomicInteger oldestGeneration;
    private final AtomicReference<CacheSnapshot> lastSnapshot;
    private final CachePolicy policy;
    private final AtomicBoolean closed;
    private final SegmentStoreMetrics.CacheManager metrics;
    @Getter
    private final CacheStorage cacheStorage;

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
        Preconditions.checkNotNull(policy, "policy");
        Preconditions.checkNotNull(cacheStorage, "cacheStorage");
        Preconditions.checkNotNull(executorService, "executorService");

        this.policy = policy;
        this.clients = new HashSet<>();
        this.oldestGeneration = new AtomicInteger();
        this.currentGeneration = new AtomicInteger();
        this.executorService = executorService;
        this.closed = new AtomicBoolean();
        this.cacheStorage = cacheStorage;
        this.lastSnapshot = new AtomicReference<>(this.cacheStorage.getSnapshot());
        this.metrics = new SegmentStoreMetrics.CacheManager();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (state() == State.RUNNING) {
                Futures.await(Services.stopAsync(this, this.executorService));
            }

            synchronized (this.clients) {
                this.clients.clear();
            }

            this.cacheStorage.close();
            this.metrics.close();
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
        if (this.closed.get()) {
            // We are done.
            return;
        }

        try {
            applyCachePolicy();
        } catch (Throwable ex) {
            if (Exceptions.mustRethrow(ex)) {
                throw ex;
            }

            // Log the error and move on. If we don't catch the exception here, the AbstractScheduledService will
            // auto-shutdown.
            log.error("{}: Error.", TRACE_OBJECT_ID, ex);
        }
    }

    @Override
    protected Scheduler scheduler() {
        long millis = this.policy.getGenerationDuration().toMillis();
        return Scheduler.newFixedDelaySchedule(millis, millis, TimeUnit.MILLISECONDS);
    }

    //endregion

    //region CacheUtilizationProvider Implementation

    @Override
    public double getCacheUtilization() {
        // We use the total number of used bytes, which includes any overhead. This will provide a more accurate
        // representation of the utilization than just the Stored Bytes.
        return (double) getCacheUsedBytes() / this.policy.getMaxSize();
    }

    @Override
    public double getCacheMaxUtilization() {
        return this.policy.getMaxUtilization();
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
        synchronized (this.clients) {
            if (!this.clients.contains(client)) {
                this.clients.add(client);
                client.updateGenerations(this.currentGeneration.get(), this.oldestGeneration.get());
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
        synchronized (this.clients) {
            this.clients.remove(client);
        }

        log.info("{} Unregistered {}.", TRACE_OBJECT_ID, client);
    }

    //endregion

    //region Helpers

    protected void applyCachePolicy() {
        // Run through all the active clients and gather status.
        CacheStatus currentStatus = collectStatus();
        fetchSnapshot();
        if (currentStatus == null || this.lastSnapshot.get().getStoredBytes() == 0) {
            // We either have no clients or we have clients and they do not have any data stored.
            return;
        }

        // Increment current generation (if needed).
        boolean currentChanged = adjustCurrentGeneration(currentStatus);

        // Increment oldest generation (if needed and if possible).
        boolean oldestChanged = adjustOldestGeneration(currentStatus);

        if (!currentChanged && !oldestChanged) {
            // Nothing changed, nothing to do.
            return;
        }

        // Notify clients that something changed (if any of the above got changed). Run in a loop, until either we can't
        // adjust the oldest anymore or we are unable to trigger any changes to the clients.
        boolean reduced;
        do {
            reduced = updateClients();
            if (reduced) {
                fetchSnapshot();
                logCurrentStatus(currentStatus);
                oldestChanged = adjustOldestGeneration(currentStatus);
            }
        } while (reduced && oldestChanged);
        this.metrics.report(this.lastSnapshot.get().getAllocatedBytes(), currentStatus.getNewestGeneration() - currentStatus.getOldestGeneration());
    }

    private CacheStatus collectStatus() {
        int cg = this.currentGeneration.get();
        int minGeneration = cg;
        int maxGeneration = 0;
        Collection<Client> clients = getCurrentClients();
        for (Client c : clients) {
            CacheStatus clientStatus;
            try {
                clientStatus = c.getCacheStatus();
            } catch (ObjectClosedException ex) {
                // This object was closed but it was not unregistered. Do it now.
                log.warn("{} Detected closed client {}.", TRACE_OBJECT_ID, c);
                unregister(c);
                continue;
            }

            if (clientStatus.oldestGeneration > cg || clientStatus.newestGeneration > cg) {
                log.warn("{} Client {} returned status that is out of bounds {}. CurrentGeneration = {}, OldestGeneration = {}.",
                        TRACE_OBJECT_ID, c, clientStatus, this.currentGeneration, this.oldestGeneration);
            }

            minGeneration = Math.min(minGeneration, clientStatus.oldestGeneration);
            maxGeneration = Math.max(maxGeneration, clientStatus.newestGeneration);
        }

        if (minGeneration > maxGeneration) {
            // Either no clients or clients are empty.
            return null;
        }

        return new CacheStatus(minGeneration, maxGeneration);
    }

    private void fetchSnapshot() {
        this.lastSnapshot.set(this.cacheStorage.getSnapshot());
    }

    private boolean updateClients() {
        boolean reduced = false;
        int cg = this.currentGeneration.get();
        int og = this.oldestGeneration.get();
        for (Client c : getCurrentClients()) {
            try {
                reduced = c.updateGenerations(cg, og) | reduced;
            } catch (ObjectClosedException ex) {
                // This object was closed but it was not unregistered. Do it now.
                log.warn("{} Detected closed client {}.", TRACE_OBJECT_ID, c);
                unregister(c);
            } catch (Throwable ex) {
                if (Exceptions.mustRethrow(ex)) {
                    throw ex;
                }

                log.warn("{} Unable to update client {}.", TRACE_OBJECT_ID, c, ex);
            }
        }

        return reduced;
    }

    private boolean adjustCurrentGeneration(CacheStatus currentStatus) {
        // We only need to increment if we had any activity in the current generation. This can be determined by comparing
        // the current generation with the newest generation from the retrieved status.
        boolean shouldIncrement = currentStatus.getNewestGeneration() >= this.currentGeneration.get();
        if (shouldIncrement) {
            this.currentGeneration.incrementAndGet();
        }

        return shouldIncrement;
    }

    private boolean adjustOldestGeneration(CacheStatus currentStatus) {
        // Figure out if we exceed the policy criteria.
        int newOldestGeneration = this.oldestGeneration.get();
        if (exceedsPolicy(currentStatus)) {
            // Start by setting the new value to the smallest reported value, and increment by one.
            newOldestGeneration = Math.max(newOldestGeneration, currentStatus.oldestGeneration) + 1;

            // Then factor in the oldest permissible generation.
            newOldestGeneration = Math.max(newOldestGeneration, getOldestPermissibleGeneration());

            // Then make sure we don't exceed the current generation.
            newOldestGeneration = Math.min(newOldestGeneration, this.currentGeneration.get());
        }

        boolean isAdjusted = newOldestGeneration > this.oldestGeneration.get();
        if (isAdjusted) {
            this.oldestGeneration.set(newOldestGeneration);
        }

        return isAdjusted;
    }

    private boolean exceedsPolicy(CacheStatus currentStatus) {
        // We need to increment the OldestGeneration only if any of the following conditions occurred:
        // 1. We currently exceed the maximum usable size as defined by the cache policy.
        // 2. The oldest generation reported by the clients is older than the oldest permissible generation.
        return getCacheUsedBytes() > this.policy.getMaxUsableSize()
                || currentStatus.getOldestGeneration() < getOldestPermissibleGeneration();
    }

    private long getCacheUsedBytes() {
        return this.lastSnapshot.get().getUsedBytes();
    }

    private int getOldestPermissibleGeneration() {
        return this.currentGeneration.get() - this.policy.getMaxGenerations() + 1;
    }

    private Collection<Client> getCurrentClients() {
        synchronized (this.clients) {
            return new ArrayList<>(this.clients);
        }
    }

    private void logCurrentStatus(CacheStatus status) {
        int size;
        synchronized (this.clients) {
            size = this.clients.size();
        }

        log.info("{} Gen: {}-{}, Clients: {}, Cache: {}.", TRACE_OBJECT_ID, this.currentGeneration, this.oldestGeneration, size, this.lastSnapshot);
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
        /**
         * The oldest generation found in any cache entry.
         */
        @Getter
        private final int oldestGeneration;
        /**
         * The newest generation found in any cache entry.
         */
        @Getter
        private final int newestGeneration;

        /**
         * Creates a new instance of the CacheStatus class.
         *
         * @param oldestGeneration The oldest generation found in any cache entry.
         * @param newestGeneration The newest generation found in any cache entry.
         */
        public CacheStatus(int oldestGeneration, int newestGeneration) {
            Preconditions.checkArgument(oldestGeneration >= 0, "oldestGeneration must be a non-negative number");
            Preconditions.checkArgument(newestGeneration >= oldestGeneration, "newestGeneration must be larger than or equal to oldestGeneration");
            this.oldestGeneration = oldestGeneration;
            this.newestGeneration = newestGeneration;
        }

        @Override
        public String toString() {
            return String.format("OG-NG = %d-%d", this.oldestGeneration, this.newestGeneration);
        }
    }

    //endregion
}
