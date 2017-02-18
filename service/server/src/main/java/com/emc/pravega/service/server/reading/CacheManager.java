/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.ObjectClosedException;
import com.emc.pravega.common.concurrent.ServiceShutdownListener;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages the lifecycle of Cache Entries. Decides which entries are to be kept in memory and which are eligible for
 * removal.
 * <p/>
 * Entry Management is indirect, and doesn't deal with them directly. Also, the management needs to be done across multiple
 * CacheManager Clients, and a common scheme needs to be used to instruct each such client when it's time to evict unused entries.
 * <p/>
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
    @GuardedBy("clients")
    private final Collection<Client> clients;
    private final ScheduledExecutorService executorService;
    private int currentGeneration;
    private int oldestGeneration;
    private final CachePolicy policy;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the CacheManager class.
     *
     * @param policy          The policy to use with this CacheManager.
     * @param executorService An executorService to use for scheduled tasks.
     */
    public CacheManager(CachePolicy policy, ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(policy, "policy");
        Preconditions.checkNotNull(executorService, "executorService");
        this.policy = policy;
        this.clients = new HashSet<>();
        this.oldestGeneration = 0;
        this.currentGeneration = 0;
        this.executorService = executorService;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            if (state() == State.RUNNING) {
                stopAsync();
                ServiceShutdownListener.awaitShutdown(this, false);
            }

            this.closed = true;
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
        Exceptions.checkNotClosed(this.closed, this);

        try {
            applyCachePolicy();
        } catch (Throwable ex) {
            if (ExceptionHelpers.mustRethrow(ex)) {
                throw ex;
            }

            // Log the error and move on. If we don't catch the exception here, the AbstractScheduledService will
            // auto-shutdown.
            log.error("{}: Error {}.", TRACE_OBJECT_ID, ex);
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
    void register(Client client) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkNotNull(client, "client");
        synchronized (this.clients) {
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
    void unregister(Client client) {
        Exceptions.checkNotClosed(this.closed, this);
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
        if (currentStatus == null || currentStatus.getSize() == 0) {
            // This indicates we have no clients or those clients have no data.
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
        long sizeReduction;
        do {
            sizeReduction = updateClients();
            if (sizeReduction > 0) {
                currentStatus = currentStatus.withUpdatedSize(-sizeReduction);
                logCurrentStatus(currentStatus);
                oldestChanged = adjustOldestGeneration(currentStatus);
            }
        } while (sizeReduction > 0 && oldestChanged);
    }

    private CacheStatus collectStatus() {
        int minGeneration = this.currentGeneration;
        int maxGeneration = 0;
        long totalSize = 0;
        Collection<Client> clients = getCurrentClients();
        if (clients.size() == 0) {
            return null;
        }

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

            if (clientStatus.getSize() == 0) {
                // Nothing interesting in this client.
                continue;
            }

            totalSize += clientStatus.getSize();
            if (clientStatus.oldestGeneration > this.currentGeneration || clientStatus.newestGeneration > this.currentGeneration) {
                log.warn("{} Client {} returned status that is out of bounds {}. CurrentGeneration = {}, OldestGeneration = {}.", TRACE_OBJECT_ID, c, clientStatus, this.currentGeneration, this.oldestGeneration);
            }

            minGeneration = Math.min(minGeneration, clientStatus.oldestGeneration);
            maxGeneration = Math.max(maxGeneration, clientStatus.newestGeneration);
        }

        return new CacheStatus(totalSize, minGeneration, maxGeneration);
    }

    private long updateClients() {
        long sizeReduction = 0;
        for (Client c : getCurrentClients()) {
            try {
                sizeReduction += Math.max(0, c.updateGenerations(this.currentGeneration, this.oldestGeneration));
            } catch (ObjectClosedException ex) {
                // This object was closed but it was not unregistered. Do it now.
                log.warn("{} Detected closed client {}.", TRACE_OBJECT_ID, c);
                unregister(c);
            } catch (Throwable ex) {
                if (ExceptionHelpers.mustRethrow(ex)) {
                    throw ex;
                }

                log.warn("{} Unable to update client {}. {}", TRACE_OBJECT_ID, c, ex);
            }
        }

        return sizeReduction;
    }

    private boolean adjustCurrentGeneration(CacheStatus currentStatus) {
        // We only need to increment if we had any activity in the current generation. This can be determined by comparing
        // the current generation with the newest generation from the retrieved status.
        boolean shouldIncrement = currentStatus.getNewestGeneration() >= this.currentGeneration;
        if (shouldIncrement) {
            this.currentGeneration++;
        }

        return shouldIncrement;
    }

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

    private boolean exceedsPolicy(CacheStatus currentStatus) {
        // We need to increment the OldestGeneration only if any of the following conditions occurred:
        // 1. We currently exceed the maximum size as defined by the cache policy.
        // 2. The oldest generation reported by the clients is older than the oldest permissible generation.
        return currentStatus.getSize() > this.policy.getMaxSize()
                || currentStatus.getOldestGeneration() < getOldestPermissibleGeneration();
    }

    private int getOldestPermissibleGeneration() {
        return this.currentGeneration - this.policy.getMaxGenerations() + 1;
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

        log.info("{} Current Generation = {}, Oldest Generation = {}, Clients = {},  CacheSize = {} MB",
                TRACE_OBJECT_ID,
                this.currentGeneration,
                this.oldestGeneration,
                size,
                status.getSize() / 1048576);
    }

    //endregion

    //region Client

    /**
     * Defines a Client that subscribes to the CacheManager.
     */
    interface Client {
        /**
         * Gets the current Cache Status.
         */
        CacheStatus getCacheStatus();

        /**
         * Called by the CacheManager to notify when there is a generation change (either current or oldest).
         *
         * @param currentGeneration The value of the current generation.
         * @param oldestGeneration  The value of the oldest generation. This is the cutoff for which entries can still
         *                          exist in the cache.
         * @return The total size of the cache data that was trimmed by this update.
         */
        long updateGenerations(int currentGeneration, int oldestGeneration);
    }

    //endregion

    //region CacheStatus

    /**
     * Represents the current status of the cache for a particular client.
     */
    static class CacheStatus {
        private final int oldestGeneration;
        private final int newestGeneration;
        private final long size;

        /**
         * Creates a new instance of the CacheStatus class.
         *
         * @param size The total size of the cache items in this particular client.
         */
        CacheStatus(long size, int oldestGeneration, int newestGeneration) {
            Preconditions.checkArgument(size >= 0, "size must be a non-negative number");
            Preconditions.checkArgument(oldestGeneration >= 0, "oldestGeneration must be a non-negative number");
            Preconditions.checkArgument(newestGeneration >= oldestGeneration, "newestGeneration must be larger than or equal to oldestGeneration");
            this.size = size;
            this.oldestGeneration = oldestGeneration;
            this.newestGeneration = newestGeneration;
        }

        /**
         * Gets a value indicating the total size of the cache items in this particular client.
         */
        long getSize() {
            return this.size;
        }

        /**
         * Gets a value indicating the oldest generation found in any cache entry.
         */
        int getOldestGeneration() {
            return this.oldestGeneration;
        }

        /**
         * Gets a value indicating the newest generation found in any cache entry.
         */
        int getNewestGeneration() {
            return this.newestGeneration;
        }

        private CacheStatus withUpdatedSize(long sizeDelta) {
            long newSize = this.size + sizeDelta;
            assert newSize >= 0 : "given sizeDelta would result in a negative size";
            return new CacheStatus(newSize, this.oldestGeneration, this.newestGeneration);
        }

        @Override
        public String toString() {
            return String.format("Size = %d, OG-NG = %d-%d", this.size, this.oldestGeneration, this.newestGeneration);
        }
    }

    //endregion
}
