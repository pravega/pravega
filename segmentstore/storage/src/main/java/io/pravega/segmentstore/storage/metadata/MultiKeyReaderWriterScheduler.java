/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

/**
 * A scheduler utility that implements pattern similar to Multiple Readers - Single Writer pattern.
 */
public class MultiKeyReaderWriterScheduler {
    /**
     * Scheduler data for each key.
     */
    @GuardedBy("keyToDataMap")
    private final HashMap<String, SchedulerData> keyToDataMap = new HashMap<>();

    /**
     * Scheduler data for each key.
     */
    private static class SchedulerData {
        int count;
        CompletableFuture blockingFuture = CompletableFuture.completedFuture(null);
        ArrayList<CompletableFuture> readerFutures = new ArrayList<>();
    }

    /**
     * Represents a lock.
     */
    @RequiredArgsConstructor
    static class MultiKeyReaderWriterAsyncLock {
        /**
         * Keys to synchronize on.
         */
        @Getter
        private final String[] keys;

        /**
         * Indicates whether the lock is a reader lock or a writer lock.
         */
        @Getter
        private final boolean isReadonly;

        /**
         * Reference to the scheduler.
         */
        private final MultiKeyReaderWriterScheduler scheduler;

        /**
         * The future is completed when all keys for this lock become available.
         */
        private CompletableFuture<Void> readyFuture;

        /**
         * The future which is completed when lock is released.
         */
        @Getter
        private final CompletableFuture<Void> doneFuture = new CompletableFuture<>();

        /**
         * Returns a CompletableFuture that will be completed when the lock is obtained.
         * @return CompletableFuture which will be complete when the lock is obtained.
         */
        CompletableFuture<Void> lock() {
            if (isReadonly) {
                return scheduler.scheduleForRead(this);
            } else {
                return scheduler.scheduleForWrite(this);
            }
        }

        /**
         * Releases the lock.
         */
        void unlock() {
            scheduler.release(this);
        }
    }

    /**
     * Adds a reference for the key.
     */
    private SchedulerData addReference(String key) {
        synchronized (keyToDataMap) {
            SchedulerData schedulerData = keyToDataMap.get(key);
            // Add if this is a new key.
            if (null == schedulerData) {
                schedulerData = new SchedulerData();
                keyToDataMap.put(key, schedulerData);
            }
            // Increment ref count.
            schedulerData.count++;
            return schedulerData;
        }
    }

    /**
     * Releases a reference for the key.
     */
    private void releaseReference(String key) {
        synchronized (keyToDataMap) {
            SchedulerData schedulerData = keyToDataMap.get(key);
            // Decrement ref count.
            schedulerData.count--;
            // clean up if required.
            if (0 == schedulerData.count) {
                keyToDataMap.remove(key);
            }
        }
    }

    /**
     * Gets a read lock over given set of keys.
     */
    MultiKeyReaderWriterAsyncLock getReadLock(String[] keys) {
        return new MultiKeyReaderWriterAsyncLock(keys, true, this);
    }

    /**
     * Gets a write lock over given set of keys.
     */
    MultiKeyReaderWriterAsyncLock getWriteLock(String[] keys) {
        return new MultiKeyReaderWriterAsyncLock(keys, false, this);
    }

    /**
     * Schedules the lock for read.
     */
    private synchronized CompletableFuture<Void> scheduleForRead(MultiKeyReaderWriterAsyncLock lock) {
        val futuresToBlockOn = new CompletableFuture[lock.getKeys().length];
        for (int i = 0; i < lock.getKeys().length; i++) {
            val key = lock.getKeys()[i];
            SchedulerData schedulerData = addReference(key);

            futuresToBlockOn[i] = schedulerData.blockingFuture;
            // Add this as reader
            schedulerData.readerFutures.add(lock.doneFuture);
        }
        lock.readyFuture = CompletableFuture.allOf(futuresToBlockOn);
        return lock.readyFuture;
    }

    /**
     * Schedules the lock for write.
     */
    private synchronized CompletableFuture<Void> scheduleForWrite(MultiKeyReaderWriterAsyncLock lock) {
        val futuresToBlockOn = new CompletableFuture[lock.getKeys().length];
        for (int i = 0; i < lock.getKeys().length; i++) {
            val key = lock.getKeys()[i];
            // Get existing data.
            SchedulerData schedulerData = addReference(key);

            // If there are outstanding readers then first "drain" all readers by making this write wait on them.
            if (schedulerData.readerFutures.size() > 0) {
                CompletableFuture[] readFutures = schedulerData.readerFutures.toArray(new CompletableFuture[schedulerData.readerFutures.size()]);
                schedulerData.blockingFuture = CompletableFuture.allOf(readFutures);
                // Empty readers
                schedulerData.readerFutures = new ArrayList<>();
            }

            // Add it to list of futures to block on.
            futuresToBlockOn[i] = schedulerData.blockingFuture;

            // Make completion of this lock's release as future on which all new requests on this key will be waiting/blocking.
            schedulerData.blockingFuture = lock.doneFuture;
        }
        // The code in lock is run when all pending futures are complete.
        lock.readyFuture = CompletableFuture.allOf(futuresToBlockOn);

        return lock.readyFuture;
    }

    /**
     * Releases the lock.
     * @param lock Lock to release.
     */
    void release(MultiKeyReaderWriterAsyncLock lock) {
        for (val key: lock.getKeys()) {
            releaseReference(key);
        }
        lock.doneFuture.complete(null);
    }
}
