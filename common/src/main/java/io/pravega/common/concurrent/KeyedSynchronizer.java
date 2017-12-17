/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.concurrent;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.Exceptions;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;

/**
 * A synchronization mechanism that allows non-reentrant locking on a particular key.
 * <p>
 * Traditional locks (such as the Java "synchronized" keyword or the primitives in java.util.concurrent) define a general
 * scope per object (i.e., it's either locked or not locked). KeyedSynchronizer allows a finer grained synchronization,
 * based on the provided key. For the same key, it is guaranteed that only one thread will be holding that lock. Keys are
 * independent of each other so holding the lock for one key will not prevent acquiring the lock for another.
 * <p>
 * This is similar to a simple map of key to a corresponding lock, however, KeyedSynchronizer takes care of the mapping
 * and cleanup as needed (when there are no more threads waiting for a particular key, its entry is removed to prevent
 * unnecessary accumulations).
 *
 * @param <KeyT> Type of key.
 */
@ThreadSafe
public class KeyedSynchronizer<KeyT> {
    //region Members

    @GuardedBy("locks")
    private final HashMap<KeyT, Lock> locks;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the KeyedSynchronizer.
     */
    public KeyedSynchronizer() {
        this.locks = new HashMap<>();
    }

    //endregion

    //region Locking

    /**
     * Acquires a lock for a given key. This method will block synchronously if another thread already holds the lock
     * for this key, until the lock becomes available and it is able to acquire it. To release the lock, the calling
     * code needs to call close() on the returned object
     * Example usage:
     * <p>
     * try (KeyedSynchronizer.LockDelegate lock = keyedSync.lock(myKey)) {
     * // Do stuff here while holding the lock
     * }
     * <p>
     * More advanced usage patterns can be employed as long as LockDelegate.close() is invoked when the lock is no
     * longer needed.
     *
     * @param key The key to acquire the lock for.
     * @return A new LockDelegate instance that can be used to release the lock.
     * @throws IllegalMonitorStateException If the current thread already owns the lock.
     */
    public LockDelegate lock(KeyT key) {
        Lock lock;
        synchronized (this.locks) {
            lock = this.locks.get(key);
            if (lock == null) {
                // Nobody else has a lock; create one.
                lock = new Lock();
                this.locks.put(key, lock);
            }

            // Record the fact that we have a new request about to lock. We need to do this while holding this lock
            // (vs during the synchronized lock() method below) in order to maintain consistency (we need to inspect
            // it without trying to acquire/release the lock.
            lock.incrementCount();
        }

        try {
            // Acquire the lock.
            Exceptions.handleInterrupted(lock::lock);
        } catch (Throwable ex) {
            if (!Exceptions.mustRethrow(ex)) {
                // Something didn't work. Record the fact that this thread isn't going to wait anymore and perform any
                // necessary cleanups.
                synchronized (this.locks) {
                    if (lock.decrementCount() <= 0) {
                        this.locks.remove(key);
                    }
                }
            }

            throw ex;
        }

        return new LockDelegate(key);
    }

    /**
     * Releases the lock for the given key.
     *
     * @param key The key to release the lock for.
     * @throws IllegalMonitorStateException If the current thread does not own the lock.
     */
    private void unlock(KeyT key) {
        Lock lock;
        synchronized (this.locks) {
            lock = this.locks.get(key);
        }

        if (lock != null) {
            lock.unlock();
            synchronized (this.locks) {
                if (lock.decrementCount() <= 0) {
                    this.locks.remove(key);
                }
            }
        }
    }

    /**
     * Gets the current number of registered locks.
     *
     * @return The number of locks.
     */
    @VisibleForTesting
    public int getLockCount() {
        synchronized (this.locks) {
            return this.locks.size();
        }
    }

    //endregion

    //region Lock

    /**
     * A simple non-reentrant lock that allows a single thread to own the lock at any given time.
     */
    private class Lock {
        @GuardedBy("this")
        private Thread ownerThread;
        @GuardedBy("locks")
        private int count = 0;

        /**
         * Increments the count of waiting threads.
         */
        @GuardedBy("locks")
        void incrementCount() {
            this.count++;
        }

        /**
         * Decrements the count of waiting threads.
         *
         * @return The current count.
         */
        @GuardedBy("locks")
        int decrementCount() {
            return --this.count;
        }

        /**
         * Acquires the lock. This method makes use of built-in Java synchronization primitives. Only one thread can
         * enter the lock() or unlock() methods at any given time, and the lock() method will block synchronously until
         * it is notified by the current lock owner that it may attempt to acquire the lock (while blocking it will
         * release the lock it holds).
         *
         * @throws IllegalMonitorStateException If the current thread already owns the lock.
         * @throws InterruptedException         If the current thread was interrupted while waiting for the lock to be acquired.
         */
        synchronized void lock() throws InterruptedException {
            Thread currentThread = Thread.currentThread();
            if (this.ownerThread == currentThread) {
                // This thread already owns the lock.
                throw new IllegalMonitorStateException("Thread '" + currentThread + "' already owns this lock.");
            }

            // Since there can be more than one thread waiting, we need to run in a loop until we are able to acquire
            // the lock.
            do {
                if (this.ownerThread == null) {
                    // We have successfully acquired the lock.
                    this.ownerThread = currentThread;
                } else {
                    // Some other thread has the lock. Wait on it to release it.
                    wait();
                }
            } while (this.ownerThread != currentThread);
        }

        /**
         * Releases the lock and notifies any waiting threads that they may attempt to re-acquire the lock.
         */
        synchronized void unlock() {
            Thread current = Thread.currentThread();
            if (this.ownerThread != current) {
                // This thread does not own this lock.
                throw new IllegalMonitorStateException("Thread '" + current + "' does not own this lock.");
            } else {
                // Release the lock and notify any threads that are waiting.
                this.ownerThread = null;
                notify();
            }
        }
    }

    //endregion

    //region LockDelegate

    /**
     * A delegate for a lock for a particular key. This can be used by the consuming code to release the lock, by calling
     * close() on it.
     */
    @RequiredArgsConstructor
    public final class LockDelegate implements AutoCloseable {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final KeyT key;

        @Override
        protected void finalize() {
            close();
        }

        @Override
        public void close() {
            if (this.closed.compareAndSet(false, true)) {
                KeyedSynchronizer.this.unlock(this.key);
            }
        }
    }

    //endregion
}