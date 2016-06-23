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

package com.emc.logservice.common;

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

/**
 * Wrapper for a Lock that auto-acquires the lock upon creation and releases it upon closing. This class is meant to
 * be used in try-with resources blocks.
 * <p>
 * Note: This class should be used with a ReadWriteAutoReleaseLock.
 */
public class AutoReleaseLock implements AutoCloseable {
    //region Members

    private final Lock lock;
    private final AutoReleaseLock replacedLock;
    private boolean closed;
    private boolean upgradeable;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AutoReleaseLock class.
     *
     * @param baseLock    The underlying lock to wrap.
     * @param upgradeable Whether this lock is upgradeable or not.
     */

    protected AutoReleaseLock(Lock baseLock, boolean upgradeable) {
        Preconditions.checkNotNull(baseLock, "baseLock");

        this.replacedLock = null;
        this.lock = baseLock;
        this.upgradeable = upgradeable;
        this.lock.lock();
    }

    /**
     * Creates a new instance of the AutoReleaseLock class.
     *
     * @param baseLock       The underlying lock to wrap.
     * @param upgradeable    Whether this lock is upgradeable or not.
     * @param acquireTimeout The amount of time to wait for acquisition of the lock, if not immediately available.
     * @throws TimeoutException     If the timeout expired prior to the acquisition of the lock.
     * @throws InterruptedException If the thread got interrupted while waiting for the lock to be acquired.
     */
    protected AutoReleaseLock(Lock baseLock, boolean upgradeable, Duration acquireTimeout) throws TimeoutException, InterruptedException {
        Preconditions.checkNotNull(baseLock, "baseLock");

        this.replacedLock = null;
        this.lock = baseLock;
        this.upgradeable = upgradeable;
        if (!this.lock.tryLock(acquireTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Unable to acquire lock within specified time frame.");
        }
    }

    /**
     * Creates a new instance of the AutoReleaseLock class that upgrades an existing lock.
     *
     * @param replacedLock A lock to replace. This lock will be suspended until the newly created AutoReleaseLock is unlocked,
     *                     at which point it will be automatically re-acquired.
     * @param baseLock     The underlying lock to wrap.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the given replacedLock is not upgradeable.
     */
    protected AutoReleaseLock(AutoReleaseLock replacedLock, Lock baseLock) {
        Preconditions.checkNotNull(baseLock, "baseLock");
        Preconditions.checkNotNull(replacedLock, "replacedLock");
        Exceptions.checkArgument(replacedLock.upgradeable, "replacedLock", "replacedLock is not upgradeable.");

        this.lock = baseLock;
        this.upgradeable = false;
        this.replacedLock = replacedLock;
        replacedLock.surrender();

        try {
            this.lock.lock();
        } catch (Exception ex) {
            this.replacedLock.reacquire();
            throw ex;
        }
    }

    /**
     * Creates a new instance of the AutoReleaseLock class that upgrades an existing lock.
     *
     * @param replacedLock   A lock to replace. This lock will be suspended until the newly created AutoReleaseLock is unlocked,
     *                       at which point it will be automatically re-acquired.
     * @param baseLock       The underlying lock to wrap.
     * @param acquireTimeout The amount of time to wait for acquisition of the lock, if not immediately available.
     * @throws TimeoutException         If the timeout expired prior to the acquisition of the lock.
     * @throws InterruptedException     If the thread got interrupted while waiting for the lock to be acquired.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the given replacedLock is not upgradeable.
     */
    protected AutoReleaseLock(AutoReleaseLock replacedLock, Lock baseLock, Duration acquireTimeout) throws TimeoutException, InterruptedException {
        Preconditions.checkNotNull(baseLock, "baseLock");
        Preconditions.checkNotNull(replacedLock, "replacedLock");
        Exceptions.checkArgument(replacedLock.upgradeable, "replacedLock", "replacedLock is not upgradeable.");

        this.lock = baseLock;
        this.upgradeable = false;
        this.replacedLock = replacedLock;
        replacedLock.surrender();

        try {
            if (!this.lock.tryLock(acquireTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("Unable to acquire lock within specified time frame.");
            }
        } catch (Exception ex) {
            this.replacedLock.reacquire();
            throw ex;
        }
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed) {
            // Nothing to do.
            return;
        }

        this.lock.unlock();
        if (this.replacedLock != null) {
            this.replacedLock.reacquire();
        }

        this.closed = false;
    }

    //endregion

    //region Lock Operations

    public boolean isUpgradeable() {
        return this.upgradeable;
    }

    private void surrender() {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        this.lock.unlock();
    }

    private void reacquire() {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        this.lock.lock();
    }

    //endregion
}