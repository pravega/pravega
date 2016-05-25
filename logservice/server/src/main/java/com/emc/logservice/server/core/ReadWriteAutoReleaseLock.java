package com.emc.logservice.server.core;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.*;

/**
 * Simplified version of the ReentrantReadWriteLock that keeps track internally of all different locks.
 * Also allows "Upgrading" from one lock to another.
 */
public class ReadWriteAutoReleaseLock {
    //region Members

    private final ReadWriteLock readWriteLock;
    private final Lock readLock;
    private final Lock writeLock;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadWriteAutoReleaseLock with a (default) non-fair ordering policy.
     */
    public ReadWriteAutoReleaseLock() {
        this(false);
    }

    /**
     * Creates a new instance of the ReadWriteAutoReleaseLock.
     *
     * @param fair true if this lock should use a fair ordering policy (ReentrantReadWriteLock constructor).
     */
    public ReadWriteAutoReleaseLock(boolean fair) {
        this.readWriteLock = new ReentrantReadWriteLock(fair);
        this.readLock = this.readWriteLock.readLock();
        this.writeLock = this.readWriteLock.writeLock();
    }

    //endregion

    //region Lock Acquisition

    /**
     * Acquires a Read Lock. Blocks the current thread until it is able to acquire it.
     * Note: Read Locks are upgradeable to Write Locks.
     *
     * @return An AutoReleaseLock, that, when closed, releases the lock.
     */
    public AutoReleaseLock acquireReadLock() {
        return acquireLock(this.readLock, true);
    }

    /**
     * Attempts to acquire a Read Lock using the specified timeout. Blocks the current thread until it is able to
     * acquire it or until the timeout expires or the thread is interrupted.
     * Note: Read Locks are upgradeable to Write Locks.
     *
     * @param timeout The timeout for the operation.
     * @return An AutoReleaseLock, that, when closed, releases the lock.
     * @throws InterruptedException If the thread got interrupted while waiting.
     * @throws TimeoutException     If the timeout expired prior to completing this operation.
     */
    public AutoReleaseLock acquireReadLock(Duration timeout) throws InterruptedException, TimeoutException {
        return acquireLock(this.readLock, true, timeout);
    }

    /**
     * Acquires a Write Lock. Blocks the current thread until it is able to acquire it.
     * Note: Write Locks are non-upgradeable.
     *
     * @return An AutoReleaseLock, that, when closed, releases the lock.
     */
    public AutoReleaseLock acquireWriteLock() {
        return acquireLock(this.writeLock, false);
    }

    /**
     * Attempts to acquire a Write Lock using the specified timeout. Blocks the current thread until it is able to
     * acquire it or until the timeout expires or the thread is interrupted.
     * Note: Write Locks are non-upgradeable.
     *
     * @param timeout The timeout for the operation.
     * @return An AutoReleaseLock, that, when closed, releases the lock.
     * @throws InterruptedException If the thread got interrupted while waiting.
     * @throws TimeoutException     If the timeout expired prior to completing this operation.
     */
    public AutoReleaseLock acquireWriteLock(Duration timeout) throws InterruptedException, TimeoutException {
        return acquireLock(this.writeLock, false, timeout);
    }

    /**
     * Upgrades the given lock to a Write Lock. Blocks the current thread until it is able to acquire it.
     * When the Write Lock is released (or closed), the suspended Lock is reacquired automatically.
     *
     * @param replacedLock A Lock to replace.
     * @return An AutoReleaseLock, that, when closed, releases the lock.
     * @throws IllegalArgumentException If the given replacedLock is not upgradeable.
     */
    public AutoReleaseLock upgradeToWriteLock(AutoReleaseLock replacedLock) {
        return upgradeToLock(replacedLock, this.writeLock);
    }

    /**
     * Attempts to acquire a Write Lock using the specified timeout, by suspending the given lock. Blocks the current
     * thread until it is able to acquire it or until the timeout expires or the thread is interrupted.
     * When the Write Lock is released (or closed), the suspended Lock is reacquired automatically.
     *
     * @param replacedLock A Lock to replace.
     * @param timeout      The timeout for the operation.
     * @return An AutoReleaseLock, that, when closed, releases the lock.
     * @throws InterruptedException     If the thread got interrupted while waiting.
     * @throws TimeoutException         If the timeout expired prior to completing this operation.
     * @throws IllegalArgumentException If the given replacedLock is not upgradeable.
     */
    public AutoReleaseLock upgradeToWriteLock(AutoReleaseLock replacedLock, Duration timeout) throws InterruptedException, TimeoutException {
        return upgradeToLock(replacedLock, this.writeLock, timeout);
    }

    private AutoReleaseLock acquireLock(Lock lock, boolean upgradeable) {
        return new AutoReleaseLock(lock, upgradeable);
    }

    private AutoReleaseLock acquireLock(Lock lock, boolean upgradeable, Duration timeout) throws InterruptedException, TimeoutException {
        return new AutoReleaseLock(lock, upgradeable, timeout);
    }

    private AutoReleaseLock upgradeToLock(AutoReleaseLock replacedLock, Lock lock) {
        // AutoReleaseLock constructor will check & throw if replacedLock is not upgradeable.
        return new AutoReleaseLock(replacedLock, lock);
    }

    private AutoReleaseLock upgradeToLock(AutoReleaseLock replacedLock, Lock lock, Duration timeout) throws InterruptedException, TimeoutException {
        // AutoReleaseLock constructor will check & throw if replacedLock is not upgradeable.
        return new AutoReleaseLock(replacedLock, lock, timeout);
    }

    //endregion
}
