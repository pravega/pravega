/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.util;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is similar to CountDownLatch and Semaphore. Many threads can await() the call of
 * release() (blocking until it is invoked.) After this all calls to await() will not block until
 * reset is called.
 */
public class ReusableLatch {

    private final Semaphore impl;
    private final AtomicBoolean released;
    private final Object releasingLock = new Object();

    public ReusableLatch() {
        this(false);
    }

    public ReusableLatch(boolean startReleased) {
        released = new AtomicBoolean(startReleased);
        if (startReleased) {
            impl = new Semaphore(Integer.MAX_VALUE);
        } else {
            impl = new Semaphore(0);
        }
    }

    /**
     * Block until another thread calls release, or the thread is interrupted.
     *
     * @throws InterruptedException If the operation was interrupted while waiting.
     */
    public void await() throws InterruptedException {
        if (released.get()) {
            return;
        }
        impl.acquire();
    }

    /**
     * Block until another thread calls release, or the thread is interrupted.
     *
     * @param timeoutMillis Timeout, in milliseconds, to wait for the release.
     * @throws InterruptedException If the operation was interrupted while waiting.
     * @throws TimeoutException     If the timeout expired prior to being able to await the release.
     */
    public void await(long timeoutMillis) throws InterruptedException, TimeoutException {
        if (released.get()) {
            return;
        }

        if (!impl.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Timeout expired prior to latch becoming available.");
        }
    }

    /**
     * Block until release is called by another thread.
     */
    public void awaitUninterruptibly() {
        if (released.get()) {
            return;
        }
        impl.acquireUninterruptibly();
    }

    /**
     * Allow all waiting threads to go through, and all future threads to proceed without blocking.
     */
    public void release() {
        if (released.compareAndSet(false, true)) {
            synchronized (releasingLock) {
                if (released.get()) {
                    impl.release(Integer.MAX_VALUE);
                }
            }
        }
    }

    /**
     * Returns whether or not release has been called and threads can call await without blocking.
     *
     * @return True if the latch is set to release state.
     */
    public boolean isReleased() {
        return released.get();
    }

    /**
     * Resets the latch to an un-release state.
     */
    public void reset() {
        if (released.compareAndSet(true, false)) {
            synchronized (releasingLock) {
                if (!released.get()) {
                    impl.drainPermits();
                }
            }
        }
    }

    /**
     * Gets the number of threads waiting.
     *
     * @return The number of threads waiting.
     */
    public int getQueueLength() {
        return this.impl.getQueueLength();
    }

    @Override
    public String toString() {
        return "LatchReleased: " + released.get();
    }
}
