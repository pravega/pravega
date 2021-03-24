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

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is similar to {@link ReusableLatch} but that works with {@link CompletableFuture} so
 * that blocking can be async and exceptions and results can be passed.
 * @param <T> The type of the futures that this class works with.
 */
@Slf4j
public class ReusableFutureLatch<T> {
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final ArrayList<CompletableFuture<T>> waitingFutures = new ArrayList<>();
    @GuardedBy("lock")
    private T result;
    @GuardedBy("lock")
    private Throwable e;
    @GuardedBy("lock")
    private boolean released;
    @GuardedBy("lock")
    private Long runningThreadId;

    public ReusableFutureLatch() {
        released = false;
    }

    /**
     * Supply a future to be notified when {@link #release(Object)} is called. If release has already been
     * called, it will be completed immediately.
     * 
     * @param toNotify The future that should be completed.
     */
    public void register(CompletableFuture<T> toNotify) {
        T result;
        Throwable e;
        synchronized (lock) {
            if (released) {
                result = this.result;
                e = this.e;
            } else {
                waitingFutures.add(toNotify);
                return;
            }
        }
        if (e == null) {
            toNotify.complete(result);
        } else {
            toNotify.completeExceptionally(e);
        }
    }
    
    /**
     * If the latch is released, completes the provided future without invoking the provided
     * runnable. If the latch is not released it will add the provided future to the list to be
     * notified and runs the provided runnable if there is not already one running.
     * 
     * If there are multiple calls to this method, only the runnable of one will be invoked. If the
     * runnable throws the exception will be thrown to the caller of this method, and future callers
     * may have their runnable method invoked (Presuming that the latch is not released)
     * 
     * @param willCallRelease A runnable that should result in {@link #release(Object)} being called.
     * @param toNotify The future to notify once release is called.
     */
    public void registerAndRunReleaser(Runnable willCallRelease, CompletableFuture<T> toNotify) {
        boolean run = false;
        boolean complete = false;
        T result = null;
        Throwable e = null;
        synchronized (lock) {
            if (released) {
                complete = true;
                result = this.result;
                e = this.e;
            } else {
                waitingFutures.add(toNotify);
                if (runningThreadId == null) {
                    run = true;
                    runningThreadId = Thread.currentThread().getId();
                }
            }
        }
        if (run) {
            log.debug("Running releaser now, runningThread:{}", Thread.currentThread().getName());
            boolean success = false;
            try {
                willCallRelease.run();
                success = true;
            } finally {
                if (!success) {
                    synchronized (lock) {
                        if (runningThreadId != null && runningThreadId == Thread.currentThread().getId()) {
                            runningThreadId = null;
                        }
                    }
                }
            }
        }
        if (complete) {
            if (e == null) {
                toNotify.complete(result);
            } else {
                toNotify.completeExceptionally(e);
            }
        }
    }

    /**
     * Complete all waiting futures, and all future calls to register be notified immediately. If release is
     * called twice consecutively the second value will be the one passed to future callers of
     * {@link #register(CompletableFuture)}
     * 
     * @param result The result to pass to waiting futures.
     */
    public void release(T result) {
        ArrayList<CompletableFuture<T>> toComplete = null;
        synchronized (lock) {
            if (!waitingFutures.isEmpty()) {
                toComplete = new ArrayList<>(waitingFutures);
                waitingFutures.clear();
            }
            e = null;
            this.result = result;
            released = true;
        }
        if (toComplete != null) {
            for (CompletableFuture<T> f : toComplete) {
                f.complete(result);
            }
        }
    }
    
    /**
     * Complete all waiting futures, and all future calls to register be notified immediately. If release is
     * called twice consecutively the second value will be the one passed to future callers of
     * {@link #register(CompletableFuture)}
     * 
     * @param e The exception to pass to waiting futures.
     */
    public void releaseExceptionally(Throwable e) {
        ArrayList<CompletableFuture<T>> toComplete = null;
        synchronized (lock) {
            if (!waitingFutures.isEmpty()) {
                toComplete = new ArrayList<>(waitingFutures);
                waitingFutures.clear();
            }
            this.e = e;
            this.result = null;
            released = true;
        }
        if (toComplete != null) {
            for (CompletableFuture<T> f : toComplete) {
                f.completeExceptionally(e);
            }
        }
    }

    /**
     * If {@link #release(Object)} or {@link #releaseExceptionally(Throwable)} has been called it
     * resets the object into the unreleased state. If release has not been called this will have no effect.
     */
    public void reset() {
        synchronized (lock) {
            released = false;
            e = null;
            result = null;
            runningThreadId = null;
        }
    }
    
    /**
     * Identical to calling {@code #releaseExceptionally(Exception); #reset()} except it is atomic.
     * @param e The exception to fail all waiting futures with.
     */
    public void releaseExceptionallyAndReset(Throwable e) {
        ArrayList<CompletableFuture<T>> toComplete = null;
        synchronized (lock) {
            if (!waitingFutures.isEmpty()) {
                toComplete = new ArrayList<>(waitingFutures);
                waitingFutures.clear();
            }
            released = false;
            this.e = null;
            result = null;
            runningThreadId = null;
        }
        if (toComplete != null) {
            for (CompletableFuture<T> f : toComplete) {
                f.completeExceptionally(e);
            }
        }
    }
    
    @Override
    public String toString() {
        synchronized (lock) {
            return "Released: " + released + " waiting: " + waitingFutures.size() + " running: "
                    + (runningThreadId != null);
        }
    }
}
