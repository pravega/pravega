/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;

/**
 * This class is similar to {@link ReusableLatch} but that works with {@link CompletableFuture} so
 * that blocking can be async and exceptions and results can be passed.
 * @param <T> The type of the futures that this class works with.
 */
public class ReusableFutureLatch<T> {
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final ArrayList<CompletableFuture<T>> waitingFutures = new ArrayList<>();
    @GuardedBy("lock")
    private T result;
    @GuardedBy("lock")
    private Exception e;
    @GuardedBy("lock")
    private boolean released;

    public ReusableFutureLatch() {
        released = false;
    }

    /**
     * Supply a future to be notified when {@link #release(Object)} is called. If release has already been
     * called, it will be compltedImmediatly.
     * 
     * @param toNotify The future that should be completed.
     */
    public void await(CompletableFuture<T> toNotify) {
        T result;
        Exception e;
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
     * Complete all waiting futures, and all future calls to await to run immediately. If release is
     * called twice consecutively the second value will be the one passed to future callers of
     * {@link #await(CompletableFuture)}
     * 
     * @param result The result to pass to waiting futures.
     */
    public void release(T result) {
        ArrayList<CompletableFuture<T>> toComplete = null;
        synchronized (lock) {
            if (!waitingFutures.isEmpty()) {
                toComplete = new ArrayList<>(toComplete);
                toComplete.clear();
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
     * Complete all waiting futures, and all future calls to await to run immediately. If release is
     * called twice consecutively the second value will be the one passed to future callers of
     * {@link #await(CompletableFuture)}
     * 
     * @param e The exception to pass to waiting futures.
     */
    public void releaseExceptionally(Exception e) {
        ArrayList<CompletableFuture<T>> toComplete = null;
        synchronized (lock) {
            if (!waitingFutures.isEmpty()) {
                toComplete = new ArrayList<>(toComplete);
                toComplete.clear();
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
     * If {@link #release(Object)} or {@link #releaseExceptionally(Exception)} has been called it
     * resets the object into the unreleased state. If release has not been called this will have no effect.
     */
    public void reset() {
        synchronized (lock) {
            released = false;
            e = null;
            result = null;
        }
    }
}
