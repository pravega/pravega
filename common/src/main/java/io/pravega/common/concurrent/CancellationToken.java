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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;

/**
 * Represents a token that can be passed around to various services or components to indicate when a task should be cancelled.
 */
@ThreadSafe
public class CancellationToken {
    /**
     * A CancellationToken that can be used as a placeholder for "no token to pass". This token instance cannot be cancelled.
     */
    public static final CancellationToken NONE = new NonCancellableToken();
    @GuardedBy("futures")
    private final Collection<CompletableFuture<?>> futures;
    @Getter
    @GuardedBy("futures")
    private boolean cancellationRequested;

    /**
     * Creates a new instance of the CancellationToken class.
     */
    public CancellationToken() {
        this.futures = new HashSet<>();
    }

    /**
     * Registers the given Future to the token.
     *
     * @param future The Future to register.
     * @param <T>    Return type of the future.
     */
    public <T> void register(CompletableFuture<T> future) {
        if (future.isDone()) {
            // Nothing to do.
            return;
        }

        synchronized (this.futures) {
            Preconditions.checkNotNull(future, "future");
            Preconditions.checkState(!this.cancellationRequested, "CancellationToken is already cancelled; cannot register new futures.");
            this.futures.add(future);
        }

        // Cleanup once the future is completed.
        future.whenComplete((r, ex) -> {
            synchronized (this.futures) {
                this.futures.remove(future);
            }
        });
    }

    /**
     * Cancels all registered futures.
     */
    public void requestCancellation() {
        Collection<CompletableFuture<?>> toInvoke;
        synchronized (this.futures) {
            this.cancellationRequested = true;
            toInvoke = new ArrayList<>(this.futures);
        }

        toInvoke.forEach(f -> f.cancel(true));
        synchronized (this.futures) {
            this.futures.clear();
        }
    }

    @Override
    public String toString() {
        synchronized (this.futures) {
            return "Cancelled = " + Boolean.toString(this.cancellationRequested);
        }
    }

    private static final class NonCancellableToken extends CancellationToken {
        @Override
        public <T> void register(CompletableFuture<T> future) {
            // This method intentionally left blank. No point in registering anything.
        }

        @Override
        public void requestCancellation() {
            // This method intentionally left blank. No point in requesting any cancellation.
        }
    }
}
