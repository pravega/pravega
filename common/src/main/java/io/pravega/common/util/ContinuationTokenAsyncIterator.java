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

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@ThreadSafe
@Slf4j
/**
 * This is a continuation token based async iterator implementation. This class takes a function that when completed will 
 * have next batch of results with continuation token. 
 * This class determines when to call the next iteration of function (if all existing results have been exhausted) and 
 * ensures there is only one outstanding call. 
 */
public class ContinuationTokenAsyncIterator<T, U> implements AsyncIterator<U> {
    private final Object lock = new Object();

    @GuardedBy("lock")
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final Queue<U> queue;
    @GuardedBy("lock")
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final AtomicReference<T> token;
    private final Function<T, CompletableFuture<Map.Entry<T, Collection<U>>>> function;
    @GuardedBy("lock")
    private CompletableFuture<Void> outstanding;
    private final AtomicBoolean canContinue;
    private final AtomicBoolean makeCall;
    
    public ContinuationTokenAsyncIterator(Function<T, CompletableFuture<Map.Entry<T, Collection<U>>>> function, T tokenIdentity) {
        this.function = function;
        this.token = new AtomicReference<>(tokenIdentity);
        this.queue = new LinkedBlockingQueue<>();
        this.outstanding = CompletableFuture.completedFuture(null);
        this.canContinue = new AtomicBoolean(true);
        this.makeCall = new AtomicBoolean(false);
    }

    @Override
    public CompletableFuture<U> getNext() {
        final T continuationToken;
        synchronized (lock) {
            // if the result is available, return it without making function call
            if (!queue.isEmpty()) {
                return CompletableFuture.completedFuture(queue.poll());
            } else {
                continuationToken = token.get();
                // make the function call if previous outstanding call completed.
                makeCall.compareAndSet(false, outstanding.isDone());
                if (outstanding.isDone()) {
                    outstanding = outstanding.thenComposeAsync(v -> function.apply(continuationToken)
                            .thenAccept(resultPair -> {
                                synchronized (lock) {
                                    if (token.get().equals(continuationToken)) {
                                        log.debug("Received the following collection after calling the function: {} with continuation token: {}",
                                                resultPair.getValue(), resultPair.getKey());
                                        canContinue.set(resultPair.getValue() != null && !resultPair.getValue().isEmpty());
                                        queue.addAll(resultPair.getValue());
                                        token.set(resultPair.getKey());
                                    }
                                }
                            }).whenComplete((x, e) -> {
                                              if (e != null) {
                                                  log.warn("Async iteration failed: ", e);
                                              }
                                          }));
                }
            }
        }

        return outstanding.thenCompose(v -> {
            if (canContinue.get()) {
                return getNext();
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }
}
