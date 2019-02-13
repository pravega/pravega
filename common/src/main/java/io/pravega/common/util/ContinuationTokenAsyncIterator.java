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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@ThreadSafe
@Slf4j
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

    public ContinuationTokenAsyncIterator(Function<T, CompletableFuture<Map.Entry<T, Collection<U>>>> function, T tokenIdentity) {
        this.function = function;
        this.token = new AtomicReference<>(tokenIdentity);
        this.queue = new LinkedBlockingQueue<>();
    }

    /**
     * If multiple getNext are called concurrently it will result in multiple remote calls with
     * same continuation token. This can mean same set of result can get added to the queue multiple times.
     * To mitigate this, we add result to queue only if the token used in the request and existing value of token are same. 
     * So if multiple getNext are concurrently called for same continuation token, the result from exactly one of them
     * is processed and included while others are ignored because first one will update the token as well. 
     */
    @Override
    public CompletableFuture<U> getNext() {
        final T continuationToken;
        // if the result is available, return it without making function call
        synchronized (lock) {
            if (!queue.isEmpty()) {
                return CompletableFuture.completedFuture(queue.poll());
            } else {
                continuationToken = token.get();
            }
        }

        return function.apply(continuationToken).thenCompose(resultPair -> {
            U polled;
            synchronized (lock) {
                if (token.get().equals(continuationToken)) {
                    log.debug("Received the following data after calling the function {}", resultPair);
                    queue.addAll(resultPair.getValue());
                    token.set(resultPair.getKey());
                }
                polled = queue.poll();
            }
            
            if (resultPair.getValue() != null && !resultPair.getValue().isEmpty() && polled == null) {
                // If concurrent getNext calls were received, and we only add results once, it could so happen that 
                // number of elements returned was less than number of outstanding calls. This could mean we return 
                // "null" for some of the calls even though the "function" may have returned a continuation token and 
                // non empty collection. So in this case, instead of returning null, we will call getNext recursively.
                return getNext();
            } else {
                return CompletableFuture.completedFuture(polled);
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("Async iteration failed: ", e);
            }
        });
    }
}
