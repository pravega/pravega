/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.stream.Stream;
import io.pravega.common.util.AsyncIterator;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@ThreadSafe
@Slf4j
class StreamsInScopeIterator implements AsyncIterator<Stream> {
    private final LinkedBlockingQueue<Stream> streams = new LinkedBlockingQueue<>();
    private final AtomicReference<Controller.ContinuationToken> token = new AtomicReference<>(Controller.ContinuationToken.newBuilder().build());
    private final Object lock = new Object();
    private final Function<Controller.ContinuationToken, CompletableFuture<Controller.StreamsInScopeResponse>> function;

    StreamsInScopeIterator(Function<Controller.ContinuationToken, CompletableFuture<Controller.StreamsInScopeResponse>> function) {
        this.function = function;
    }

    /**
     * If multiple getNext are called concurrently it will result in multiple rpc calls to the server with
     * same continuation token. This can mean same set of streams can get added to the queue multiple times.
     * To mitigate this, we add streams to queue only if the token used in the request and existing value of token are same. 
     * So if multiple getNext are concurrently called for same continuation token, the result from exactly one of them
     * is processed and included while others are ignored because first one will update the token as well. 
     */
    @Override
    public CompletableFuture<Stream> getNext() {
        final Controller.ContinuationToken continuationToken = token.get();
        return function.apply(continuationToken).thenApply(streamsInScope -> {
            synchronized (lock) {
                token.getAndUpdate(existing -> {
                    if (existing.equals(continuationToken)) {
                        log.debug("Received the following data after calling the function {}", streamsInScope.getStreamsList());
                        streams.addAll(streamsInScope.getStreamsList().stream()
                                                     .map(x -> (Stream) new StreamImpl(x.getScope(), x.getStream()))
                                                     .collect(Collectors.toList()));
                        return streamsInScope.getContinuationToken();
                    } else {
                        return existing;
                    }
                });
            }
        
            return streams.poll();
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("StreamsInScope async iteration failed: ", e);
            }
        });
    }
}
