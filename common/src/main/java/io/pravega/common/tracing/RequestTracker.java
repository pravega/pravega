/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.tracing;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RequestTracker {

    private static final RequestTracker INSTANCE = new RequestTracker();

    private final Map<String, List<Long>> ongoingRequests;

    private RequestTracker() {
        ongoingRequests = new HashMap<>();
    }

    public static RequestTracker getInstance() {
        return INSTANCE;
    }

    public static String createRPCRequestDescriptor(String...requestInfo) {
        return Stream.of(requestInfo).collect(Collectors.joining("-"));
    }

    public long getRequestIdFor(String...requestInfo) {
        return getRequestIdFor(RequestTracker.createRPCRequestDescriptor(requestInfo));
    }

    public synchronized long getRequestIdFor(String requestDescriptor) {
        Preconditions.checkArgument(requestDescriptor != null, "Attempting to untrack a null RPC request descriptor.");
        long deletedValue = Long.MIN_VALUE;
        if (!ongoingRequests.containsKey(requestDescriptor)) {
            log.warn("Attempting to untrack a non-existing key: {}.", requestDescriptor);
            return deletedValue;
        }

        if (ongoingRequests.get(requestDescriptor).size() > 1) {
            log.warn("{} concurrent requests with same descriptor: {}. Choosing first one.", ongoingRequests.get(requestDescriptor).size(), requestDescriptor);
        }

        return ongoingRequests.get(requestDescriptor).get(0);
    }

    public void trackRequest(RequestTag requestTag) {
        trackRequest(requestTag.getRequestDescriptor(), requestTag.getRequestId());
    }

    public synchronized void trackRequest(String key, Long requestId) {
        Preconditions.checkArgument(key != null, "Attempting to track a null RPC request descriptor.");
        log.info("Tracking request {} with id {}", key, requestId);
        ongoingRequests.putIfAbsent(key, new ArrayList<>());
        ongoingRequests.computeIfPresent(key, (k, v) -> {
            v.add(requestId);
            return v;
        });
    }

    public synchronized long untrackRequest(String key) {
        Preconditions.checkArgument(key != null, "Attempting to untrack a null RPC request descriptor.");
        long deletedValue = Long.MIN_VALUE;
        if (!ongoingRequests.containsKey(key)) {
            log.warn("Attempting to untrack a non-existing key: {}.", key);
            return deletedValue;
        }

        deletedValue = ongoingRequests.get(key).get(0);
        if (ongoingRequests.get(key).size() < 2) {
            ongoingRequests.remove(key);
        } else {
            // In the case of having parallel requests with the same key, we cannot distinguish among them (delete first).
            ongoingRequests.compute(key, (k, v) -> {
                v.remove(0);
                return v;
            });
        }

        log.info("Untracking request {} with id {}", key, deletedValue);
        return deletedValue;
    }
}
