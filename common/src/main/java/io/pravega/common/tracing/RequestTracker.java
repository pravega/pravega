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

    public synchronized boolean existsRequest(String requestDescriptor) {
        return ongoingRequests.containsKey(requestDescriptor);
    }

    public RequestTag getRequestTagFor(String...requestInfo) {
        return getRequestTagFor(RequestTracker.createRPCRequestDescriptor(requestInfo));
    }

    public synchronized RequestTag getRequestTagFor(String requestDescriptor) {
        Preconditions.checkArgument(requestDescriptor != null, "Attempting to untrack a null RPC request descriptor.");
        RequestTag deletedValue = null;
        if (!ongoingRequests.containsKey(requestDescriptor)) {
            log.warn("Attempting to untrack a non-existing key: {}.", requestDescriptor);
            return deletedValue;
        }

        if (ongoingRequests.get(requestDescriptor).size() > 1) {
            log.warn("{} concurrent requests with same descriptor: {}. Choosing first one.", ongoingRequests.get(requestDescriptor).size(), requestDescriptor);
        }

        return new RequestTag(requestDescriptor, ongoingRequests.get(requestDescriptor).get(0));
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
        ongoingRequests.putIfAbsent(key, new ArrayList<>());
        ongoingRequests.computeIfPresent(key, (k, v) -> {
            v.add(requestId);
            return v;
        });
        log.info("Tracking request {} with id {}. Current ongoing requests: {}.", key, requestId,
                ongoingRequests.values().stream().mapToInt(List::size).sum());
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

        log.info("Untracking request {} with id {}. Current ongoing requests: {}.", key, deletedValue,
                ongoingRequests.values().stream().mapToInt(List::size).sum());
        return deletedValue;
    }

    /**
     * This method first attempts to load a tag from a request that is assumed to exist. However, if we work with
     * clients or channels that do not attach tags to requests, then we initialize and track the request at the server
     * side. In the worst case, we will have the ability of tracking a request from the RPC server onwards.
     *
     * @param requestId Alternative request id in the case there is no request id in headers.
     * @param requestInfo Alternative descriptor to identify the call in the case there is no descriptor in headers.
     * @return Request tag formed either from request headers or from arguments given.
     */
    public static RequestTag initializeAndTrackRequestTag(long requestId, String...requestInfo) {
        RequestTag requestTag = RequestTracker.getInstance().getRequestTagFor(requestInfo);
        if (requestTag == null) {
            log.warn("Request tags not found for this request: requestId={}, descriptor={}. Create request tag at this point.", requestId,
                    RequestTracker.createRPCRequestDescriptor(requestInfo));
            requestTag = new RequestTag(RequestTracker.createRPCRequestDescriptor(requestInfo), requestId);
            RequestTracker.getInstance().trackRequest(requestTag);
        }

        log.info("[requestId={}] Getting tags from request {}.", requestTag.getRequestId(), requestTag.getRequestDescriptor());
        return requestTag;
    }
}
