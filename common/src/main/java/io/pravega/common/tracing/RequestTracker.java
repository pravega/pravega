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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is a helper for multiple classes to get the client-generated id for a request (requestId) based on the
 * available information of the request itself (requestDescriptor) in multiple methods. The objective is to cache such
 * [requestDescriptor, requestId] pairs to we allow classes within a component (Controller, Segment Store) to also log
 * the client-generated ids, making it easier to trace the lifecycle of a request across multiple logs.
 */
@Slf4j
public class RequestTracker {

    private static final RequestTracker INSTANCE = new RequestTracker();
    private static final String INTER_FIELD_DELIMITER = "-";

    private final Cache<String, List<Long>> ongoingRequests;

    private RequestTracker() {
        // Clean request tags after a certain amount of time.
        ongoingRequests = CacheBuilder.newBuilder()
                                      .maximumSize(10000)
                                      .expireAfterWrite(10, TimeUnit.MINUTES)
                                      .build();
    }

    public static RequestTracker getInstance() {
        return INSTANCE;
    }

    public static String createRequestDescriptor(String...requestInfo) {
        return Stream.of(requestInfo).collect(Collectors.joining(INTER_FIELD_DELIMITER));
    }

    public RequestTag getRequestTagFor(String...requestInfo) {
        return getRequestTagFor(RequestTracker.createRequestDescriptor(requestInfo));
    }

    public RequestTag getRequestTagFor(String requestDescriptor) {
        Preconditions.checkArgument(requestDescriptor != null, "Attempting to get a null request descriptor.");
        List<Long> requestIds = ongoingRequests.getIfPresent(requestDescriptor);
        if (requestIds == null) {
            log.warn("Attempting to get a non-existing tag: {}.", requestDescriptor);
            return new RequestTag(requestDescriptor, RequestTag.NON_EXISTENT_ID);
        } else if (requestIds.size() > 1) {
            log.warn("{} concurrent requests with same descriptor: {}. Retrieving a default requestId instead.", requestIds, requestDescriptor);
            return new RequestTag(requestDescriptor, RequestTag.NON_EXISTENT_ID);
        }

        return new RequestTag(requestDescriptor, requestIds.get(0));
    }

    public long getRequestIdFor(String...requestInfo) {
        return getRequestIdFor(RequestTracker.createRequestDescriptor(requestInfo));
    }

    public long getRequestIdFor(String requestDescriptor) {
        return getRequestTagFor(requestDescriptor).getRequestId();
    }

    public void trackRequest(RequestTag requestTag) {
        trackRequest(requestTag.getRequestDescriptor(), requestTag.getRequestId());
    }

    public synchronized void trackRequest(String requestDescriptor, Long requestId) {
        Preconditions.checkArgument(requestDescriptor != null, "Attempting to track a null request descriptor.");
        List<Long> requestIds = ongoingRequests.getIfPresent(requestDescriptor);
        if (requestIds == null) {
            requestIds = new ArrayList<>();
        }

        requestIds.add(requestId);
        ongoingRequests.put(requestDescriptor, requestIds);
        log.info("Tracking request {} with id {}. Current ongoing requests: {}.", requestDescriptor, requestId,
                ongoingRequests.asMap().values().stream().mapToInt(List::size).sum());
    }

    public synchronized long untrackRequest(String requestDescriptor) {
        Preconditions.checkArgument(requestDescriptor != null, "Attempting to untrack a null request descriptor.");
        List<Long> requestIds = ongoingRequests.getIfPresent(requestDescriptor);
        if (requestIds == null) {
            log.warn("Attempting to untrack a non-existing key: {}.", requestDescriptor);
            return RequestTag.NON_EXISTENT_ID;
        }

        if (requestIds.size() > 1) {
            log.warn("{} concurrent requests with same descriptor: {}. Untracking all of them.", requestIds, requestDescriptor);
            return RequestTag.NON_EXISTENT_ID;
        }

        ongoingRequests.invalidate(requestDescriptor);
        log.info("Untracking request {} with id {}. Current ongoing requests: {}.", requestDescriptor, requestIds,
                ongoingRequests.asMap().values().stream().mapToInt(List::size).sum());
        return requestIds.get(0);
    }

    /**
     * This method first attempts to load a tag from a request that is assumed to exist. However, if we work with
     * clients or channels that do not attach tags to requests, then we initialize and track the request at the server
     * side. In the worst case, we will have the ability of tracking a request from the server entry point onwards.
     *
     * @param requestId Alternative request id in the case there is no request id in headers.
     * @param requestInfo Alternative descriptor to identify the call in the case there is no descriptor in headers.
     * @return Request tag formed either from request headers or from arguments given.
     */
    public static RequestTag initializeAndTrackRequestTag(long requestId, String...requestInfo) {
        RequestTag requestTag = RequestTracker.getInstance().getRequestTagFor(requestInfo);
        if (!requestTag.isTracked()) {
            log.info("Request tags not found for this request: requestId={}, descriptor={}. Create request tag at this point.", requestId,
                    RequestTracker.createRequestDescriptor(requestInfo));
            requestTag = new RequestTag(RequestTracker.createRequestDescriptor(requestInfo), requestId);
            RequestTracker.getInstance().trackRequest(requestTag);
        }

        log.info("[requestId={}] Getting tags from request {}.", requestTag.getRequestId(), requestTag.getRequestDescriptor());
        return requestTag;
    }
}
