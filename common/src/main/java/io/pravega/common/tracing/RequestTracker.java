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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is a helper for multiple classes to get the client-generated id for a request (requestId) based on the
 * available information of the request itself (requestDescriptor) in multiple methods. The objective is to cache such
 * [requestDescriptor, requestId] pairs to we allow classes within a component (Controller, Segment Store) to also log
 * the client-generated ids, making it easier to trace the lifecycle of a request across multiple logs.
 */
@Slf4j
public final class RequestTracker {

    private static final String INTER_FIELD_DELIMITER = "-";
    private static final int MAX_CACHE_SIZE = 1000000;
    private static final int EVICTION_PERIOD_MINUTES = 10;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Cache<String, List<Long>> ongoingRequests;

    public RequestTracker() {
        // Clean request tags after a certain amount of time.
        ongoingRequests = CacheBuilder.newBuilder()
                                      .maximumSize(MAX_CACHE_SIZE)
                                      .expireAfterWrite(EVICTION_PERIOD_MINUTES, TimeUnit.MINUTES)
                                      .build();
    }

    /**
     * Creates a request descriptor or key to locate the client request id.
     *
     * @param requestInfo Fields to form the request descriptor.
     * @return Request descriptor.
     */
    public static String buildRequestDescriptor(String...requestInfo) {
        return Stream.of(requestInfo).collect(Collectors.joining(INTER_FIELD_DELIMITER));
    }

    /**
     * Retrieves a RequestTag object formed by a request descriptor and request id pair. In the case of concurrent
     * requests with the same descriptor,
     *
     * @param requestInfo Fields to form the request descriptor.
     * @return Request descriptor and request id pair embedded in a RequestTag object.
     */
    public RequestTag getRequestTagFor(String...requestInfo) {
        return getRequestTagFor(RequestTracker.buildRequestDescriptor(requestInfo));
    }

    public RequestTag getRequestTagFor(String requestDescriptor) {
        Preconditions.checkNotNull(requestDescriptor, "Attempting to get a null request descriptor.");
        long requestId;
        List<Long> descriptorIds;

        synchronized (lock) {
            descriptorIds = ongoingRequests.getIfPresent(requestDescriptor);
            requestId = (descriptorIds == null || descriptorIds.size() == 0) ? RequestTag.NON_EXISTENT_ID : descriptorIds.get(0);
            if (descriptorIds == null) {
                log.debug("Attempting to get a non-existing tag: {}.", requestDescriptor);
            } else if (descriptorIds.size() > 1) {
                log.debug("{} request ids associated with same descriptor: {}. Propagating only first one: {}.",
                        descriptorIds, requestDescriptor, requestId);
            }
        }

        return new RequestTag(requestDescriptor, requestId);
    }

    /**
     * Retrieves a request id associated to a request descriptor.
     *
     * @param requestInfo Fields to form the request descriptor.
     * @return Request descriptor and request id pair embedded in a RequestTag object.
     */
    public long getRequestIdFor(String...requestInfo) {
        return getRequestIdFor(RequestTracker.buildRequestDescriptor(requestInfo));
    }

    public long getRequestIdFor(String requestDescriptor) {
        return getRequestTagFor(requestDescriptor).getRequestId();
    }

    /**
     * Adds a request descriptor and request id pair in the cache. In the case of tracking a request with an existing
     * descriptor, this method adds the request id to the list associated to the descriptor.
     *
     * @param requestTag Request to be cached for further tracing.
     */
    public void trackRequest(RequestTag requestTag) {
        trackRequest(requestTag.getRequestDescriptor(), requestTag.getRequestId());
    }

    public void trackRequest(String requestDescriptor, long requestId) {
        Preconditions.checkNotNull(requestDescriptor, "Attempting to track a null request descriptor.");

        synchronized (lock) {
            List<Long> requestIds = ongoingRequests.getIfPresent(requestDescriptor);
            if (requestIds == null) {
                requestIds = Collections.synchronizedList(new ArrayList<>());
            }

            requestIds.add(requestId);
            ongoingRequests.put(requestDescriptor, requestIds);
        }

        log.debug("Tracking request {} with id {}.", requestDescriptor, requestId);
    }

    /**
     * Remove a request id from an associated request descriptor. In the case that there is only one request id, the
     * whole entry is evicted from cache. If there are multiple request ids for a given descriptor, the last request id
     * in the list is deleted from the cache.
     *
     * @param requestTag Request tag to remove from cache.
     * @return Request id removed from cache.
     */
    public long untrackRequest(RequestTag requestTag) {
        return untrackRequest(requestTag.getRequestDescriptor());
    }

    public long untrackRequest(String requestDescriptor) {
        Preconditions.checkNotNull(requestDescriptor, "Attempting to untrack a null request descriptor.");
        long removedRequestId;
        List<Long> requestIds;

        synchronized (lock) {
            requestIds = ongoingRequests.getIfPresent(requestDescriptor);
            if (requestIds == null) {
                log.debug("Attempting to untrack a non-existing key: {}.", requestDescriptor);
                return RequestTag.NON_EXISTENT_ID;
            }

            if (requestIds.size() > 1) {
                removedRequestId = requestIds.remove(requestIds.size() - 1);
                log.debug("{} concurrent requests with same descriptor: {}. Untracking the last of them {}.", requestIds,
                        requestDescriptor, removedRequestId);
                ongoingRequests.put(requestDescriptor, requestIds);
            } else {
                ongoingRequests.invalidate(requestDescriptor);
                removedRequestId = requestIds.get(0);
            }
        }

        log.debug("Untracking request {} with id {}.", requestDescriptor, requestIds);
        return removedRequestId;
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
    public RequestTag initializeAndTrackRequestTag(long requestId, String...requestInfo) {
        RequestTag requestTag = getRequestTagFor(requestInfo);
        if (!requestTag.isTracked()) {
            log.debug("Tags not found for this request: requestId={}, descriptor={}. Create request tag at this point.",
                    requestId, RequestTracker.buildRequestDescriptor(requestInfo));
            requestTag = new RequestTag(RequestTracker.buildRequestDescriptor(requestInfo), requestId);
            trackRequest(requestTag);
        }

        return requestTag;
    }
}
