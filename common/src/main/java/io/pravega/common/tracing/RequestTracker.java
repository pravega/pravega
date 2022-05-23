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
package io.pravega.common.tracing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is a helper for multiple classes to get the client-generated id for a request (requestId) based on the
 * available information of the request itself (requestDescriptor) in multiple methods. The objective is to cache such
 * requestDescriptor and requestId pairs to allow classes within a component (Controller, Segment Store) to also log the
 * client-generated ids, making it easier to trace the lifecycle of a request across multiple logs.
 */
@Slf4j
public final class RequestTracker {

    @VisibleForTesting
    static final int MAX_PARALLEL_REQUESTS = 10;
    private static final String INTER_FIELD_DELIMITER = "-";
    private static final int MAX_CACHE_SIZE = 100000;
    private static final int EVICTION_PERIOD_MINUTES = 10;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Cache<String, List<Long>> ongoingRequests;
    @Getter
    private final boolean tracingEnabled;

    public RequestTracker(boolean tracingEnabled) {
        this.tracingEnabled = tracingEnabled;

        // Clean request tags after a certain amount of time.
        if (tracingEnabled) {
            ongoingRequests = CacheBuilder.newBuilder()
                                          .maximumSize(MAX_CACHE_SIZE)
                                          .expireAfterWrite(EVICTION_PERIOD_MINUTES, TimeUnit.MINUTES)
                                          .build();
        } else {
            ongoingRequests = null;
        }
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
     * Retrieves a {@link RequestTag} object formed by a request descriptor and request id by means of using method
     * {@link #getRequestTagFor(String...)}. In addition, this method transforms a set of input string parameters into
     * a single request descriptor.
     *
     * @param requestInfo Fields to form the request descriptor.
     * @return Request descriptor and request id pair embedded in a {@link RequestTag} object.
     */
    public RequestTag getRequestTagFor(String...requestInfo) {
        return getRequestTagFor(RequestTracker.buildRequestDescriptor(requestInfo));
    }

    /**
     * Retrieves a {@link RequestTag} object formed by a request descriptor and request id. If the request descriptor
     * does not exist or tracing is disabled, a new {@link RequestTag} object with a default request id is returned. In
     * the case of concurrent requests with the same descriptor, multiple request ids will be associated to that request
     * descriptor. The policy adopted is to retrieve the first one that was stored in the cache. Given that tracing is
     * applied to idempotent operations, this allows us to consistently trace the operation that actually changes the
     * state of the system. The rest of concurrent operations will be rejected and their response will be logged with a
     * different requestId, as an indicator that another client request was ongoing. For more information, we refer to
     * this PDP: https://github.com/pravega/pravega/wiki/PDP-31:-End-to-end-Request-Tags
     *
     * @param requestDescriptor Request descriptor as a single string.
     * @return Request descriptor and request id pair embedded in a {@link RequestTag} object.
     */
    public RequestTag getRequestTagFor(String requestDescriptor) {
        Preconditions.checkNotNull(requestDescriptor, "Attempting to get a null request descriptor.");
        if (!tracingEnabled) {
            return new RequestTag(requestDescriptor, RequestTag.NON_EXISTENT_ID);
        }

        long requestId;
        List<Long> descriptorIds;
        synchronized (lock) {
            descriptorIds = ongoingRequests.getIfPresent(requestDescriptor);
            // If there are multiple parallel requests for the same descriptor, the first one will be the primary (i.e., the one
            // used to log the lifecycle of the request).
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
     * Retrieves a request id associated to a request descriptor. This method overloads {@link #getRequestTagFor(String)}
     * by transforming a set of input string parameters into a single request descriptor.
     *
     * @param requestInfo Fields to form the request descriptor.
     * @return Request descriptor and request id pair embedded in a RequestTag object.
     */
    public long getRequestIdFor(String...requestInfo) {
        return getRequestIdFor(RequestTracker.buildRequestDescriptor(requestInfo));
    }

    /**
     * Retrieves the requestId associated to a request descriptor by means of using {@link #getRequestTagFor(String...)}.
     *
     * @param requestDescriptor Request descriptor as a single string.
     * @return Request id associated to the given descriptor.
     */
    public long getRequestIdFor(String requestDescriptor) {
        return getRequestTagFor(requestDescriptor).getRequestId();
    }

    /**
     * Adds a request descriptor and request id pair in the cache based on an input {@link RequestTag} object by
     * overloading {{@link #trackRequest(String, long)}}.
     *
     * @param requestTag Request tag to be cached for further tracing.
     */
    public void trackRequest(RequestTag requestTag) {
        trackRequest(requestTag.getRequestDescriptor(), requestTag.getRequestId());
    }

    /**
     * Adds a request descriptor and request id pair in the cache if tracing is enabled. In the case of tracking a
     * request with an existing descriptor, this method adds the request id to the list associated to the descriptor.
     *
     * @param requestDescriptor Request descriptor as a single string.
     * @param requestId Request id associated to the given descriptor.
     */
    public void trackRequest(String requestDescriptor, long requestId) {
        Preconditions.checkNotNull(requestDescriptor, "Attempting to track a null request descriptor.");
        if (!tracingEnabled) {
            return;
        }

        synchronized (lock) {
            List<Long> requestIds = ongoingRequests.getIfPresent(requestDescriptor);
            if (requestIds == null) {
                requestIds = new ArrayList<>();
                ongoingRequests.put(requestDescriptor, requestIds);
            }

            requestIds.add(requestId);
            if (requestIds.size() > MAX_PARALLEL_REQUESTS) {
                // Delete the oldest parallel that is not the primary (first) one request id to bound the size of this list.
                requestIds.remove(1);
            }
        }

        log.debug("Tracking request {} with id {}.", requestDescriptor, requestId);
    }

    /**
     * Remove a request id from an associated request descriptor within a {@link RequestTag} object by overloading
     * {{@link #untrackRequest(String)}}.
     *
     * @param requestTag Request tag to remove from cache.
     * @return Request id removed from cache.
     */
    public long untrackRequest(RequestTag requestTag) {
        return untrackRequest(requestTag.getRequestDescriptor());
    }

    /**
     * Removes and returns a request id from an associated request descriptor. If the request descriptor does not exist
     * or tracing is disabled, a default request id is returned. In the case that there is only one request id, the
     * whole entry is evicted from cache. If there are multiple request ids for a given descriptor, the last request id
     * in the list is deleted from the cache.
     *
     * @param requestDescriptor Request tag to remove from cache.
     * @return Request id removed from cache.
     */
    public long untrackRequest(String requestDescriptor) {
        Preconditions.checkNotNull(requestDescriptor, "Attempting to untrack a null request descriptor.");
        if (!tracingEnabled) {
            return RequestTag.NON_EXISTENT_ID;
        }

        long removedRequestId;
        List<Long> requestIds;
        synchronized (lock) {
            requestIds = ongoingRequests.getIfPresent(requestDescriptor);
            if (requestIds == null || requestIds.size() == 0) {
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
     * This method first attempts to load a {@link RequestTag} from a request that is assumed to exist. However, if we
     * work with clients or channels that do not attach tags to requests, then we initialize and track the request at
     * the server side. In the worst case, we will have the ability of tracking a request from the server entry point
     * onwards.
     *
     * @param requestId Alternative request id in the case there is no request id in headers.
     * @param requestInfo Alternative descriptor to identify the call in the case there is no descriptor in headers.
     * @return Request tag formed either from request headers or from arguments given.
     */
    public RequestTag initializeAndTrackRequestTag(long requestId, String...requestInfo) {
        RequestTag requestTag = getRequestTagFor(requestInfo);
        if (tracingEnabled && !requestTag.isTracked()) {
            log.debug("Tags not found for this request: requestId={}, descriptor={}. Create request tag at this point.",
                    requestId, RequestTracker.buildRequestDescriptor(requestInfo));
            requestTag = new RequestTag(RequestTracker.buildRequestDescriptor(requestInfo), requestId);
            trackRequest(requestTag);
        }

        return requestTag;
    }

    /**
     * Returns the number of descriptors in the cache.
     *
     * @return Number of request descriptors in the cache.
     */
    @VisibleForTesting
    public long getNumDescriptors() {
        synchronized (lock) {
            return ongoingRequests.size();
        }
    }
}
