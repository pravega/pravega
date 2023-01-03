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

import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for RequestTracker class.
 */
public class RequestTrackerTest {

    @Test
    public void requestDescriptorBuildingTest() {
        final String operation = "createStream";
        final String scope = "scope";
        final String stream = "stream";
        final String expectedRequestDescriptor = "createStream-scope-stream";
        Assert.assertEquals(expectedRequestDescriptor, RequestTracker.buildRequestDescriptor(operation, scope, stream));
    }

    /**
     * Test basic operations for tracing requests in the RequestTracker cache.
     */
    @Test
    public void testRequestTagLifecycle() {
        RequestTracker requestTracker = new RequestTracker(true);
        final String requestDescriptor = RequestTracker.buildRequestDescriptor("createStream", "scope", "stream");
        final long requestId = 123;

        // Track the request and assert that it has been correctly cached.
        requestTracker.trackRequest(requestDescriptor, requestId);
        Assert.assertEquals(new RequestTag(requestDescriptor, requestId), requestTracker.getRequestTagFor(requestDescriptor));
        Assert.assertEquals(requestId, requestTracker.getRequestIdFor(requestDescriptor));

        // Delete the request tag, the associated request id should be retrieved.
        Assert.assertEquals(requestId, requestTracker.untrackRequest(requestDescriptor));

        // Delete a non-existing key, so a default value is retrieved.
        Assert.assertEquals(RequestTag.NON_EXISTENT_ID, requestTracker.untrackRequest(requestDescriptor));

        // Check that null arguments are not accepted and appropriate exceptions are thrown.
        AssertExtensions.assertThrows(NullPointerException.class,
                () -> requestTracker.trackRequest(new RequestTag(null, requestId)));
        AssertExtensions.assertThrows(NullPointerException.class,
                () -> requestTracker.untrackRequest(new RequestTag(null, requestId)));
        String nullDescriptor = null;
        AssertExtensions.assertThrows(NullPointerException.class, () -> requestTracker.getRequestIdFor(nullDescriptor));
    }

    /**
     * This tests aims at asserting the behavior of RequestTracker in the presence of multiple requests being traced
     * with the same request descriptor. That is, due to the idempotence of traced operations, there cannot be multiple
     * concurrent requests performing actual changes on the system's state (e.g., createStream, deleteStream). Therefore,
     * the policy adopted is that in the case of concurrent operations with the same request descriptor, the first id
     * in the descriptor's associated list will represent all the side effects of that operation; this includes the
     * activity of the requests that perform changes in the system, as well as the other requests that are rejected.
     * Note that from a debugging perspective, we log the requests ids that are represented by the first one on the list.
     */
    @Test
    public void testPolicyForRequestsWithSameDescriptor() {
        RequestTracker requestTracker = new RequestTracker(true);
        final String requestDescriptor = RequestTracker.buildRequestDescriptor("createStream", "scope", "stream");

        // Log multiple request ids associated with the same request descriptor and assert that the first one is retrieved.
        for (int i = 0; i < RequestTracker.MAX_PARALLEL_REQUESTS; i++) {
            requestTracker.trackRequest(requestDescriptor, i);
            Assert.assertEquals(0, requestTracker.getRequestIdFor(requestDescriptor));
        }
        Assert.assertEquals(1, requestTracker.getNumDescriptors());

        // When untracking requests ids from the descriptor, the most recent ones should be removed first.
        for (int i = RequestTracker.MAX_PARALLEL_REQUESTS - 1; i >= 0; i--) {
            Assert.assertEquals(i, requestTracker.untrackRequest(requestDescriptor));
        }

        // Getting a non-existing key, so a default value is retrieved.
        Assert.assertEquals(RequestTag.NON_EXISTENT_ID, requestTracker.getRequestIdFor(requestDescriptor));
        Assert.assertEquals(0, requestTracker.getNumDescriptors());
        Assert.assertNotNull(requestTracker.initializeAndTrackRequestTag(123L, "createStream", "scope", "stream"));
    }

    /**
     * Tests the behavior of parallel requests when they outnumber the max size for parallel requests defined in
     * {@link RequestTracker}.
     */
    @Test
    public void testMoreThanMaxParallelRequestsWithSameDescriptor() {
        RequestTracker requestTracker = new RequestTracker(true);
        final String requestDescriptor = RequestTracker.buildRequestDescriptor("createStream", "scope", "stream");

        // Log multiple request ids associated with the same request descriptor and assert that the first one is retrieved.
        // Note that we should always keep
        int totalParallelRequests = RequestTracker.MAX_PARALLEL_REQUESTS * 10;
        for (int i = 0; i < totalParallelRequests; i++) {
            requestTracker.trackRequest(requestDescriptor, i);
            Assert.assertEquals(0, requestTracker.getRequestIdFor(requestDescriptor));
        }

        // When untracking requests ids from the descriptor, the most recent ones should be removed first.
        for (int i = totalParallelRequests - 1; i > totalParallelRequests - RequestTracker.MAX_PARALLEL_REQUESTS; i--) {
            Assert.assertEquals(i, requestTracker.untrackRequest(requestDescriptor));
        }

        // Getting a non-existing key, so a default value is retrieved.
        Assert.assertEquals(RequestTag.NON_EXISTENT_ID, requestTracker.getRequestIdFor(requestDescriptor));
    }

    @Test
    public void testDisabledRequestTracking() {
        RequestTracker requestTracker = new RequestTracker(false);
        final String requestDescriptor = RequestTracker.buildRequestDescriptor("createStream", "scope", "stream");
        requestTracker.trackRequest(requestDescriptor, 123L);
        Assert.assertFalse(requestTracker.isTracingEnabled());
        Assert.assertEquals(requestTracker.getRequestIdFor(requestDescriptor), RequestTag.NON_EXISTENT_ID);
        Assert.assertEquals(requestTracker.getRequestIdFor("createStream", "scope", "stream"), RequestTag.NON_EXISTENT_ID);
        Assert.assertEquals(requestTracker.untrackRequest(requestTracker.getRequestTagFor("createStream", "scope", "stream")), RequestTag.NON_EXISTENT_ID);
        Assert.assertNotNull(requestTracker.initializeAndTrackRequestTag(123L, "createStream", "scope", "stream"));
    }
}
