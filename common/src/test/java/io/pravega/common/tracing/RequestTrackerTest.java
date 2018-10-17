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
        final String requestDescriptor = RequestTracker.buildRequestDescriptor("createStream", "scope", "stream");
        final long requestId = 123;

        // Track the request and assert that it has been correctly cached.
        RequestTracker.getInstance().trackRequest(requestDescriptor, requestId);
        Assert.assertEquals(new RequestTag(requestDescriptor, requestId), RequestTracker.getInstance().getRequestTagFor(requestDescriptor));
        Assert.assertEquals(requestId, RequestTracker.getInstance().getRequestIdFor(requestDescriptor));

        // Delete the request tag, the associated request id should be retrieved.
        Assert.assertEquals(requestId, RequestTracker.getInstance().untrackRequest(requestDescriptor));

        // Delete a non-existing key, so a default value is retrieved.
        Assert.assertEquals(Long.MIN_VALUE, RequestTracker.getInstance().untrackRequest(requestDescriptor));

        // Check that null arguments are not accepted and appropriate exceptions are thrown.
        AssertExtensions.assertThrows(NullPointerException.class,
                () -> RequestTracker.getInstance().trackRequest(new RequestTag(null, requestId)));
        AssertExtensions.assertThrows(NullPointerException.class,
                () -> RequestTracker.getInstance().untrackRequest(new RequestTag(null, requestId)));
        String nullDescriptor = null;
        AssertExtensions.assertThrows(NullPointerException.class, () -> RequestTracker.getInstance().getRequestIdFor(nullDescriptor));
    }

    /**
     * This tests aims at asserting the behavior of RequestTracker in the presence of multiple requests being traced
     * with the same request descriptor. That is, due to the idempotence of traced operations, there cannot be multiple
     * concurrent requests performing actual changes on the system's state (e.g., createStream, deleteStream). Therefore,
     * the policy adopted is that in the case of concurrent operations with the same request descriptor, the first id
     * in the descriptor's associated list will represent all the side-effects of that operation; this includes the
     * activity of the requests that does perform changes in the system, as well as the other requests that are rejected.
     * Note that from a debugging perspective, we log the requests ids that are represented by the first one on the list.
     */
    @Test
    public void testPolicyForRequestsWithSameDescriptor() {
        final String requestDescriptor = RequestTracker.buildRequestDescriptor("createStream", "scope", "stream");

        // Log multiple request ids associated with the same request descriptor and assert that the first one is retrieved.
        for (int i = 0; i < 10; i++) {
            RequestTracker.getInstance().trackRequest(requestDescriptor, i);
            Assert.assertEquals(0, RequestTracker.getInstance().getRequestIdFor(requestDescriptor));
        }

        // When untracking requests ids from the descriptor, the most recent ones should be removed first.
        for (int i = 9; i >= 0; i--) {
            Assert.assertEquals(i, RequestTracker.getInstance().untrackRequest(requestDescriptor));
        }

        // Getting a non-existing key, so a default value is retrieved.
        Assert.assertEquals(Long.MIN_VALUE, RequestTracker.getInstance().getRequestIdFor(requestDescriptor));
    }
}
