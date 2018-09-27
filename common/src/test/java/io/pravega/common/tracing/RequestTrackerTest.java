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

    @Test
    public void requestTagLifecycleTest() {
        final String requestDescriptor = RequestTracker.buildRequestDescriptor("createStream", "scope", "stream");
        final long requestId = 123;

        // Track the request and assert that it has been correctly cached.
        RequestTracker.getInstance().trackRequest(requestDescriptor, requestId);
        Assert.assertEquals(new RequestTag(requestDescriptor, requestId), RequestTracker.getInstance().getRequestTagFor(requestDescriptor));
        Assert.assertEquals(requestId, RequestTracker.getInstance().getRequestIdFor(requestDescriptor));

        // Delete the request tag

    }

}
