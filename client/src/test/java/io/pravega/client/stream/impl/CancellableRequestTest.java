/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.common.concurrent.Futures;
import io.pravega.test.common.ThreadPooledTestSuite;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CancellableRequestTest extends ThreadPooledTestSuite {

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Test
    public void testTermination() {
        CancellableRequest<Boolean> request = new CancellableRequest<>();
        request.start(() -> CompletableFuture.completedFuture(null), any -> true, executorService());
        assertTrue(Futures.await(request.getFuture()));
    }

    @Test
    public void testCancellation() {
        CancellableRequest<Boolean> request = new CancellableRequest<>();
        request.start(() -> CompletableFuture.completedFuture(null), any -> false, executorService());
        assertFalse(request.getFuture().isDone());
        request.cancel();
        assertTrue(request.getFuture().isDone());
    }
}
