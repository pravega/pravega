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
package io.pravega.client.stream.impl;

import io.pravega.client.control.impl.CancellableRequest;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

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
