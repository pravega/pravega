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
package io.pravega.common.concurrent;

import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the CancellationToken class.
 */
public class CancellationTokenTests {
    /**
     * Tests that RequestCancellation cancels futures.
     */
    @Test
    public void testRequestCancellation() {
        final int futureCount = 10;
        CancellationToken token = new CancellationToken();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        Predicate<Integer> isAlreadyCompleted = i -> i % 2 == 0; // Every other future is already completed.
        for (int i = 0; i < futureCount; i++) {
            val f = new CompletableFuture<Void>();
            if (isAlreadyCompleted.test(i)) {
                f.complete(null);
            }

            futures.add(f);
            token.register(f);
        }

        token.requestCancellation();
        for (int i = 0; i < futures.size(); i++) {
            if (isAlreadyCompleted.test(i)) {
                Assert.assertTrue("Already completed future was cancelled.", Futures.isSuccessful(futures.get(i)));
            } else {
                AssertExtensions.assertThrows(
                        "Future was not cancelled.",
                        futures.get(i)::join,
                        ex -> ex instanceof CancellationException);
            }
        }
    }

    /**
     * Verifies that CancellationToken.NONE has no effect.
     */
    @Test
    public void testNonCancellableToken() {
        final int futureCount = 10;
        CancellationToken token = CancellationToken.NONE;
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        Predicate<Integer> isAlreadyCompleted = i -> i % 2 == 0; // Every other future is already completed.
        for (int i = 0; i < futureCount; i++) {
            val f = new CompletableFuture<Void>();
            if (isAlreadyCompleted.test(i)) {
                f.complete(null);
            }

            futures.add(f);
            token.register(f);
        }

        token.requestCancellation();
        for (int i = 0; i < futures.size(); i++) {
            if (isAlreadyCompleted.test(i)) {
                Assert.assertTrue("Already completed future was cancelled.", Futures.isSuccessful(futures.get(i)));
            } else {
                Assert.assertFalse("Non-completed future was completed.", futures.get(i).isDone());
            }
        }
    }
}