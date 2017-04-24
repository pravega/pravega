/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.common.concurrent;

import io.pravega.testcommon.AssertExtensions;
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
                Assert.assertTrue("Already completed future was cancelled.", FutureHelpers.isSuccessful(futures.get(i)));
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
                Assert.assertTrue("Already completed future was cancelled.", FutureHelpers.isSuccessful(futures.get(i)));
            } else {
                Assert.assertFalse("Non-completed future was completed.", futures.get(i).isDone());
            }
        }
    }
}