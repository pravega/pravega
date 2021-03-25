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

import com.google.common.util.concurrent.Service;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the AbstractThreadPoolService class.
 */
public class AbstractThreadPoolServiceTests extends ThreadPooledTestSuite {
    private static final long SHORT_TIMEOUT_MILLIS = 10;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    @Rule
    public final Timeout globalTimeout = new Timeout(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

    /**
     * Tests the behavior of AbstractThreadPoolService when a normal shutdown (no errors) happens.
     */
    @Test
    public void testShutdownNoFailure() {
        @Cleanup
        val s = newService();

        // Stop it and verify it hasn't shut down - it should still be waiting on the runFuture.
        s.stopAsync();
        AssertExtensions.assertThrows(
                "Service stopped even though the runFuture did not complete.",
                () -> s.awaitTerminated(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof TimeoutException);
        Assert.assertEquals("Unexpected state while shutting down.", Service.State.STOPPING, s.state());

        // Complete the future and await normal termination.
        s.runFuture.complete(null);
        s.awaitTerminated();
        Assert.assertEquals("Unexpected state upon normal shutdown.", Service.State.TERMINATED, s.state());
    }


    /**
     * Tests the behavior of AbstractThreadPoolService when the runFuture completes (normally or not).
     */
    @Test
    public void testAutoShutdown() {
        // When completed normally.
        @Cleanup
        val s1 = newService();
        s1.runFuture.complete(null);
        s1.awaitTerminated();
        Assert.assertEquals("Unexpected state upon auto-shutdown (normal completion).", Service.State.TERMINATED, s1.state());

        // When completed with failure.
        @Cleanup
        val s2 = newService();
        s2.runFuture.completeExceptionally(new IntentionalException());
        AssertExtensions.assertThrows(
                "Service did not fail when runFuture failed.",
                () -> s2.awaitTerminated(),
                ex -> ex instanceof IllegalStateException);
        Assert.assertEquals("Unexpected state upon auto-shutdown (failure).", Service.State.FAILED, s2.state());
        Assert.assertTrue("Unexpected failure cause.", s2.failureCause() instanceof IntentionalException);
    }


    /**
     * Tests the behavior of AbstractThreadPoolService during shutdown when only a StopException is present.
     */
    @Test
    public void testShutdownStopException() {
        @Cleanup
        val s = newService();

        // Stop it and verify it hasn't shut down - it should still be waiting on the runFuture.
        val correctEx = new IntentionalException();
        val wrongEx = new IntentionalException();
        s.errorHandler(correctEx);
        s.errorHandler(wrongEx);

        s.stopAsync();
        s.runFuture.complete(null);
        AssertExtensions.assertThrows(
                "Service did not fail when a StopException has been recorded.",
                () -> s.awaitTerminated(),
                ex -> ex instanceof IllegalStateException);
        Assert.assertEquals("Unexpected state upon failed shutdown.", Service.State.FAILED, s.state());
        Assert.assertEquals("Unexpected failure cause.", correctEx, s.failureCause());

    }

    /**
     * Tests the behavior of AbstractThreadPoolService during shutdown when only a RunException is present.
     */
    @Test
    public void testShutdownRunException() {
        @Cleanup
        val s = newService();

        s.stopAsync();
        s.runFuture.completeExceptionally(new IntentionalException());
        AssertExtensions.assertThrows(
                "Service did not fail when runFuture failed.",
                () -> s.awaitTerminated(),
                ex -> ex instanceof IllegalStateException);
        Assert.assertEquals("Unexpected state upon failed shutdown.", Service.State.FAILED, s.state());
        Assert.assertTrue("Unexpected failure cause.", s.failureCause() instanceof IntentionalException);
    }

    /**
     * Tests the behavior of AbstractThreadPoolService during shutdown when both a StopException and a RunException are present.
     */
    @Test
    public void testShutdownStopAndRunException() {
        val s = newService();

        // Stop it and verify it hasn't shut down - it should still be waiting on the runFuture.
        val stopException = new IntentionalException("stop");
        s.errorHandler(stopException);
        val runException = new IntentionalException("run");
        s.runFuture.completeExceptionally(runException);

        AssertExtensions.assertThrows(
                "Service did not fail.",
                () -> s.awaitTerminated(),
                ex -> ex instanceof IllegalStateException);
        Assert.assertEquals("Unexpected state upon failed shutdown.", Service.State.FAILED, s.state());
        Assert.assertEquals("Unexpected failure cause.", stopException, s.failureCause());
        Assert.assertEquals("Unexpected suppressed exception.", runException, s.failureCause().getSuppressed()[0]);
    }

    /**
     * Tests the case when the Service stops due to a CancellationException.
     */
    @Test
    public void testCancellationException() {
        val s = newService();

        // Stop it and verify it hasn't shut down - it should still be waiting on the runFuture.
        val stopException = new IntentionalException("stop");
        s.errorHandler(stopException);
        s.runFuture.completeExceptionally(new CancellationException());

        // Complete the future and await normal termination.
        s.awaitTerminated();
        Assert.assertEquals("Unexpected state.", Service.State.TERMINATED, s.state());
    }

    TestService newService() {
        val s = new TestService();
        s.startAsync().awaitRunning();
        return s;
    }

    private class TestService extends AbstractThreadPoolService {
        private final CompletableFuture<Void> runFuture;

        TestService() {
            super(TestService.class.getName(), executorService());
            this.runFuture = new CompletableFuture<>();
        }

        @Override
        protected Duration getShutdownTimeout() {
            return TIMEOUT;
        }

        @Override
        protected CompletableFuture<Void> doRun() {
            return this.runFuture;
        }
    }
}
