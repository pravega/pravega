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
package io.pravega.segmentstore.server.reading;

import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for RedirectedReadResultEntry.
 */
public class RedirectedReadResultEntryTests {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests the ability of the ReadResultEntry base class to adjust offsets when instructed so.
     */
    @Test
    public void testConstructor() {
        final long originalOffset = 123;
        final int originalLength = 321;
        final int positiveDelta = 8976;
        final int negativeDelta = -76;
        val baseEntry = new MockReadResultEntry(originalOffset, originalLength);

        AssertExtensions.assertThrows(
                "Constructor allowed changing to a negative offset.",
                () -> new RedirectedReadResultEntry(baseEntry, -originalOffset - 1, this::illegalGetNext, 2),
                ex -> ex instanceof IllegalArgumentException);

        // Adjust up.
        RedirectedReadResultEntry redirectedEntry = new RedirectedReadResultEntry(baseEntry, positiveDelta, this::illegalGetNext, 2);
        Assert.assertEquals("Unexpected value for getStreamSegmentOffset after up-adjustment.", originalOffset + positiveDelta, redirectedEntry.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected value for getRequestedReadLength after up-adjustment (no change expected).", originalLength, redirectedEntry.getRequestedReadLength());

        // Adjust down.
        redirectedEntry = new RedirectedReadResultEntry(baseEntry, negativeDelta, this::illegalGetNext, 2);
        Assert.assertEquals("Unexpected value for getStreamSegmentOffset after down-adjustment.", originalOffset + negativeDelta, redirectedEntry.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected value for getRequestedReadLength after down-adjustment (no change expected).", originalLength, redirectedEntry.getRequestedReadLength());

        // Verify other properties are as they should.
        Assert.assertEquals("Unexpected value for getType.", baseEntry.getType(), redirectedEntry.getType());
        Assert.assertEquals("Unexpected value for getRequestedReadLength.", baseEntry.getRequestedReadLength(), redirectedEntry.getRequestedReadLength());

        // getContent will be thoroughly tested in its own unit test.
        baseEntry.getContent().complete(new ByteArraySegment(new byte[1]));
        Assert.assertEquals("Unexpected result for getContent.", baseEntry.getContent().join(), redirectedEntry.getContent().join());

        redirectedEntry.requestContent(Duration.ZERO);
        Assert.assertTrue("BaseEntry.getContent() was not completed when requestContent was invoked.", Futures.isSuccessful(baseEntry.getContent()));
    }

    /**
     * Tests the ability to retry (and switch base) when a failure occurred in requestContent().
     */
    @Test
    public void testRequestContent() {
        // More than one retry (by design, it will only retry one time; the next time it will simply throw).
        FailureReadResultEntry f1 = new FailureReadResultEntry(ReadResultEntryType.Cache, 1, 1, () -> {
            throw new ObjectClosedException(this);
        });
        RedirectedReadResultEntry e1 = new RedirectedReadResultEntry(f1, 0, (o, l, m) -> f1, 2);
        AssertExtensions.assertThrows(
                "requestContent did not throw when attempting to retry more than once.",
                () -> e1.requestContent(TIMEOUT),
                ex -> ex instanceof ObjectClosedException);

        // Ineligible exception.
        FailureReadResultEntry f2 = new FailureReadResultEntry(ReadResultEntryType.Cache, 1, 1, () -> {
            throw new IllegalArgumentException();
        });
        RedirectedReadResultEntry e2 = new RedirectedReadResultEntry(f1, 0, (o, l, m) -> f2, 2);
        AssertExtensions.assertThrows(
                "requestContent did not throw when an ineligible exception got thrown.",
                () -> e2.requestContent(TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // Given back another Redirect.
        RedirectedReadResultEntry e3 = new RedirectedReadResultEntry(f1, 0, (o, l, m) -> e1, 2);
        AssertExtensions.assertThrows(
                "requestContent did not throw when retry yielded another RedirectReadResultEntry.",
                () -> e3.requestContent(TIMEOUT),
                ex -> ex instanceof ObjectClosedException);

        // Given redirect function fails.
        RedirectedReadResultEntry e4 = new RedirectedReadResultEntry(f1, 0, (o, l, m) -> {
            throw new IntentionalException();
        }, 2);
        AssertExtensions.assertThrows(
                "requestContent did not throw when retry failed.",
                () -> e4.requestContent(TIMEOUT),
                ex -> ex instanceof IntentionalException);

        // One that works correctly.
        AtomicBoolean requestInvoked = new AtomicBoolean();
        FailureReadResultEntry f5 = new FailureReadResultEntry(ReadResultEntryType.Cache, 2, 1, () -> requestInvoked.set(true));
        f1.setCompletionCallback(i -> { // Do nothing.
        });
        RedirectedReadResultEntry e5 = new RedirectedReadResultEntry(f1, 1, (o, l, m) -> f5, 2);
        e5.requestContent(TIMEOUT);
        Assert.assertTrue("requestTimeout was not invoked for successful redirect.", requestInvoked.get());
        Assert.assertEquals("Unexpected result from getCompletionCallback after successful redirect.", f5.getCompletionCallback(), f1.getCompletionCallback());
        Assert.assertEquals("Unexpected result from getRequestedReadLength after successful redirect.", f5.getRequestedReadLength(), e5.getRequestedReadLength());
        Assert.assertEquals("Unexpected result from getStreamSegmentOffset after successful redirect.", f5.getStreamSegmentOffset(), e5.getStreamSegmentOffset());
    }

    /**
     * Tests the ability to retry (and switch base) when a failure occurred in getContent().
     */
    @Test
    public void testGetContent() {
        // More than one retry (by design, it will only retry one time; the next time it will simply throw).
        MockReadResultEntry t1 = new MockReadResultEntry(1, 1);
        RedirectedReadResultEntry e1 = new RedirectedReadResultEntry(t1, 0, (o, l, m) -> {
            Assert.assertEquals("Unexpected merge offset.", 2, m);
            return t1;
        }, 2);
        t1.getContent().completeExceptionally(new StreamSegmentNotExistsException("foo"));
        AssertExtensions.assertSuppliedFutureThrows(
                "getContent() did not throw when attempting to retry more than once.",
                e1::getContent,
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Ineligible exception.
        MockReadResultEntry t2 = new MockReadResultEntry(1, 1);
        RedirectedReadResultEntry e2 = new RedirectedReadResultEntry(t2, 0, (o, l, m) -> t2, 2);
        t2.getContent().completeExceptionally(new IntentionalException());
        AssertExtensions.assertSuppliedFutureThrows(
                "getContent() did not throw when an ineligible exception got thrown.",
                e2::getContent,
                ex -> ex instanceof IntentionalException);

        // Given back another Redirect.
        MockReadResultEntry t3 = new MockReadResultEntry(1, 1);
        RedirectedReadResultEntry e3 = new RedirectedReadResultEntry(t3, 0, (o, l, m) -> e1, 2);
        t3.getContent().completeExceptionally(new StreamSegmentNotExistsException("foo"));
        AssertExtensions.assertSuppliedFutureThrows(
                "getContent() did not throw when a retry yielded another RedirectReadResultEntry.",
                e3::getContent,
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Given redirect function fails.
        MockReadResultEntry t4 = new MockReadResultEntry(1, 1);
        t4.getContent().completeExceptionally(new StreamSegmentNotExistsException("foo"));
        RedirectedReadResultEntry e4 = new RedirectedReadResultEntry(t4, 0, (o, l, m) -> {
            throw new IntentionalException();
        }, 2);
        AssertExtensions.assertSuppliedFutureThrows(
                "getContent() did not throw when retry failed.",
                e4::getContent,
                ex -> ex instanceof StreamSegmentNotExistsException);

        // One that works correctly.
        MockReadResultEntry t5Bad = new MockReadResultEntry(1, 1);
        t5Bad.getContent().completeExceptionally(new StreamSegmentNotExistsException("foo"));
        MockReadResultEntry t5Good = new MockReadResultEntry(2, 1);
        t1.setCompletionCallback(i -> { // Do nothing.
        });
        t5Good.getContent().complete(new ByteArraySegment(new byte[1]));
        RedirectedReadResultEntry e5 = new RedirectedReadResultEntry(t5Bad, 1, (o, l, m) -> t5Good, 2);
        val finalResult = e5.getContent().join();
        Assert.assertEquals("Unexpected result from getCompletionCallback after successful redirect.", t5Bad.getCompletionCallback(), t5Good.getCompletionCallback());
        Assert.assertEquals("Unexpected result from getRequestedReadLength after successful redirect.", t5Bad.getRequestedReadLength(), e5.getRequestedReadLength());
        Assert.assertEquals("Unexpected result from getStreamSegmentOffset after successful redirect.", t5Good.getStreamSegmentOffset(), e5.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected result from getContent after successful redirect.", t5Good.getContent().join(), finalResult);
    }

    @Test
    public void testFail() {
        MockReadResultEntry t1 = new MockReadResultEntry(1, 1);
        val retryInvoked = new AtomicBoolean(false);
        RedirectedReadResultEntry e1 = new RedirectedReadResultEntry(t1, 0, (o, l, m) -> {
            retryInvoked.set(true);
            return null;
        }, 2);
        e1.fail(new CancellationException());
        Assert.assertTrue("Expected the callback to have been failed.", t1.getContent().isCompletedExceptionally());
        Assert.assertFalse("Not expected a retry invocation.", retryInvoked.get());
    }

    private CompletableReadResultEntry illegalGetNext(long offset, int length, long mergeOffset) {
        throw new IllegalStateException("Cannot invoke this operation at this time.");
    }

    private static class FailureReadResultEntry extends ReadResultEntryBase {
        private final Runnable requestContent;

        FailureReadResultEntry(ReadResultEntryType type, long streamSegmentOffset, int requestedReadLength, Runnable requestContent) {
            super(type, streamSegmentOffset, requestedReadLength);
            this.requestContent = requestContent;
        }

        @Override
        public void requestContent(Duration timeout) {
            this.requestContent.run();
        }
    }

    private static class MockReadResultEntry extends ReadResultEntryBase {
        MockReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
            super(ReadResultEntryType.Cache, streamSegmentOffset, requestedReadLength);
        }

        @Override
        public void requestContent(Duration timeout) {
            this.getContent().complete(null);
        }
    }
}
