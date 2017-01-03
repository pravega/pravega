/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.ObjectClosedException;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.IntentionalException;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for RedirectedReadResultEntry.
 */
public class RedirectedReadResultEntryTests {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final AtomicReference<ScheduledExecutorService> executorService = new AtomicReference<>();

    @Before
    public void before() {
        this.executorService.set(Executors.newScheduledThreadPool(1));
    }

    @After
    public void after() {
        this.executorService.get().shutdown();
    }

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
                () -> new RedirectedReadResultEntry(baseEntry, -originalOffset - 1, this::illegalGetNext, this.executorService.get()),
                ex -> ex instanceof IllegalArgumentException);

        // Adjust up.
        RedirectedReadResultEntry redirectedEntry = new TestRedirectedReadResultEntry(baseEntry, positiveDelta, this::illegalGetNext, this.executorService.get());
        Assert.assertEquals("Unexpected value for getStreamSegmentOffset after up-adjustment.", originalOffset + positiveDelta, redirectedEntry.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected value for getRequestedReadLength after up-adjustment (no change expected).", originalLength, redirectedEntry.getRequestedReadLength());

        // Adjust down.
        redirectedEntry = new TestRedirectedReadResultEntry(baseEntry, negativeDelta, this::illegalGetNext, this.executorService.get());
        Assert.assertEquals("Unexpected value for getStreamSegmentOffset after down-adjustment.", originalOffset + negativeDelta, redirectedEntry.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected value for getRequestedReadLength after down-adjustment (no change expected).", originalLength, redirectedEntry.getRequestedReadLength());

        // Verify other properties are as they should.
        Assert.assertEquals("Unexpected value for getType.", baseEntry.getType(), redirectedEntry.getType());
        Assert.assertEquals("Unexpected value for getRequestedReadLength.", baseEntry.getRequestedReadLength(), redirectedEntry.getRequestedReadLength());

        // getContent will be thoroughly tested in its own unit test.
        baseEntry.getContent().complete(new ReadResultEntryContents(new ByteArrayInputStream(new byte[1]), 1));
        Assert.assertEquals("Unexpected result for getContent.", baseEntry.getContent().join(), redirectedEntry.getContent().join());

        redirectedEntry.requestContent(Duration.ZERO);
        Assert.assertTrue("BaseEntry.getContent() was not completed when requestContent was invoked.", FutureHelpers.isSuccessful(baseEntry.getContent()));
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
        RedirectedReadResultEntry e1 = new TestRedirectedReadResultEntry(f1, 0, (o, l) -> f1, this.executorService.get());
        AssertExtensions.assertThrows(
                "requestContent did not throw when attempting to retry more than once.",
                () -> e1.requestContent(TIMEOUT),
                ex -> ex instanceof ObjectClosedException);

        // Ineligible exception.
        FailureReadResultEntry f2 = new FailureReadResultEntry(ReadResultEntryType.Cache, 1, 1, () -> {
            throw new IllegalArgumentException();
        });
        RedirectedReadResultEntry e2 = new TestRedirectedReadResultEntry(f1, 0, (o, l) -> f2, this.executorService.get());
        AssertExtensions.assertThrows(
                "requestContent did not throw when an ineligible exception got thrown.",
                () -> e2.requestContent(TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // Given back another Redirect.
        RedirectedReadResultEntry e3 = new TestRedirectedReadResultEntry(f1, 0, (o, l) -> e1, this.executorService.get());
        AssertExtensions.assertThrows(
                "requestContent did not throw when retry yielded another RedirectReadResultEntry.",
                () -> e3.requestContent(TIMEOUT),
                ex -> ex instanceof ObjectClosedException);

        // Given redirect function fails.
        RedirectedReadResultEntry e4 = new TestRedirectedReadResultEntry(f1, 0, (o, l) -> {
            throw new IntentionalException();
        }, this.executorService.get());
        AssertExtensions.assertThrows(
                "requestContent did not throw when retry failed.",
                () -> e4.requestContent(TIMEOUT),
                ex -> ex instanceof IntentionalException);

        // One that works correctly.
        AtomicBoolean requestInvoked = new AtomicBoolean();
        FailureReadResultEntry f5 = new FailureReadResultEntry(ReadResultEntryType.Cache, 2, 1, () -> requestInvoked.set(true));
        f1.setCompletionCallback(i -> { // Do nothing.
        });
        RedirectedReadResultEntry e5 = new TestRedirectedReadResultEntry(f1, 1, (o, l) -> f5, this.executorService.get());
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
        RedirectedReadResultEntry e1 = new TestRedirectedReadResultEntry(t1, 0, (o, l) -> t1, this.executorService.get());
        t1.getContent().completeExceptionally(new StreamSegmentNotExistsException("foo"));
        AssertExtensions.assertThrows(
                "getContent() did not throw when attempting to retry more than once.",
                e1::getContent,
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Ineligible exception.
        MockReadResultEntry t2 = new MockReadResultEntry(1, 1);
        RedirectedReadResultEntry e2 = new TestRedirectedReadResultEntry(t2, 0, (o, l) -> t2, this.executorService.get());
        t2.getContent().completeExceptionally(new IntentionalException());
        AssertExtensions.assertThrows(
                "getContent() did not throw when an ineligible exception got thrown.",
                e2::getContent,
                ex -> ex instanceof IntentionalException);

        // Given back another Redirect.
        MockReadResultEntry t3 = new MockReadResultEntry(1, 1);
        RedirectedReadResultEntry e3 = new TestRedirectedReadResultEntry(t3, 0, (o, l) -> e1, this.executorService.get());
        t3.getContent().completeExceptionally(new StreamSegmentNotExistsException("foo"));
        AssertExtensions.assertThrows(
                "getContent() did not throw when a retry yielded another RedirectReadResultEntry.",
                e3::getContent,
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Given redirect function fails.
        MockReadResultEntry t4 = new MockReadResultEntry(1, 1);
        t4.getContent().completeExceptionally(new StreamSegmentNotExistsException("foo"));
        RedirectedReadResultEntry e4 = new TestRedirectedReadResultEntry(t4, 0, (o, l) -> {
            throw new IntentionalException();
        }, this.executorService.get());
        AssertExtensions.assertThrows(
                "getContent() did not throw when retry failed.",
                e4::getContent,
                ex -> ex instanceof StreamSegmentNotExistsException);

        // One that works correctly.
        MockReadResultEntry t5Bad = new MockReadResultEntry(1, 1);
        t5Bad.getContent().completeExceptionally(new StreamSegmentNotExistsException("foo"));
        MockReadResultEntry t5Good = new MockReadResultEntry(2, 1);
        t1.setCompletionCallback(i -> { // Do nothing.
        });
        t5Good.getContent().complete(new ReadResultEntryContents(new ByteArrayInputStream(new byte[1]), 1));
        RedirectedReadResultEntry e5 = new TestRedirectedReadResultEntry(t5Bad, 1, (o, l) -> t5Good, this.executorService.get());
        val finalResult = e5.getContent().join();
        Assert.assertEquals("Unexpected result from getCompletionCallback after successful redirect.", t5Bad.getCompletionCallback(), t5Good.getCompletionCallback());
        Assert.assertEquals("Unexpected result from getRequestedReadLength after successful redirect.", t5Bad.getRequestedReadLength(), e5.getRequestedReadLength());
        Assert.assertEquals("Unexpected result from getStreamSegmentOffset after successful redirect.", t5Good.getStreamSegmentOffset(), e5.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected result from getContent after successful redirect.", t5Good.getContent().join(), finalResult);
    }

    private CompletableReadResultEntry illegalGetNext(long offset, int length) {
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

    private static class TestRedirectedReadResultEntry extends RedirectedReadResultEntry {
        TestRedirectedReadResultEntry(CompletableReadResultEntry baseEntry, long offsetAdjustment, GetEntry retryGetEntry, ScheduledExecutorService executorService) {
            super(baseEntry, offsetAdjustment, retryGetEntry, executorService);
        }

        @Override
        protected Duration getExceptionDelay(Throwable ex) {
            return Duration.ZERO;
        }
    }
}
