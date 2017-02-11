/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.IntentionalException;
import com.emc.pravega.testcommon.ThreadPooledTestSuite;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Unit tests for AsyncReadResultProcessor.
 */
public class AsyncReadResultProcessorTests extends ThreadPooledTestSuite {
    private static final int ENTRY_COUNT = 10000;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests the AsyncReadResultProcessor on catch-up reads (that are already available in memory).
     */
    @Test
    public void testCatchUpReads() throws Exception {
        // Pre-generate some entries.
        ArrayList<byte[]> entries = new ArrayList<>();
        int totalLength = generateEntries(entries);

        // Setup an entry provider supplier.
        AtomicInteger currentIndex = new AtomicInteger();
        StreamSegmentReadResult.NextEntrySupplier supplier = (offset, length) -> {
            int idx = currentIndex.getAndIncrement();
            if (idx >= entries.size()) {
                return null;
            }

            return new CacheReadResultEntry(offset, entries.get(idx), 0, entries.get(idx).length);
        };

        // Start an AsyncReadResultProcessor.
        @Cleanup
        StreamSegmentReadResult rr = new StreamSegmentReadResult(0, totalLength, supplier, "");
        TestReadResultHandler testReadResultHandler = new TestReadResultHandler(entries);
        try (AsyncReadResultProcessor rp = AsyncReadResultProcessor.process(rr, testReadResultHandler, executorService())) {
            // Wait for it to complete, and then verify that no errors have been recorded via the callbacks.
            testReadResultHandler.completed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            if (testReadResultHandler.error.get() != null) {
                Assert.fail("Read failure: " + testReadResultHandler.error.get().toString());
            }

            Assert.assertEquals("Unexpected number of reads processed.", entries.size(), testReadResultHandler.readCount.get());
        }

        Assert.assertTrue("ReadResult was not closed when the AsyncReadResultProcessor was closed.", rr.isClosed());
    }

    /**
     * Tests the AsyncReadResultProcessor on Future Reads (that are not yet available in memory, but soon would be).
     */
    @Test
    public void testFutureReads() throws Exception {
        // Pre-generate some entries.
        ArrayList<byte[]> entries = new ArrayList<>();
        int totalLength = generateEntries(entries);

        // Setup an entry provider supplier.
        AtomicInteger currentIndex = new AtomicInteger();
        StreamSegmentReadResult.NextEntrySupplier supplier = (offset, length) -> {
            int idx = currentIndex.getAndIncrement();
            if (idx >= entries.size()) {
                return null;
            }

            Supplier<ReadResultEntryContents> entryContentsSupplier = () -> new ReadResultEntryContents(new ByteArrayInputStream(entries.get(idx)), entries.get(idx).length);
            return new TestFutureReadResultEntry(offset, length, entryContentsSupplier, executorService());
        };

        // Start an AsyncReadResultProcessor.
        @Cleanup
        StreamSegmentReadResult rr = new StreamSegmentReadResult(0, totalLength, supplier, "");
        TestReadResultHandler testReadResultHandler = new TestReadResultHandler(entries);
        try (AsyncReadResultProcessor rp = AsyncReadResultProcessor.process(rr, testReadResultHandler, executorService())) {
            // Wait for it to complete, and then verify that no errors have been recorded via the callbacks.
            testReadResultHandler.completed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            if (testReadResultHandler.error.get() != null) {
                Assert.fail("Read failure: " + testReadResultHandler.error.toString());
            }

            Assert.assertEquals("Unexpected number of reads processed.", entries.size(), testReadResultHandler.readCount.get());
        }

        Assert.assertTrue("ReadResult was not closed when the AsyncReadResultProcessor was closed.", rr.isClosed());
    }

    /**
     * Tests the AsyncReadResultProcessor when it encounters read failures.
     */
    @Test
    public void testReadFailures() throws Exception {
        // Pre-generate some entries.
        final int totalLength = 1000;
        final Semaphore barrier = new Semaphore(0);

        // Setup an entry provider supplier that returns Future Reads, which will eventually fail.
        StreamSegmentReadResult.NextEntrySupplier supplier = (offset, length) -> {
            Supplier<ReadResultEntryContents> entryContentsSupplier = () -> {
                barrier.acquireUninterruptibly();
                throw new IntentionalException("Intentional");
            };

            return new TestFutureReadResultEntry(offset, length, entryContentsSupplier, executorService());
        };

        // Start an AsyncReadResultProcessor.
        @Cleanup
        StreamSegmentReadResult rr = new StreamSegmentReadResult(0, totalLength, supplier, "");
        TestReadResultHandler testReadResultHandler = new TestReadResultHandler(new ArrayList<>());
        try (AsyncReadResultProcessor rp = AsyncReadResultProcessor.process(rr, testReadResultHandler, executorService())) {
            barrier.release();

            // Wait for it to complete, and then verify that no errors have been recorded via the callbacks.
            AssertExtensions.assertThrows(
                    "Processor did not complete with the expected failure.",
                    () -> testReadResultHandler.completed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                    ex -> ExceptionHelpers.getRealException(ex) instanceof IntentionalException);

            Assert.assertEquals("Unexpected number of reads processed.", 0, testReadResultHandler.readCount.get());
            Assert.assertNotNull("No read failure encountered.", testReadResultHandler.error.get());
            Assert.assertTrue("Unexpected type of exception was raised: " + testReadResultHandler.error.get(), testReadResultHandler.error.get() instanceof IntentionalException);
        }

        Assert.assertTrue("ReadResult was not closed when the AsyncReadResultProcessor was closed.", rr.isClosed());
    }

    private int generateEntries(ArrayList<byte[]> entries) {
        int totalLength = 0;
        for (int i = 0; i < ENTRY_COUNT; i++) {
            byte[] data = String.format("AppendId=%d,AppendOffset=%d", i, totalLength).getBytes();
            entries.add(data);
            totalLength += data.length;
        }

        return totalLength;
    }

    private static class TestReadResultHandler implements AsyncReadResultHandler {
        public final AtomicReference<Throwable> error = new AtomicReference<>();
        public final AtomicInteger readCount = new AtomicInteger();
        private final AtomicInteger readEntryCount = new AtomicInteger();
        private final List<byte[]> entries;
        private final CompletableFuture<Void> completed;

        public TestReadResultHandler(List<byte[]> entries) {
            this.entries = entries;
            this.completed = new CompletableFuture<>();
        }

        @Override
        public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
            return true;
        }

        @Override
        public boolean processEntry(ReadResultEntry e) {
            try {
                Assert.assertTrue("Received Entry that is not ready to serve data yet.", FutureHelpers.isSuccessful(e.getContent()));
                ReadResultEntryContents c = e.getContent().join();
                byte[] data = new byte[c.getLength()];
                StreamHelpers.readAll(c.getData(), data, 0, data.length);
                int idx = readEntryCount.getAndIncrement();
                AssertExtensions.assertLessThan("Read too many entries.", entries.size(), idx);
                byte[] expected = entries.get(idx);
                Assert.assertArrayEquals(String.format("Unexpected read contents after reading %d entries.", idx + 1), expected, data);
                readCount.incrementAndGet();
            } catch (Exception ex) {
                processError(ex);
                return false;
            }

            return true;
        }

        @Override
        public void processError(Throwable cause) {
            this.error.set(cause);
            Assert.assertFalse("Result is already completed.", this.completed.isDone());
            this.completed.completeExceptionally(cause);
        }

        @Override
        public void processResultComplete() {
            Assert.assertFalse("Result is already completed.", this.completed.isDone());
            this.completed.complete(null);
        }

        @Override
        public Duration getRequestContentTimeout() {
            return TIMEOUT;
        }
    }

    private static class TestFutureReadResultEntry extends FutureReadResultEntry {
        private final Supplier<ReadResultEntryContents> resultSupplier;
        private final Executor executor;

        TestFutureReadResultEntry(long streamSegmentOffset, int requestedReadLength, Supplier<ReadResultEntryContents> resultSupplier, Executor executor) {
            super(streamSegmentOffset, requestedReadLength);
            this.resultSupplier = resultSupplier;
            this.executor = executor;
        }

        public void complete(ReadResultEntryContents contents) {
            super.complete(contents);
        }

        /**
         * Cancels this pending read result entry.
         */
        public void cancel() {
            fail(new CancellationException());
        }

        public void fail(Throwable cause) {
            super.fail(cause);
        }

        @Override
        public void requestContent(Duration timeout) {
            this.executor.execute(() -> {
                try {
                    complete(this.resultSupplier.get());
                } catch (Exception ex) {
                    fail(ex);
                }
            });
        }
    }
}
