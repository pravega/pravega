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

package com.emc.logservice.server.reading;

import com.emc.logservice.common.StreamHelpers;
import com.emc.logservice.contracts.ReadResultEntryContents;
import com.emc.logservice.server.CloseableExecutorService;
import com.emc.logservice.server.ServiceShutdownListener;
import com.emc.nautilus.testcommon.AssertExtensions;
import com.emc.nautilus.testcommon.IntentionalException;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Unit tests for AsyncReadResultProcessor.
 */
public class AsyncReadResultProcessorTests {
    private static final int ENTRY_COUNT = 10000;
    private static final int THREAD_POOL_SIZE = 50;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final String SEGMENT_NAME = "Segment";

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

            return new MemoryReadResultEntry(new ByteArrayReadIndexEntry(offset, entries.get(idx)), 0, entries.get(idx).length);
        };

        // Setup an entry processor.
        AtomicInteger readCount = new AtomicInteger();
        AtomicReference<Throwable> readFailure = new AtomicReference<>();
        val entryProcessor = createEntryProcessor(entries, readCount, readFailure);

        // Start an AsyncReadResultProcessor.
        @Cleanup
        CloseableExecutorService executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
        @Cleanup
        StreamSegmentReadResult rr = new StreamSegmentReadResult(SEGMENT_NAME, 0, totalLength, supplier, "");
        try (AsyncReadResultProcessor rp = new AsyncReadResultProcessor(rr, UUID.randomUUID(), entryProcessor, f -> readFailure.set(f.getCause()), executor.get())) {
            rp.startAsync().awaitRunning();

            // Wait for it to complete, and then verify that no errors have been recorded via the callbacks.
            ServiceShutdownListener.awaitShutdown(rp, TIMEOUT, true);

            if (readFailure.get() != null) {
                Assert.fail("Read failure: " + readFailure.toString());
            }

            Assert.assertEquals("Unexpected number of reads processed.", entries.size(), readCount.get());
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
        @Cleanup
        CloseableExecutorService executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
        AtomicInteger currentIndex = new AtomicInteger();
        StreamSegmentReadResult.NextEntrySupplier supplier = (offset, length) -> {
            int idx = currentIndex.getAndIncrement();
            if (idx >= entries.size()) {
                return null;
            }

            Supplier<ReadResultEntryContents> entryContentsSupplier = () -> new ReadResultEntryContents(new ByteArrayInputStream(entries.get(idx)), entries.get(idx).length);
            return new TestPlaceholderReadResultEntry(offset, length, entryContentsSupplier, executor.get());
        };

        // Setup an entry processor.
        AtomicInteger readCount = new AtomicInteger();
        AtomicReference<Throwable> readFailure = new AtomicReference<>();
        val entryProcessor = createEntryProcessor(entries, readCount, readFailure);

        // Start an AsyncReadResultProcessor.
        @Cleanup
        StreamSegmentReadResult rr = new StreamSegmentReadResult(SEGMENT_NAME, 0, totalLength, supplier, "");
        try (AsyncReadResultProcessor rp = new AsyncReadResultProcessor(rr, UUID.randomUUID(), entryProcessor, f -> readFailure.set(f.getCause()), executor.get())) {
            rp.startAsync().awaitRunning();

            // Wait for it to complete, and then verify that no errors have been recorded via the callbacks.
            ServiceShutdownListener.awaitShutdown(rp, TIMEOUT, true);

            if (readFailure.get() != null) {
                Assert.fail("Read failure: " + readFailure.toString());
            }

            Assert.assertEquals("Unexpected number of reads processed.", entries.size(), readCount.get());
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

        // Setup an entry provider supplier that returns Future Reads, which will eventually fail.
        @Cleanup
        CloseableExecutorService executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
        StreamSegmentReadResult.NextEntrySupplier supplier = (offset, length) -> {
            Supplier<ReadResultEntryContents> entryContentsSupplier = () -> {
                throw new IntentionalException("Intentional");
            };

            return new TestPlaceholderReadResultEntry(offset, length, entryContentsSupplier, executor.get());
        };

        // Setup an entry processor.
        AtomicInteger readCount = new AtomicInteger();
        AtomicReference<Throwable> readFailure = new AtomicReference<>();
        Function<AsyncReadResultProcessor.AsyncReadResultEntry, Boolean> entryProcessor = e -> {
            readCount.incrementAndGet();
            return true;
        };

        // Start an AsyncReadResultProcessor.
        @Cleanup
        StreamSegmentReadResult rr = new StreamSegmentReadResult(SEGMENT_NAME, 0, totalLength, supplier, "");
        try (AsyncReadResultProcessor rp = new AsyncReadResultProcessor(rr, UUID.randomUUID(), entryProcessor, f -> readFailure.set(f.getCause()), executor.get())) {
            rp.startAsync().awaitRunning();

            // Wait for it to complete, and then verify that no errors have been recorded via the callbacks.
            ServiceShutdownListener.awaitShutdown(rp, TIMEOUT, true);

            Assert.assertEquals("Unexpected number of reads processed.", 0, readCount.get());
            Assert.assertNotNull("No read failure encountered.", readFailure.get());
            Assert.assertTrue("Unexpected type of exception was raised.", readFailure.get() instanceof IntentionalException);
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

    private Function<AsyncReadResultProcessor.AsyncReadResultEntry, Boolean> createEntryProcessor(ArrayList<byte[]> entries, AtomicInteger readCount, AtomicReference<Throwable> readFailure) {
        AtomicInteger readEntryCount = new AtomicInteger();
        return e -> {
            ReadResultEntryContents c = e.getEntry().getContent().join();
            byte[] data = new byte[c.getLength()];
            try {
                StreamHelpers.readAll(c.getData(), data, 0, data.length);
                int idx = readEntryCount.getAndIncrement();
                AssertExtensions.assertLessThan("Read too many entries.", entries.size(), idx);
                byte[] expected = entries.get(idx);
                Assert.assertArrayEquals(String.format("Unexpected read contents after reading %d entries.", idx + 1), expected, data);
                readCount.incrementAndGet();
            } catch (Exception ex) {
                readFailure.set(ex);
                return false;
            }

            return true;
        };
    }

    private static class TestPlaceholderReadResultEntry extends PlaceholderReadResultEntry {
        private final Supplier<ReadResultEntryContents> resultSupplier;
        private final Executor executor;

        public TestPlaceholderReadResultEntry(long streamSegmentOffset, int requestedReadLength, Supplier<ReadResultEntryContents> resultSupplier, Executor executor) {
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
            super.cancel();
        }

        public void fail(Throwable cause) {
            super.fail(cause);
        }

        @Override
        public CompletableFuture<ReadResultEntryContents> getContent() {
            this.executor.execute(() -> {
                try {
                    complete(this.resultSupplier.get());
                } catch (Exception ex) {
                    fail(ex);
                }
            });
            return super.getContent();
        }
    }
}
