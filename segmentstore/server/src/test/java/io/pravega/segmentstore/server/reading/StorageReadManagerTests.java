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

import io.pravega.common.hash.RandomFactory;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.storage.ReadOnlyStorage;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.val;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the StorageReadManager class.
 */
public class StorageReadManagerTests extends ThreadPooledTestSuite {
    private static final int MIN_SEGMENT_LENGTH = 101;
    private static final int MAX_SEGMENT_LENGTH = MIN_SEGMENT_LENGTH * 100;
    private static final SegmentMetadata SEGMENT_METADATA = new StreamSegmentMetadata("Segment1", 0, 0);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests the execute method with valid Requests:
     * * All StreamSegments exist and have enough data.
     * * All read offsets are valid (but we may choose to read more than the length of the Segment).
     * * ReadRequests may overlap.
     */
    @Test
    public void testValidRequests() throws Exception {
        final int defaultReadLength = MIN_SEGMENT_LENGTH - 1;
        final int offsetIncrement = defaultReadLength / 3;

        @Cleanup
        Storage storage = InMemoryStorageFactory.newStorage(executorService());
        storage.initialize(1);
        byte[] segmentData = populateSegment(storage);
        @Cleanup
        StorageReadManager reader = new StorageReadManager(SEGMENT_METADATA, storage, executorService());
        HashMap<StorageReadManager.Request, CompletableFuture<StorageReadManager.Result>> requestCompletions = new HashMap<>();
        int readOffset = 0;
        while (readOffset < segmentData.length) {
            int readLength = Math.min(defaultReadLength, segmentData.length - readOffset);
            CompletableFuture<StorageReadManager.Result> requestCompletion = new CompletableFuture<>();
            StorageReadManager.Request r = new StorageReadManager.Request(readOffset, readLength, requestCompletion::complete, requestCompletion::completeExceptionally, TIMEOUT);
            reader.execute(r);
            requestCompletions.put(r, requestCompletion);
            readOffset += offsetIncrement;
        }

        // Check that the read requests returned with the right data.
        for (val entry : requestCompletions.entrySet()) {
            StorageReadManager.Result readData = entry.getValue().join();
            StorageReadManager.Request request = entry.getKey();
            int expectedReadLength = Math.min(request.getLength(), (int) (segmentData.length - request.getOffset()));

            Assert.assertNotNull("No data returned for request " + request, readData);
            Assert.assertEquals("Unexpected read length for request " + request, expectedReadLength, readData.getData().getLength());
            AssertExtensions.assertStreamEquals(
                "Unexpected read contents for request " + request,
                new ByteArrayInputStream(segmentData, (int) request.getOffset(), expectedReadLength),
                readData.getData().getReader(), expectedReadLength);
        }
    }

    /**
     * Tests the execute method with invalid Requests.
     * * StreamSegment does not exist
     * * Invalid read offset
     * * Too long of a read (offset+length is beyond the Segment's length)
     */
    @Test
    public void testInvalidRequests() {
        @Cleanup
        Storage storage = InMemoryStorageFactory.newStorage(executorService());
        storage.initialize(1);

        // Segment does not exist.
        AssertExtensions.assertThrows(
                "Request was not failed when StreamSegment does not exist.",
                () -> {
                    SegmentMetadata sm = new StreamSegmentMetadata(SEGMENT_METADATA.getName(), 0, 0);
                    @Cleanup
                    StorageReadManager nonExistentReader = new StorageReadManager(sm, storage, executorService());
                    sendRequest(nonExistentReader, 0, 1).join();
                },
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Now create segment, it should exist and request should succeed.
        byte[] segmentData = populateSegment(storage);
        @Cleanup
        StorageReadManager reader = new StorageReadManager(SEGMENT_METADATA, storage, executorService());
        sendRequest(reader, 0, 1).join();

        // Invalid read offset.
        AssertExtensions.assertSuppliedFutureThrows(
                "Request was not failed when bad offset was provided.",
                () -> sendRequest(reader, segmentData.length + 1, 1),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        // Invalid read length.
        AssertExtensions.assertSuppliedFutureThrows(
                "Request was not failed when bad offset + length was provided.",
                () -> sendRequest(reader, segmentData.length - 1, 2),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        // Make sure valid request succeeds after invalid one
        sendRequest(reader, 0, 1).join();
    }

    private CompletableFuture<StorageReadManager.Result> sendRequest(StorageReadManager reader, long offset, int length) {
        CompletableFuture<StorageReadManager.Result> requestCompletion = new CompletableFuture<>();
        reader.execute(new StorageReadManager.Request(offset, length, requestCompletion::complete, requestCompletion::completeExceptionally, TIMEOUT));
        return requestCompletion;
    }

    /**
     * Tests the ability to queue dependent reads (subsequent reads that only want to read a part of a previous read).
     * Test this both with successful and failed reads.
     */
    @Test
    public void testDependents() {
        final Duration waitTimeout = Duration.ofSeconds(5);
        TestStorage storage = new TestStorage();
        CompletableFuture<Integer> signal = new CompletableFuture<>();
        AtomicBoolean wasReadInvoked = new AtomicBoolean();
        storage.readImplementation = () -> {
            if (wasReadInvoked.getAndSet(true)) {
                Assert.fail("Read was invoked multiple times, which is a likely indicator that the requests were not chained.");
            }
            return signal;
        };

        @Cleanup
        StorageReadManager reader = new StorageReadManager(SEGMENT_METADATA, storage, executorService());

        // Create some reads.
        CompletableFuture<StorageReadManager.Result> c1 = new CompletableFuture<>();
        CompletableFuture<StorageReadManager.Result> c2 = new CompletableFuture<>();
        reader.execute(new StorageReadManager.Request(0, 100, c1::complete, c1::completeExceptionally, TIMEOUT));
        reader.execute(new StorageReadManager.Request(50, 100, c2::complete, c2::completeExceptionally, TIMEOUT));

        Assert.assertFalse("One or more of the reads has completed prematurely.", c1.isDone() || c2.isDone());

        signal.completeExceptionally(new IntentionalException());
        AssertExtensions.assertThrows(
                "The first read was not failed with the correct exception.",
                () -> c1.get(waitTimeout.toMillis(), TimeUnit.MILLISECONDS),
                ex -> ex instanceof IntentionalException);

        AssertExtensions.assertThrows(
                "The second read was not failed with the correct exception.",
                () -> c2.get(waitTimeout.toMillis(), TimeUnit.MILLISECONDS),
                ex -> ex instanceof IntentionalException);
    }

    /**
     * Tests the ability to auto-cancel the requests when the StorageReadManager is closed.
     */
    @Test
    public void testAutoCancelRequests() {
        final int readCount = 100;
        TestStorage storage = new TestStorage();
        storage.readImplementation = CompletableFuture::new; // Just return a Future which we will never complete - simulates a high latency read.
        @Cleanup
        StorageReadManager reader = new StorageReadManager(SEGMENT_METADATA, storage, executorService());

        // Create some reads.
        HashMap<StorageReadManager.Request, CompletableFuture<StorageReadManager.Result>> requestCompletions = new HashMap<>();

        for (int i = 0; i < readCount; i++) {
            CompletableFuture<StorageReadManager.Result> requestCompletion = new CompletableFuture<>();
            StorageReadManager.Request r = new StorageReadManager.Request(i * 10, 9, requestCompletion::complete, requestCompletion::completeExceptionally, TIMEOUT);
            reader.execute(r);
            requestCompletions.put(r, requestCompletion);
        }

        // Verify the reads aren't failed yet.
        for (val entry : requestCompletions.entrySet()) {
            Assert.assertFalse("Request is unexpectedly completed before close for request " + entry.getKey(), entry.getValue().isDone());
        }

        // Close the reader and verify the reads have all been cancelled.
        reader.close();
        for (val entry : requestCompletions.entrySet()) {
            Assert.assertTrue("Request is not completed with exception after close for request " + entry.getKey(), entry.getValue().isCompletedExceptionally());
            AssertExtensions.assertThrows(
                    "Request was not failed with a CancellationException after close for request " + entry.getKey(),
                    entry.getValue()::join,
                    ex -> ex instanceof CancellationException);
        }
    }

    private byte[] populateSegment(Storage storage) {
        Random random = RandomFactory.create();
        int length = MIN_SEGMENT_LENGTH + random.nextInt(MAX_SEGMENT_LENGTH - MIN_SEGMENT_LENGTH);
        byte[] segmentData = new byte[length];
        random.nextBytes(segmentData);
        storage.create(SEGMENT_METADATA.getName(), TIMEOUT).join();
        val writeHandle = storage.openWrite(SEGMENT_METADATA.getName()).join();
        storage.write(writeHandle, 0, new ByteArrayInputStream(segmentData), segmentData.length, TIMEOUT).join();
        return segmentData;
    }

    private static class TestStorage implements ReadOnlyStorage {
        Supplier<CompletableFuture<Integer>> readImplementation;

        @Override
        public void initialize(long epoch) {
            // Nothing to do.
        }

        @Override
        public void close() {
            // Nothing to do.
        }

        @Override
        public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
            return CompletableFuture.completedFuture(InMemoryStorage.newHandle(streamSegmentName, true));
        }

        @Override
        public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
            return this.readImplementation.get();
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
            // This method is not needed.
            throw new NotImplementedException("getStreamSegmentInfo");
        }

        @Override
        public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
            // This method is not needed.
            throw new NotImplementedException("exists");
        }
    }
}
