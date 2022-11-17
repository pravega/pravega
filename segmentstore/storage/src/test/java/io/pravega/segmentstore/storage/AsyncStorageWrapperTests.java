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
package io.pravega.segmentstore.storage;

import io.pravega.common.Exceptions;
import io.pravega.common.util.ReusableLatch;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for AsyncStorageWrapper class.
 *
 * How these tests work:
 * - A TestStorage is created that invokes a common function with two arguments: operation code and segment name.
 * - Each test has two ReusableLatch maps: one for when an operation was invoked, and one when the operation should complete.
 * - In order to detect when an operation begins, the test awaits the "invoked" latch for that operation.
 * - In order to finalize an operation, the test releases the "waitOn" latch for that operation.
 * - The tests (based on the scenario) will do all sort of checks in between (and after) these steps to verify the class
 * works as expected.
 */
public class AsyncStorageWrapperTests extends ThreadPooledTestSuite {
    private static final int LOCK_TIMEOUT_MILLIS = 50;
    private static final int TIMEOUT_MILLIS = LOCK_TIMEOUT_MILLIS * 100;
    private static final Duration TIMEOUT = Duration.ofMillis(TIMEOUT_MILLIS);

    @Rule
    public Timeout globalTimeout = Timeout.millis(TIMEOUT_MILLIS);

    @Override
    protected int getThreadPoolSize() {
        return 2;
    }

    /**
     * Tests pass-through functionality (the ability to invoke the appropriate method in the inner SyncStorage).
     */
    @Test
    public void testPassThrough() {
        final String segmentName = "Segment";
        final String concatSourceName = "Concat";
        val handle = InMemoryStorage.newHandle(segmentName, false);
        AtomicReference<Object> toReturn = new AtomicReference<>();
        AtomicReference<BiConsumer<String, String>> validator = new AtomicReference<>();
        val innerStorage = new TestStorage((operation, segment) -> {
            validator.get().accept(operation, segment);
            return toReturn.get();
        });

        @Cleanup
        val s = new AsyncStorageWrapper(innerStorage, executorService());

        // Create
        toReturn.set(handle);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.CREATE, o);
            Assert.assertEquals(segmentName, segment);
        });
        val createResult = s.create(segmentName, TIMEOUT).join();
        Assert.assertEquals(toReturn.get(), createResult);

        // Delete
        toReturn.set(null);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.DELETE, o);
            Assert.assertEquals(segmentName, segment);
        });
        s.delete(handle, TIMEOUT).join();

        // OpenRead
        toReturn.set(handle);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.OPEN_READ, o);
            Assert.assertEquals(segmentName, segment);
        });
        val openReadResult = s.openRead(segmentName).join();
        Assert.assertEquals(toReturn.get(), openReadResult);

        // OpenWrite
        toReturn.set(handle);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.OPEN_WRITE, o);
            Assert.assertEquals(segmentName, segment);
        });
        val openWriteResult = s.openWrite(segmentName).join();
        Assert.assertEquals(toReturn.get(), openWriteResult);

        // GetInfo
        toReturn.set(StreamSegmentInformation.builder().name(segmentName).build());
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.GET_INFO, o);
            Assert.assertEquals(segmentName, segment);
        });
        val getInfoResult = s.getStreamSegmentInfo(segmentName, TIMEOUT).join();
        Assert.assertEquals(toReturn.get(), getInfoResult);

        // Exists
        toReturn.set(true);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.EXISTS, o);
            Assert.assertEquals(segmentName, segment);
        });
        val existsResult = s.exists(segmentName, TIMEOUT).join();
        Assert.assertEquals(toReturn.get(), existsResult);

        // Read
        toReturn.set(10);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.READ, o);
            Assert.assertEquals(segmentName, segment);
        });
        val readResult = s.read(handle, 0, new byte[0], 0, 0, TIMEOUT).join();
        Assert.assertEquals(toReturn.get(), readResult);

        // Write
        toReturn.set(null);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.WRITE, o);
            Assert.assertEquals(segmentName, segment);
        });
        s.write(handle, 0, new ByteArrayInputStream(new byte[0]), 0, TIMEOUT).join();

        // Seal
        toReturn.set(null);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.SEAL, o);
            Assert.assertEquals(segmentName, segment);
        });
        s.seal(handle, TIMEOUT).join();

        // Concat
        toReturn.set(null);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.CONCAT, o);
            Assert.assertEquals(segmentName + "|" + concatSourceName, segment);
        });
        s.concat(handle, 0, concatSourceName, TIMEOUT).join();

        // Truncate
        toReturn.set(null);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.TRUNCATE, o);
            Assert.assertEquals(segmentName, segment);
        });
        s.truncate(handle, 0, TIMEOUT).join();
    }

    /**
     * Tests basic same-segment concurrency for simple operations. Since all operations use the same sequencing mechanism
     * it suffices to test using two arbitrary operations instead of every possible pair.
     */
    @Test
    public void testConcurrencySameSegment() throws Exception {
        final String segmentName = "Segment";
        final String op1 = TestStorage.CREATE;
        final String op2 = TestStorage.DELETE;

        // Create a set of latches that can be used to detect when an operation was invoked and when to release it.
        val invoked = new HashMap<String, ReusableLatch>();
        val waitOn = new HashMap<String, ReusableLatch>();
        invoked.put(op1, new ReusableLatch());
        invoked.put(op2, new ReusableLatch());
        waitOn.put(op1, new ReusableLatch());
        waitOn.put(op2, new ReusableLatch());

        val innerStorage = new TestStorage((operation, segment) -> {
            invoked.get(operation).release();
            try {
                waitOn.get(operation).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Exceptions.sneakyThrow(e);
            }
            return null;
        });

        @Cleanup
        val s = new AsyncStorageWrapper(innerStorage, executorService());

        // Begin executing one operation (Create) and wait for it properly "acquire" the lock.
        val futures = new ArrayList<CompletableFuture<?>>();
        futures.add(s.create(segmentName, TIMEOUT));
        invoked.get(op1).await(LOCK_TIMEOUT_MILLIS);
        Assert.assertEquals("Unexpected number of active segments.", 1, s.getSegmentWithOngoingOperationsCount());

        // Begin executing a second operation (Delete) and verify that it hasn't started within a reasonable amount of time.
        futures.add(s.delete(InMemoryStorage.newHandle(segmentName, false), TIMEOUT));
        AssertExtensions.assertThrows(
                "Second operation was invoked while the first one was still running.",
                () -> invoked.get(op2).await(LOCK_TIMEOUT_MILLIS),
                ex -> ex instanceof TimeoutException);

        // Complete the first operation and await the second operation to begin executing, then release it too.
        waitOn.get(op1).release();
        invoked.get(op2).await(LOCK_TIMEOUT_MILLIS);
        Assert.assertEquals("Unexpected number of active segments.", 1, s.getSegmentWithOngoingOperationsCount());
        waitOn.get(op2).release();

        // Wait for both operations to complete. This will re-throw any exceptions that may have occurred.
        allOf(futures).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected final number of active segments.", 0, s.getSegmentWithOngoingOperationsCount());
    }

    /**
     * Tests the fact that different segment do not interfere (block) with each other for concurrent operations.
     */
    @Test
    public void testConcurrencyDifferentSegment() throws Exception {
        final String segment1 = "Segment1";
        final String segment2 = "Segment2";

        // Create a set of latches that can be used to detect when an operation was invoked and when to release it.
        val invoked = new HashMap<String, ReusableLatch>();
        val waitOn = new HashMap<String, ReusableLatch>();
        invoked.put(segment1, new ReusableLatch());
        invoked.put(segment2, new ReusableLatch());
        waitOn.put(segment1, new ReusableLatch());
        waitOn.put(segment2, new ReusableLatch());

        val innerStorage = new TestStorage((operation, segment) -> {
            invoked.get(segment).release();
            try {
                waitOn.get(segment).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Exceptions.sneakyThrow(e);
            }
            return null;
        });

        @Cleanup
        val s = new AsyncStorageWrapper(innerStorage, executorService());

        // Begin executing one create.
        val futures = new ArrayList<CompletableFuture<?>>();
        futures.add(s.create(segment1, TIMEOUT));
        invoked.get(segment1).await(LOCK_TIMEOUT_MILLIS);
        Assert.assertEquals("Unexpected number of active segments.", 1, s.getSegmentWithOngoingOperationsCount());

        // Begin executing the second create and verify it is not blocked by the first one.
        futures.add(s.create(segment2, TIMEOUT));
        invoked.get(segment2).await(LOCK_TIMEOUT_MILLIS);
        Assert.assertEquals("Unexpected number of active segments.", 2, s.getSegmentWithOngoingOperationsCount());

        // Complete the first operation and await the second operation to begin executing, then release it too.
        waitOn.get(segment1).release();
        waitOn.get(segment2).release();

        // Wait for both operations to complete. This will re-throw any exceptions that may have occurred.
        allOf(futures).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected final number of active segments.", 0, s.getSegmentWithOngoingOperationsCount());
    }

    /**
     * Tests the segment-based concurrency when concat is involved. In particular, that a concat() will wait for any pending
     * operations on each involved segment and that any subsequent operation on any of those segments will be queued up.
     */
    @Test
    public void testConcurrencyConcat() throws Exception {
        final String segment1 = "Segment1";
        final String segment2 = "Segment2";
        final BiFunction<String, String, String> joiner = (op, segment) -> op + "|" + segment;
        final String createSegment1Key = joiner.apply(TestStorage.CREATE, segment1);
        final String createSegment2Key = joiner.apply(TestStorage.CREATE, segment2);
        final String concatKey = joiner.apply(TestStorage.CONCAT, segment1 + "|" + segment2);
        final String writeSegment1Key = joiner.apply(TestStorage.WRITE, segment1);
        final String writeSegment2Key = joiner.apply(TestStorage.WRITE, segment2);

        // Create a set of latches that can be used to detect when an operation was invoked and when to release it.
        val invoked = new HashMap<String, ReusableLatch>();
        val waitOn = new HashMap<String, ReusableLatch>();
        invoked.put(createSegment1Key, new ReusableLatch());
        invoked.put(createSegment2Key, new ReusableLatch());
        invoked.put(concatKey, new ReusableLatch());
        invoked.put(writeSegment1Key, new ReusableLatch());
        invoked.put(writeSegment2Key, new ReusableLatch());

        waitOn.put(createSegment1Key, new ReusableLatch());
        waitOn.put(createSegment2Key, new ReusableLatch());
        waitOn.put(concatKey, new ReusableLatch());
        waitOn.put(writeSegment1Key, new ReusableLatch());
        waitOn.put(writeSegment2Key, new ReusableLatch());

        val innerStorage = new TestStorage((operation, segment) -> {
            invoked.get(joiner.apply(operation, segment)).release();
            try {
                waitOn.get(joiner.apply(operation, segment)).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Exceptions.sneakyThrow(e);
            }
            return null;
        });

        @Cleanup
        val s = new AsyncStorageWrapper(innerStorage, executorService());

        // Issue two Create operations with the two segments and wait for both of them to be running.
        val futures = new ArrayList<CompletableFuture<?>>();
        futures.add(s.create(segment1, TIMEOUT));
        futures.add(s.create(segment2, TIMEOUT));
        invoked.get(createSegment1Key).await(LOCK_TIMEOUT_MILLIS);
        invoked.get(createSegment2Key).await(LOCK_TIMEOUT_MILLIS);
        Assert.assertEquals("Unexpected number of active segments.", 2, s.getSegmentWithOngoingOperationsCount());

        // Initiate the concat and complete one of the original operations, and verify the concat did not start.
        futures.add(s.concat(InMemoryStorage.newHandle(segment1, false), 0, segment2, TIMEOUT));
        waitOn.get(createSegment1Key).release();
        AssertExtensions.assertThrows(
                "Concat was invoked while the at least one of the creates was running.",
                () -> invoked.get(concatKey).await(LOCK_TIMEOUT_MILLIS),
                ex -> ex instanceof TimeoutException);

        // Finish up the "source" create and verify the concat is released.
        waitOn.get(createSegment2Key).release();
        invoked.get(concatKey).await(TIMEOUT_MILLIS);

        // Add more operations after the concat and verify they are queued up (that they haven't started).
        futures.add(s.write(InMemoryStorage.newHandle(segment1, false), 0, new ByteArrayInputStream(new byte[0]), 0, TIMEOUT));
        futures.add(s.write(InMemoryStorage.newHandle(segment2, false), 0, new ByteArrayInputStream(new byte[0]), 0, TIMEOUT));
        AssertExtensions.assertThrows(
                "Write(target) was invoked while concat was running",
                () -> invoked.get(writeSegment1Key).await(LOCK_TIMEOUT_MILLIS),
                ex -> ex instanceof TimeoutException);

        AssertExtensions.assertThrows(
                "Write(source) was invoked while concat was running",
                () -> invoked.get(writeSegment2Key).await(LOCK_TIMEOUT_MILLIS),
                ex -> ex instanceof TimeoutException);
        Assert.assertEquals("Unexpected number of active segments.", 2, s.getSegmentWithOngoingOperationsCount());

        // Finish up the concat and verify the two writes are released.
        waitOn.get(concatKey).release();
        invoked.get(writeSegment1Key).await(LOCK_TIMEOUT_MILLIS);
        invoked.get(writeSegment2Key).await(LOCK_TIMEOUT_MILLIS);
        waitOn.get(writeSegment1Key).release();
        waitOn.get(writeSegment2Key).release();

        allOf(futures).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected number of active segments.", 0, s.getSegmentWithOngoingOperationsCount());

    }

    @Test
    public void testSupportsAtomicWrites() {
        val innerStorage = new TestStorage((operation, segment) -> null);
        @Cleanup
        val s = new AsyncStorageWrapper(innerStorage, executorService());
        Assert.assertFalse(s.supportsAtomicWrites());
    }

    private CompletableFuture<Void> allOf(Collection<CompletableFuture<?>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }

    //region TestStorage

    @RequiredArgsConstructor
    private static class TestStorage implements SyncStorage {
        private static final String OPEN_READ = "openRead";
        private static final String GET_INFO = "getInfo";
        private static final String EXISTS = "exists";
        private static final String READ = "read";
        private static final String OPEN_WRITE = "openWrite";
        private static final String CREATE = "create";
        private static final String DELETE = "delete";
        private static final String WRITE = "write";
        private static final String SEAL = "seal";
        private static final String CONCAT = "concat";
        private static final String TRUNCATE = "truncate";
        private final BiFunction<String, String, Object> methodInvoked;

        @Override
        public SegmentHandle openRead(String streamSegmentName) {
            return (SegmentHandle) this.methodInvoked.apply(OPEN_READ, streamSegmentName);
        }

        @Override
        public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) {
            return (Integer) this.methodInvoked.apply(READ, handle.getSegmentName());
        }

        @Override
        public SegmentProperties getStreamSegmentInfo(String streamSegmentName) {
            return (SegmentProperties) this.methodInvoked.apply(GET_INFO, streamSegmentName);
        }

        @Override
        public boolean exists(String streamSegmentName) {
            return (Boolean) this.methodInvoked.apply(EXISTS, streamSegmentName);
        }

        @Override
        public SegmentHandle openWrite(String streamSegmentName) {
            return (SegmentHandle) this.methodInvoked.apply(OPEN_WRITE, streamSegmentName);
        }

        @Override
        public SegmentHandle create(String streamSegmentName) {
            return (SegmentHandle) this.methodInvoked.apply(CREATE, streamSegmentName);
        }

        @Override
        public void delete(SegmentHandle handle) {
            this.methodInvoked.apply(DELETE, handle.getSegmentName());
        }

        @Override
        public void write(SegmentHandle handle, long offset, InputStream data, int length) {
            this.methodInvoked.apply(WRITE, handle.getSegmentName());
        }

        @Override
        public void seal(SegmentHandle handle) {
            this.methodInvoked.apply(SEAL, handle.getSegmentName());
        }

        @Override
        public void concat(SegmentHandle targetHandle, long offset, String sourceSegment) {
            this.methodInvoked.apply(CONCAT, targetHandle.getSegmentName() + "|" + sourceSegment);
        }

        @Override
        public void truncate(SegmentHandle handle, long offset) {
            this.methodInvoked.apply(TRUNCATE, handle.getSegmentName());
        }

        // region Unimplemented methods

        @Override
        public void unseal(SegmentHandle handle) {
        }

        @Override
        public boolean supportsTruncation() {
            return true;
        }

        @Override
        public void close() {
        }

        @Override
        public void initialize(long containerEpoch) {
        }

        @Override
        public Iterator<SegmentProperties> listSegments() {
            return null;
        }
        //endregion
    }

    //endregion
}
