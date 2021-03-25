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
package io.pravega.segmentstore.storage.cache;

import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.Data;
import lombok.val;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Benchmark tests for {@link CacheStorage} and its implementations. This should be used to compare the runtime performance
 * of various changes in the implementation. Since the results may vary significantly based on the hardware used, outcomes
 * are not comparable across environments. The way to use this is to establish a benchmark using the base code and then
 * re-run it with the modifications already implemented and tested.
 *
 * The number of iterations {@link #ITERATION_COUNT} should always be greater than 1 since the first iteration will incur
 * the costs of the initial memory allocation, while the remaining ones will benefit from the memory already being allocated.
 *
 * This is marked as @Ignore since these are not real unit tests (no correctness checking) and they take a long time to execute.
 */
@Ignore
public class BenchmarkTests {
    private static final long MAX_CACHE_SIZE = 16 * 1024 * 1024 * 1024L;
    private static final int ENTRY_SIZE = 10 * 1024;
    private static final int ENTRY_COUNT = 1000 * 1000;
    private static final int ITERATION_COUNT = 5;
    private static final int RANDOM_OPERATIONS_THREAD_COUNT = 1;
    private static final int RANDOM_OPERATIONS_INSERT_PERCENTAGE = 60; // Must be 0-100.
    private final Random random = new Random(0);

    /**
     * Tests sequential operations:
     * - {@link #ENTRY_COUNT} calls to {@link CacheStorage#insert} (with an entry of size {@link #ENTRY_SIZE}).
     * - {@link #ENTRY_COUNT} calls to {@link CacheStorage#append} (with data of size {@link CacheStorage#getAppendableLength}).
     * - {@link #ENTRY_COUNT} calls to {@link CacheStorage#replace} (with an entry of equal size).
     * - {@link #ENTRY_COUNT} calls to {@link CacheStorage#get}.
     * - {@link #ENTRY_COUNT} calls to {@link CacheStorage#delete}.
     */
    @Test
    public void testSequentialOperations() {
        test(this::testSequentialOperations);
    }

    private SequentialResult testSequentialOperations(CacheStorage s) {
        val writeBuffer = new ByteArraySegment(new byte[ENTRY_SIZE]);
        val appendBuffer = new ByteArraySegment(writeBuffer.array(), 0, s.getAppendableLength(ENTRY_SIZE));
        val readBuffer = new byte[ENTRY_SIZE];
        this.random.nextBytes(writeBuffer.array());
        int[] ids = new int[ENTRY_COUNT];

        val insert = measure(() -> {
            for (int i = 0; i < ENTRY_COUNT; i++) {
                ids[i] = s.insert(writeBuffer);
            }
        });

        val append = measure(() -> {
            for (int i = 0; i < ENTRY_COUNT; i++) {
                s.append(ids[i], writeBuffer.getLength(), appendBuffer);
            }
        });

        val replace = measure(() -> {
            for (int i = 0; i < ENTRY_COUNT; i++) {
                ids[i] = s.replace(ids[i], writeBuffer);
            }
        });

        val get = measure(() -> {
            for (int i = 0; i < ENTRY_COUNT; i++) {
                BufferView result = s.get(ids[i]);
                result.copyTo(ByteBuffer.wrap(readBuffer));
            }
        });

        val delete = measure(() -> {
            for (int i = 0; i < ENTRY_COUNT; i++) {
                s.delete(ids[i]);
            }
        });

        return new SequentialResult(insert, replace, append, get, delete);
    }

    /**
     * Tests {@link #ENTRY_COUNT} random operations with {@link #RANDOM_OPERATIONS_INSERT_PERCENTAGE} chance of insertions
     * (and 100%-{@link #RANDOM_OPERATIONS_INSERT_PERCENTAGE} chance of deletions).
     */
    @Test
    public void testRandomOperations() {
        test(this::testRandomOperations);
    }

    private RandomResult testRandomOperations(CacheStorage s) {
        val writeBuffer = new ByteArraySegment(new byte[ENTRY_SIZE]);
        this.random.nextBytes(writeBuffer.array());
        val ids = new ArrayList<Integer>();

        val timer = new Timer();
        val threads = new ArrayList<Thread>();
        val iterations = new AtomicInteger(0);
        val insertCount = new AtomicInteger(0);
        val getCount = new AtomicInteger(0);
        val deleteCount = new AtomicInteger(0);
        for (int threadId = 0; threadId < RANDOM_OPERATIONS_THREAD_COUNT; threadId++) {
            val t = new Thread(() -> {
                val readBuffer = new byte[ENTRY_SIZE * 2];
                int i;
                while ((i = iterations.incrementAndGet()) <= ENTRY_COUNT) {
                    boolean insert;
                    int length;
                    synchronized (ids) {
                        insert = (this.random.nextInt(100) < RANDOM_OPERATIONS_INSERT_PERCENTAGE) || ids.isEmpty();
                        length = insert ? this.random.nextInt(writeBuffer.getLength()) : 0;
                    }

                    if (insert) {
                        int insertedId = s.insert(writeBuffer.slice(0, length));
                        synchronized (ids) {
                            ids.add(insertedId);
                        }
                        insertCount.incrementAndGet();
                    } else {
                        // delete
                        int toRemove;
                        synchronized (ids) {
                            toRemove = ids.remove(this.random.nextInt(ids.size()));
                        }
                        s.delete(toRemove);
                        deleteCount.incrementAndGet();
                    }

                    int toRead = -1;
                    synchronized (ids) {
                        if (!ids.isEmpty()) {
                            toRead = ids.get(this.random.nextInt(ids.size()));
                        }
                    }
                    if (toRead >= 0) {
                        BufferView result = s.get(toRead);
                        if (result != null) {
                            result.copyTo(ByteBuffer.wrap(readBuffer));
                        }
                        getCount.incrementAndGet();
                    }
                }
            });
            t.start();
            threads.add(t);
        }

        for (val t : threads) {
            Exceptions.handleInterrupted(t::join);
        }

        Duration elapsed = timer.getElapsed();
        ids.forEach(s::delete); // do not count this.
        return new RandomResult(elapsed, insertCount.get(), getCount.get(), deleteCount.get());
    }

    private <T> void test(Function<CacheStorage, T> toTest) {
        @Cleanup
        val s = new DirectMemoryCache(MAX_CACHE_SIZE);
        for (int i = 0; i < ITERATION_COUNT; i++) {
            val r = toTest.apply(s);
            System.out.println(String.format("#%d: %s", i + 1, r));
        }
    }

    private Duration measure(Runnable toRun) {
        System.gc();
        val timer = new Timer();
        toRun.run();
        return timer.getElapsed();
    }

    @Data
    private static class RandomResult {
        final Duration elapsed;
        final int insertCount;
        final int getCount;
        final int deleteCount;

        @Override
        public String toString() {
            return String.format("Elapsed: %dms, InsertCount: %d, GetCount: %d, DeleteCount: %d",
                    elapsed.toMillis(), this.insertCount, this.getCount, this.deleteCount);
        }
    }

    @Data
    private static class SequentialResult {
        final Duration insert;
        final Duration replace;
        final Duration append;
        final Duration get;
        final Duration delete;

        @Override
        public String toString() {
            return String.format("Insert: %dms, Replace: %dms, Append: %dms, Get: %dms, Delete: %dms",
                    insert.toMillis(), replace.toMillis(), append.toMillis(), get.toMillis(), delete.toMillis());
        }
    }
}
