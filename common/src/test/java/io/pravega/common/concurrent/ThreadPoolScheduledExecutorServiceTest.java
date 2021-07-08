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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ThreadPoolScheduledExecutorServiceTest {

    private static ThreadPoolScheduledExecutorService createPool(int min, int max) {
        return new ThreadPoolScheduledExecutorService(min,
                max,
                100,
                ExecutorServiceHelpers.getThreadFactory("ThreadPoolScheduledExecutorServiceTest"),
                new AbortPolicy());
    }
    
    
    @Test(timeout = 10000)
    public void testRunsTask() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1, 1);
        CompletableFuture<Integer> result = new CompletableFuture<Integer>();
        pool.submit(() -> result.complete(5));
        assertEquals(Integer.valueOf(5), result.get(5, SECONDS));
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, SECONDS));
    }
    
    @Test(timeout = 10000)
    public void testRunsDelayTask() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1, 1);
        CompletableFuture<Long> result = new CompletableFuture<Long>();
        long startTime = System.nanoTime();
        pool.schedule(() -> result.complete(System.nanoTime()), 100, MILLISECONDS);
        long runTime = result.get(5, SECONDS);
        assertTrue(runTime > startTime + 50 * 1000 * 1000);
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, SECONDS));
    }
    
    @Test(timeout = 10000)
    public void testCancelsDelayTask() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1, 1);
        AtomicInteger count = new AtomicInteger(0);
        ScheduledFuture<Integer> future = pool.schedule(() -> count.incrementAndGet(), 100, SECONDS);
        assertTrue(future.cancel(false));
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
        AssertExtensions.assertThrows(CancellationException.class, () -> future.get());
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, SECONDS));
    }
    
    @Test(timeout = 10000)
    public void testSpawnsOptionalThreads() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(3, 3);
        AtomicInteger count = new AtomicInteger(0);
        CyclicBarrier barrior = new CyclicBarrier(4);
        AtomicReference<Exception> error = new AtomicReference<>();
        pool.submit(() -> {
            count.incrementAndGet();
            try {
                barrior.await();
            } catch (Exception e) {
                error.set(e);
            } 
        });
        pool.submit(() -> {
            count.incrementAndGet();
            try {
                barrior.await();
            } catch (Exception e) {
                error.set(e);
            } 
        });
        pool.submit(() -> {
            count.incrementAndGet();
            try {
                barrior.await();
            } catch (Exception e) {
                error.set(e);
            } 
        });
        barrior.await(5, SECONDS);
        assertEquals(3, count.get());
        assertNull(error.get());
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, SECONDS));
    }
    
    @Test(timeout = 10000)
    public void testRunsDelayLoop() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1, 1);
        AtomicInteger count = new AtomicInteger(0);
        long startTime = System.nanoTime();
        ScheduledFuture<?> future = pool.scheduleWithFixedDelay(() -> {
            int value = count.incrementAndGet();
            if (value >= 20) {
                throw new RuntimeException("Expected test error");
            }
        }, 10, 10, MILLISECONDS);
        AssertExtensions.assertEventuallyEquals(20, () -> count.get(), 5000);
        AssertExtensions.assertThrows(RuntimeException.class, () -> future.get(5000, MILLISECONDS));
        assertTrue(System.nanoTime() > startTime + 19 * 10 * 1000 * 1000L);
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, SECONDS));
        assertEquals(20, count.get());
    }
    
    
    @Test(timeout = 10000)
    public void testRunsRateLoop() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1, 1);
        AtomicInteger count = new AtomicInteger(0);
        long startTime = System.nanoTime();
        ScheduledFuture<?> future = pool.scheduleAtFixedRate(() -> {
            int value = count.incrementAndGet();
            if (value >= 20) {
                throw new RuntimeException("Expected test error");
            }
        }, 10, 10, MILLISECONDS);
        AssertExtensions.assertEventuallyEquals(20, () -> count.get(), 5000);
        AssertExtensions.assertThrows(RuntimeException.class, () -> future.get(5000, MILLISECONDS));
        assertTrue(System.nanoTime() > startTime + 19 * 10 * 1000 * 1000L);
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, SECONDS));
        assertEquals(20, count.get());
    }
    
}
