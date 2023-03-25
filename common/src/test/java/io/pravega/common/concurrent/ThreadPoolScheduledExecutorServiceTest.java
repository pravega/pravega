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

import io.pravega.common.util.ReusableLatch;
import io.pravega.test.common.AssertExtensions;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ThreadPoolScheduledExecutorServiceTest {

    private static ThreadPoolScheduledExecutorService createPool(int threads) {
        return new ThreadPoolScheduledExecutorService(threads,
                ExecutorServiceHelpers.getThreadFactory("ThreadPoolScheduledExecutorServiceTest"));
    }   
    
    @Test(timeout = 10000)
    public void testRunsTask() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1);
        CompletableFuture<Integer> result = new CompletableFuture<Integer>();
        Future<Boolean> future = pool.submit(() -> result.complete(5));
        assertEquals(Integer.valueOf(5), result.get(5, SECONDS));
        assertEquals(true, future.get());
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, SECONDS));
    }
    
    @Test(timeout = 10000)
    public void testRunsDelayTask() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1);
        CompletableFuture<Long> result = new CompletableFuture<Long>();
        long startTime = System.nanoTime();
        Future<Boolean> future = pool.schedule(() -> result.complete(System.nanoTime()), 100, MILLISECONDS);
        long runTime = result.get(5, SECONDS);
        assertTrue(runTime > startTime + 50 * 1000 * 1000);
        assertTrue(future.get(5, SECONDS));
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, SECONDS));
    }
    
    @Test(timeout = 10000)
    public void testCancelsDelayTask() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1);
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
        ThreadPoolScheduledExecutorService pool = createPool(3);
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
        ThreadPoolScheduledExecutorService pool = createPool(1);
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
        ThreadPoolScheduledExecutorService pool = createPool(1);
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
    
    @Test(timeout = 10000)
    public void testShutdown() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1);
        AtomicInteger count = new AtomicInteger(0);
        ReusableLatch latch = new ReusableLatch(false);
        AtomicReference<Exception> error = new AtomicReference<>();
        pool.submit(() -> {
            count.incrementAndGet();
            try {
                latch.await();
            } catch (Exception e) {
                error.set(e);
            } 
        });
        pool.submit(() -> count.incrementAndGet());
        assertFalse(pool.isShutdown());
        assertFalse(pool.isTerminated());
        pool.shutdown();
        assertTrue(pool.isShutdown());
        AssertExtensions.assertThrows(RejectedExecutionException.class,
                                      () -> pool.submit(() -> count.incrementAndGet()));
        latch.release();
        pool.awaitTermination(1, SECONDS);
        assertNull(error.get());
        assertTrue(pool.isTerminated());
        assertEquals(2, count.get());
    }
    
    @Test(timeout = 10000)
    public void testShutdownNow() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1);
        AtomicInteger count = new AtomicInteger(0);
        ReusableLatch latch = new ReusableLatch(false);
        AtomicReference<Exception> error = new AtomicReference<>();
        pool.submit(() -> {
            count.incrementAndGet();
            try {
                latch.await();
            } catch (Exception e) {
                error.set(e);
            } 
        });
        pool.submit(() -> count.incrementAndGet());
        assertFalse(pool.isShutdown());
        assertFalse(pool.isTerminated());
        AssertExtensions.assertEventuallyEquals(1, count::get, 5000);
        List<Runnable> remaining = pool.shutdownNow();
        assertEquals(1, remaining.size());
        assertTrue(pool.isShutdown());
        AssertExtensions.assertThrows(RejectedExecutionException.class,
                                      () -> pool.submit(() -> count.incrementAndGet()));
        //No need to call latch.release() because thread should be interupted
        assertTrue(pool.awaitTermination(5, SECONDS));
        assertTrue(pool.isTerminated());
        assertNotNull(error.get());
        assertEquals(InterruptedException.class, error.get().getClass());
        assertEquals(1, count.get());
    }
    
    @Test(timeout = 10000)
    public void testCancel() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1);
        AtomicInteger count = new AtomicInteger(0);
        ScheduledFuture<Integer> future = pool.schedule(() -> count.incrementAndGet(), 20, SECONDS);
        assertTrue(future.cancel(false));
        AssertExtensions.assertThrows(CancellationException.class, () -> future.get());
        assertEquals(0, count.get());
        assertTrue(pool.shutdownNow().isEmpty());
        assertTrue(pool.awaitTermination(1, SECONDS));
    }
    
    @Test(timeout = 10000)
    public void testCancelRecurring() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1);
        AtomicInteger count = new AtomicInteger(0);
        AtomicReference<Exception> error = new AtomicReference<>();
        ScheduledFuture<?> future = pool.scheduleAtFixedRate(() -> {
            count.incrementAndGet();
        }, 0, 20, SECONDS);
        assertFalse(future.isCancelled());
        assertFalse(future.isDone());
        AssertExtensions.assertEventuallyEquals(1, count::get, 5000);
        assertTrue(future.cancel(false));
        AssertExtensions.assertThrows(CancellationException.class, () -> future.get());
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
        assertEquals(1, count.get());
        assertTrue(pool.getQueue().isEmpty());
        assertTrue(pool.shutdownNow().isEmpty());
        assertTrue(pool.awaitTermination(1, SECONDS));
    }

    @Test(timeout = 10000)
    public void testCancelRecurringWithInitialDelay() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1);
        AtomicInteger count = new AtomicInteger(0);
        ScheduledFuture<?> future = pool.scheduleAtFixedRate(() -> {
            count.incrementAndGet();
        }, 10, 2, SECONDS);
        assertFalse(future.isCancelled());
        assertFalse(future.isDone());
        assertTrue(future.cancel(false));
        AssertExtensions.assertThrows(CancellationException.class, () -> future.get());
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
        assertEquals(0, count.get());
        assertTrue(pool.getQueue().isEmpty());
        assertTrue(pool.shutdownNow().isEmpty());
        assertTrue(pool.awaitTermination(1, SECONDS));
    }
    
    @Test(timeout = 10000)
    public void testShutdownWithRecurring() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1);
        ReusableLatch latch = new ReusableLatch(false);
        AtomicInteger count = new AtomicInteger(0);
        AtomicReference<Exception> error = new AtomicReference<>();
        ScheduledFuture<?> future = pool.scheduleAtFixedRate(() -> {
            count.incrementAndGet();
            try {
                latch.await();
            } catch (Exception e) {
                error.set(e);
            } 
        }, 0, 20, SECONDS);
        assertFalse(future.isCancelled());
        assertFalse(future.isDone());
        AssertExtensions.assertEventuallyEquals(1, count::get, 5000);
        pool.shutdown();
        latch.release();
        AssertExtensions.assertThrows(CancellationException.class, () -> future.get());
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
        assertEquals(1, count.get());
        assertTrue(pool.awaitTermination(1, SECONDS));
    }
    
    @Test(timeout = 10000)
    public void testDelays() throws Exception {
        ThreadPoolScheduledExecutorService pool = createPool(1);
        ScheduledFuture<?> f20 = pool.schedule(() -> { }, 20, SECONDS);
        ScheduledFuture<?> f30 = pool.schedule(() -> { }, 30, SECONDS);
        assertTrue(f20.getDelay(SECONDS) <= 20);
        assertTrue(f20.getDelay(SECONDS) > 18);
        assertTrue(f30.getDelay(SECONDS) <= 30);
        assertTrue(f30.getDelay(SECONDS) > 28);
        assertTrue(f20.compareTo(f30) < 0);
        assertTrue(f30.compareTo(f20) > 0);
        assertTrue(f30.compareTo(f30) == 0);
        pool.shutdown();
        assertTrue(pool.awaitTermination(1, SECONDS));
    }
}
