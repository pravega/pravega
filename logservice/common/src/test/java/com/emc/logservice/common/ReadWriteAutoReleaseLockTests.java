package com.emc.logservice.common;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Unit tests for ReadWriteAutoReleaseLock class.
 */
public class ReadWriteAutoReleaseLockTests {

    private static final Duration AcquireTimeout = Duration.ofMillis(30);

    /**
     * Tests the interaction between multiple Read locks (i.e., acquiring a second read lock while another read lock
     * is in progress).
     */
    @Test
    public void testReadReadLock() throws Exception {
        final int Count = 5;
        final ReadWriteAutoReleaseLock lock = new ReadWriteAutoReleaseLock();
        List<Thread> threads = new ArrayList<>();
        CompletableFuture<Void> waitFuture = new CompletableFuture<>();
        AtomicInteger lockCount = new AtomicInteger();

        // Start a number of parallel threads, have them acquire read locks and hold on to those locks until all of
        // them acquire it. If any timeout occurs (unable to acquire), we report this as failure and bail out.
        for (int i = 0; i < Count; i++) {
            Thread t = new Thread(() -> {
                try (AutoReleaseLock ignored = lock.acquireReadLock(AcquireTimeout)) {
                    if (lockCount.incrementAndGet() < Count) {
                        // There are still locks to be acquired.
                        waitFuture.join();
                    }
                    else {
                        // We are the last lock to be acquired.
                        waitFuture.complete(null);
                    }
                }
                catch (TimeoutException | InterruptedException ex) {
                    // Failed to acquire the lock due to timeout or test failure.
                    waitFuture.completeExceptionally(ex);
                }
            });

            t.start();
            threads.add(t);
        }

        try {
            waitFuture.get(AcquireTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (Exception ex) {
            // An error occurred while executing; stop all threads.
            threads.forEach(Thread::interrupt);
            throw ex;
        }

        Assert.assertEquals("Unexpected number of locks acquired.", Count, lockCount.get());
    }

    /**
     * Tests the acquisition of a Write lock when a Read lock is in progress.
     */
    @Test
    public void testReadWriteLock() throws Exception {
        ReadWriteAutoReleaseLock lock = new ReadWriteAutoReleaseLock();
        testSequentialLocks(lock::acquireReadLock, lock::acquireWriteLock);
    }

    /**
     * Tests the acquisition of a Read lock when a Write lock is in progress.
     */
    @Test
    public void testWriteReadLock() throws Exception {
        ReadWriteAutoReleaseLock lock = new ReadWriteAutoReleaseLock();
        testSequentialLocks(lock::acquireWriteLock, lock::acquireReadLock);
    }

    /**
     * Tests the acquisition of a Write lock when another Write lock is in progress.
     */
    @Test
    public void testWriteWriteLock() throws Exception {
        ReadWriteAutoReleaseLock lock = new ReadWriteAutoReleaseLock();
        testSequentialLocks(lock::acquireWriteLock, lock::acquireWriteLock);
    }

    /**
     * Tests the fact that two concurrent lock acquisitions are granted sequentially (and do not overlap).
     *
     * @param acquireLock1
     * @param acquireLock2
     */
    private void testSequentialLocks(LockAcquirer acquireLock1, LockAcquirer acquireLock2) throws Exception {
        final String L1Start = "L1Start";
        final String L1End = "L1End";
        final String L2Start = "L2Start";
        final String L2End = "L2End";
        final int ExpectedEventCount = 4;
        List<Thread> threads = new ArrayList<>();
        CompletableFuture<Void> waitFuture = new CompletableFuture<>();
        List<String> events = new ArrayList<>();

        Consumer<Void> successCallback = v -> {
            synchronized (events) {
                if (events.size() >= ExpectedEventCount) {
                    waitFuture.complete(null);
                }
            }
        };

        Thread thread1 = createThread(acquireLock1, L1Start, L1End, events, successCallback, waitFuture::completeExceptionally);
        thread1.start();
        threads.add(thread1);

        Thread thread2 = createThread(acquireLock2, L2Start, L2End, events, successCallback, waitFuture::completeExceptionally);
        thread2.start();
        threads.add(thread2);

        try {
            waitFuture.get(AcquireTimeout.toMillis() * 4, TimeUnit.MILLISECONDS);
        }
        catch (Exception ex) {
            // An error occurred while executing; stop all threads.
            threads.forEach(Thread::interrupt);
            throw ex;
        }

        // Check to see that all the events were recorded properly.
        Assert.assertEquals("Unexpected number of events recorded: " + String.join(",", events), ExpectedEventCount, events.size());
        int l1StartPos = events.indexOf(L1Start);
        int l1EndPos = events.indexOf(L1End);
        int l2StartPos = events.indexOf(L2Start);
        int l2EndPos = events.indexOf(L2End);
        Assert.assertFalse("At lease one of the events is missing: " + String.join(",", events), l1StartPos < 0 || l2StartPos < 0 || l1EndPos < 0 || l2EndPos < 0);
        Assert.assertTrue("The events in the same lock were not in order: " + String.join(",", events), l1StartPos < l1EndPos && l2StartPos < l2EndPos);

        // Check to see that the locks were acquired in order. Either L1 is before L2 or L2 is before L1
        Assert.assertTrue("Locks did not seem to be acquired atomically: " + String.join(",", events), (l1StartPos < l2StartPos && l1EndPos < l2StartPos) || (l2StartPos < l1StartPos && l2EndPos < l1StartPos));
    }

    private Thread createThread(LockAcquirer acquireLock, String startToken, String endToken, List<String> events, Consumer<Void> successCallback, Consumer<Throwable> failureCallback) {
        return new Thread(() ->
        {
            try (AutoReleaseLock ignored = acquireLock.apply(AcquireTimeout)) {
                // Add start/end tokens to the event list, and wait a bit between them.
                synchronized (events) {
                    events.add(startToken);
                }

                Thread.sleep(AcquireTimeout.toMillis() * 3 / 4);

                synchronized (events) {
                    events.add(endToken);
                }
            }
            catch (TimeoutException | InterruptedException ex) {
                // Failed to acquire the lock due to timeout or test failure.
                CallbackHelpers.invokeSafely(failureCallback, ex, null);
                return;
            }

            // Completed successfully.
            CallbackHelpers.invokeSafely(successCallback, null, null);
        });
    }

    private interface LockAcquirer {
        AutoReleaseLock apply(Duration t) throws TimeoutException, InterruptedException;
    }
}
