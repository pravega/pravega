/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.testcommon;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;

import io.pravega.testcommon.AssertExtensions.RunnableWithException;

public class Async {

    public static void testBlocking(RunnableWithException blockingFunction, Runnable unblocker) {
        final AtomicReference<Exception> exception = new AtomicReference<>(null);
        final Semaphore isBlocked = new Semaphore(0);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    blockingFunction.run();
                } catch (Exception e) {
                    exception.set(e);
                }
                isBlocked.release();
            }
        });
        t.start();
        try {
            Assert.assertFalse(isBlocked.tryAcquire(200, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        unblocker.run();
        try {
            isBlocked.acquire();
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (exception.get() != null) {
            throw new RuntimeException(exception.get());
        } 
    }
    
    public static <ResultT> ResultT testBlocking(Callable<ResultT> blockingFunction, Runnable unblocker) {
        final AtomicReference<ResultT> result = new AtomicReference<>(null);
        final AtomicReference<Exception> exception = new AtomicReference<>(null);
        final Semaphore isBlocked = new Semaphore(0);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    result.set(blockingFunction.call());
                } catch (Exception e) {
                    exception.set(e);
                }
                isBlocked.release();
            }
        });
        t.start();
        try {
            Assert.assertFalse(isBlocked.tryAcquire(200, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        unblocker.run();
        try {
            isBlocked.acquire();
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (exception.get() != null) {
            throw new RuntimeException(exception.get());
        } else {
            return result.get();
        }
    }
    
}
