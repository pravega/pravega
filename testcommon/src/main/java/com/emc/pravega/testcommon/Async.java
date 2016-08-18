package com.emc.pravega.testcommon;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;

public class Async {

    public static <Result> Result testBlocking(Callable<Result> blockingFunction, Runnable unblocker) {
        final AtomicReference<Result> result = new AtomicReference<>(null);
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
            Assert.assertFalse(isBlocked.tryAcquire(100, TimeUnit.MILLISECONDS));
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
