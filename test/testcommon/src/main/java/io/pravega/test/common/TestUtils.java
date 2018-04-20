/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.common;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import io.pravega.test.common.AssertExtensions.RunnableWithException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class for Tests.
 */
@Slf4j
public class TestUtils {
    // Linux uses ports from range 32768 - 61000.
    private static final int BASE_PORT = 32768;
    private static final int MAX_PORT_COUNT = 28233;

    // We use a random start position here to avoid ports conflicts when this method is executed from multiple processes
    // in parallel. This is needed since the processes will contend for the same port sequence.
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(new Random().nextInt(MAX_PORT_COUNT));

    /**
     * A helper method to get a random free port.
     *
     * @return free port.
     */
    public static int getAvailableListenPort() {
        for (int i = 0; i < MAX_PORT_COUNT; i++) {
            int candidatePort = BASE_PORT + NEXT_PORT.getAndIncrement() % MAX_PORT_COUNT;
            try {
                ServerSocket serverSocket = new ServerSocket(candidatePort);
                serverSocket.close();
                return candidatePort;
            } catch (IOException e) {
                // Do nothing. Try another port.
            }
        }
        throw new IllegalStateException(
                String.format("Could not assign port in range %d - %d", BASE_PORT, MAX_PORT_COUNT + BASE_PORT));
    }

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
            if (isBlocked.tryAcquire(200, TimeUnit.MILLISECONDS)) {
                if (exception.get() != null) {
                    throw new RuntimeException("Blocking code threw an exception", exception.get());
                } else {
                    throw new RuntimeException("Failed to block.");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        unblocker.run();
        try {
            if (!isBlocked.tryAcquire(2000, TimeUnit.MILLISECONDS)) {
                RuntimeException e = new RuntimeException("Failed to unblock");
                e.setStackTrace(t.getStackTrace());
                t.interrupt();
                throw new RuntimeException(e);
            }
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
