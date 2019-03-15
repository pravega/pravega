/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.filesystem;

import io.pravega.common.io.FileHelpers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit tests for FileSystemBasedLock.
 */
public class FileSystemLockTest {

    static final long ONE_HOUR = Duration.of(1, ChronoUnit.HOURS).toMillis();
    static final long ONE_MILLI = Duration.of(1, ChronoUnit.MILLIS).toMillis();
    static final long TWO_MILLI = Duration.of(2, ChronoUnit.MILLIS).toMillis();
    static final long HUNDRED_MILLI = Duration.of(100, ChronoUnit.MILLIS).toMillis();

    private File baseDir = null;
    private AtomicBoolean inCriticalSection = new AtomicBoolean();
    private Random rnd = new Random();

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
    }

    @After
    public void tearDown() {
        FileHelpers.deleteFileOrDirectory(baseDir);
        baseDir = null;
    }

    /**
     * Test that lock file is created and deleted.
     * @throws Exception
     */
    @Test()
    public void testBasicLocking() throws Exception {
        Path lockFilepath = Paths.get(baseDir.getAbsolutePath(), "test1");
        FileSystemBasedLock lock = new FileSystemBasedLock(lockFilepath);
        Assert.assertTrue("Lock file should be created after creating lock.", Files.exists(lockFilepath));
        lock.close();
        Assert.assertFalse("Lock file should be deleted after close.", Files.exists(lockFilepath));
        // Calling close multiple times should be okay.
        lock.close();
    }

    /**
     * Create first lock but do not close it.
     * Try to create second lock with same file, the lock should timeout.
     * @throws Exception
     */
    @Test()
    public void testConflict() throws Exception {
        Path lockFilepath = Paths.get(baseDir.getAbsolutePath(), "test1");
        FileSystemBasedLock lock1 = new FileSystemBasedLock(lockFilepath, ONE_HOUR, ONE_MILLI, TWO_MILLI);
        Assert.assertTrue("Lock file should be created after creating lock.", Files.exists(lockFilepath));
        try {
            FileSystemBasedLock lock2 = new FileSystemBasedLock(lockFilepath, ONE_HOUR, ONE_MILLI, TWO_MILLI);
            Assert.fail("Exception should be thrown");
        } catch (TimeoutException ex) {
            // good exception thrown as expected
        } finally {
            Files.delete(lockFilepath);
        }
    }

    /**
     * Create a test lock, but don't close it.
     * Wait for it to expire.
     * Create second lock, it should succeed.
     * @throws Exception
     */
    @Test()
    public void testExpiredLock() throws Exception {
        Path lockFilepath = Paths.get(baseDir.getAbsolutePath(), "test1");
        FileSystemBasedLock lock1 = new FileSystemBasedLock(lockFilepath, ONE_MILLI, ONE_MILLI, HUNDRED_MILLI);
        Assert.assertTrue("Lock file should be created after creating lock.", Files.exists(lockFilepath));
        Thread.sleep(5);
        try {
            // The lock should be expired here.
            FileSystemBasedLock lock2 = new FileSystemBasedLock(lockFilepath, ONE_MILLI, ONE_MILLI, HUNDRED_MILLI);
        } finally {
            Files.deleteIfExists(lockFilepath);
        }
    }

    /**
     * Creates array of futures - each lambda asserts that only one lambda is in critical section.
     * @throws Exception
     */
    @Test()
    public void testMutualExclusion() throws Exception {
        Path lockFilepath = Paths.get(baseDir.getAbsolutePath(), "testMutualExclusion");
        int parallelism = 100;
        CompletableFuture[] futures = new CompletableFuture[parallelism];
        inCriticalSection.set(false);
        for (int i = 0; i < parallelism; i++) {
            futures[i] = CompletableFuture.runAsync(() -> {
                try (FileSystemBasedLock lock1 = new FileSystemBasedLock(lockFilepath, ONE_HOUR, ONE_MILLI, ONE_HOUR)) {
                    Assert.assertFalse("Mutual exclusion failed", inCriticalSection.get());
                    // Note that code under test i.e locking implementation and aseert is before the use of atomic boolean
                    inCriticalSection.set(true);
                    // Sleep some random time, during this time other thread will try to aquire the lock and potentially
                    // sees inCriticalSection == true if there is a bug in lock.
                    int limit = rnd.nextInt(5);
                    for (int j = 0; j < limit; j++) {
                        Thread.sleep(ONE_MILLI); // wait for random time.
                    }
                    inCriticalSection.set(false);
                } catch (Exception e) {
                   Assert.fail("Exception thrown:" + e.toString());
                }
            });
        }
        CompletableFuture.allOf(futures).join();
    }
}
