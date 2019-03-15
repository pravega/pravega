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

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Implements a mutually exclusive lock by using file system.
 */
public class FileSystemBasedLock implements AutoCloseable {
    private static final long DEFAULT_SLEEP = Duration.of(10, ChronoUnit.MILLIS).toMillis();
    private static final long DEFAULT_TIMEOUT = Duration.of(1, ChronoUnit.SECONDS).toMillis();
    private static final long DEFAULT_LEASE_EXPIRATION = Duration.of(10, ChronoUnit.SECONDS).toMillis();

    Path path;

    /**
     * Creates file system based lock.
     * @param path Path to use for lock file.
     * @throws Exception Throws exception.
     */
    public FileSystemBasedLock(Path path) throws Exception {
        this(path, DEFAULT_LEASE_EXPIRATION, DEFAULT_SLEEP, DEFAULT_TIMEOUT);
    }

    /**
     * Creates file system based lock.
     * @param path Path to use for lock file.
     * @param leaseDuration Lease expiration timeout.
     * @param sleep Duartion to wait in mili.
     * @param timeout Timeout in mili.
     * @throws Exception Throws exception.
     */
    public FileSystemBasedLock(Path path, long leaseDuration, long sleep, long timeout) throws Exception {
        long startTime = new Date().getTime();
        this.path = path;
        boolean done = false;
        while (!done) {
            try {
                Files.createFile(path, PosixFilePermissions.asFileAttribute(FileSystemPermissions.READ_WRITE_PERMISSION));
                return;
            } catch (FileAlreadyExistsException ex) {
                try {
                    BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
                    if (hasExpired(attributes.creationTime().toMillis(), leaseDuration)) {
                        Files.deleteIfExists(path);
                    }
                } catch (IOException e) {
                    // The file may be deleted by the time we try to read attributes on it.
                    if (!(e instanceof NoSuchFileException)) {
                        throw e;
                    }
                }
            }
            if (hasExpired(startTime, timeout)) {
                throw new TimeoutException();
            }
            Thread.sleep(sleep);
        }
    }

    /**
     * Determines whether given time interval has expired.
     * @param startTime Start of interval.
     * @param duration Duration of interval.
     * @return
     */
    private boolean hasExpired(long startTime, long duration) {
        return (new Date().getTime() - startTime) > duration;
    }

    /**
     * Closes the lock.
     * @throws Exception
     */

    @Override
    public void close() throws Exception {
        Files.deleteIfExists(path);
    }
}
