/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.filesystem;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class  FileModificationEventWatcherTests extends FileModificationMonitorTests {

    private final static Consumer<WatchEvent<?>> NOOP_CONSUMER = c -> { };

    @Override
    FileModificationMonitor prepareObjectUnderTest(Path path) throws FileNotFoundException {
        return prepareObjectUnderTest(path, true);
    }

    @Override
    FileModificationMonitor prepareObjectUnderTest(Path path, boolean checkForFileExistence) throws FileNotFoundException {
        return new FileModificationEventWatcher(path, NOOP_CONSUMER, true, checkForFileExistence);
    }

    @Test(timeout = 15000)
    public void testInvokesCallBackForFileModification() throws IOException, InterruptedException {
        File tempFile = this.createTempFile();

        AtomicBoolean isCallbackInvoked = new AtomicBoolean(false);
        FileModificationEventWatcher watcher = new FileModificationEventWatcher(tempFile.toPath(),
                c -> isCallbackInvoked.set(true), false, true);
        watcher.startMonitoring();

        // The watcher might not get fully started when we modify the watched file, so ensuring that it is.
        for (int i = 0; i < 3; i++) {
            if (watcher.isWatchRegistered()) {
                break;
            } else {
                Thread.sleep(1000);
            }
        }
        if (!watcher.isWatchRegistered()) {
            throw new RuntimeException("Failed to start the watcher");
        }

        // Modify the watched file.
        FileUtils.writeStringToFile(tempFile, "hello", StandardCharsets.UTF_8, true);
        watcher.join(15 * 1000); // Wait for max 15 seconds for the thread to die.

        assertTrue(isCallbackInvoked.get());

        watcher.stopMonitoring();
        cleanupTempFile(tempFile);
    }

    @Test
    public void testStartAndStopInSequence() throws IOException {
        FileModificationMonitor monitor = this.prepareObjectUnderTest(PATH_VALID_NONEXISTENT, false);
        monitor.startMonitoring();
        monitor.stopMonitoring();
    }

    @Test
    public void testSetsUpWatchDirAndFileNamesCorrectly() throws IOException {
        FileModificationEventWatcher watcher = new FileModificationEventWatcher(SHARED_FILE.toPath(),
                NOOP_CONSUMER);

        assertEquals(SHARED_FILE.toPath().getParent(), watcher.getWatchedDirectory());
        assertEquals(SHARED_FILE.getName(), watcher.getWatchedFileName());
    }
}
