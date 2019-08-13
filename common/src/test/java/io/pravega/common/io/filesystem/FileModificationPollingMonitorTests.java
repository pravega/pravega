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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;

public class FileModificationPollingMonitorTests extends FileModificationMonitorTests {

    private final static Consumer<File> NOOP_CONSUMER = c -> { };

    @Override
    FileModificationMonitor prepareObjectUnderTest(String path) throws FileNotFoundException {
        return new FileModificationPollingMonitor(path, NOOP_CONSUMER);
    }

    @Test(timeout = 2000)
    public void testInvokesCallBackForFileModification() throws IOException, InterruptedException {
        Path dir = Files.createTempDirectory("fw-");
        File file = File.createTempFile("tf-", ".temp", dir.toFile());

        AtomicBoolean isCallbackInvoked = new AtomicBoolean(false);
        FileModificationMonitor watcher = new FileModificationPollingMonitor(file.toPath().toString(),
                c -> isCallbackInvoked.set(true), 100);
        watcher.startMonitoring();

        // Modify the watched file.
        FileUtils.writeStringToFile(file, "hello", StandardCharsets.UTF_8, true);

        // Wait for some time
        Thread.sleep(500);

        assertTrue(isCallbackInvoked.get());

        if (file.exists()) {
            file.delete();
        }
        watcher.stopMonitoring();
    }
}
