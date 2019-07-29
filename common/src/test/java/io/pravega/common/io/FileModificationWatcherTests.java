/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertTrue;

public class FileModificationWatcherTests {

    private final static String PATH_NULL = null;
    private final static String PATH_EMPTY = "";
    private final static String PATH_NONEMPTY = "non-empty";
    private final static String PATH_NONEXISTENT = System.currentTimeMillis() + ".file";

    private final static Consumer<WatchEvent<?>> NOOP_CONSUMER = c -> { };

    @Test
    public void testCtorRejectsNullInput() {
        assertThrows("Null fileToWatch argument wasn't rejected.",
                () -> new FileModificationWatcher(PATH_NULL, NOOP_CONSUMER),
                e -> e instanceof NullPointerException);

        assertThrows("Null callback argument wasn't rejected.",
                () -> new FileModificationWatcher(PATH_NONEMPTY, null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void testCtorRejectsEmptyFileToWatchArgument() {
        assertThrows("Empty fileToWatch argument wasn't rejected.",
                () -> new FileModificationWatcher(PATH_EMPTY, NOOP_CONSUMER),
                e -> e instanceof FileNotFoundException);
    }

    @Test
    public void testCtorRejectsNonExistentFileToWatchArgument() {
        assertThrows("Empty fileToWatch argument wasn't rejected.",
                () -> new FileModificationWatcher(PATH_NONEXISTENT, NOOP_CONSUMER),
                e -> e instanceof FileNotFoundException);
    }

    @Test
    public void testInvokesCallBackForFileModification() throws IOException, InterruptedException {
        Path dir = Files.createTempDirectory("fw-");
        File file = File.createTempFile("tf-", ".temp", dir.toFile());
        Path filePath = file.toPath();

        AtomicBoolean isCallbackInvoked = new AtomicBoolean(false);
        FileModificationWatcher watcher = new FileModificationWatcher(filePath,
                c -> isCallbackInvoked.set(true), false);
        watcher.setDaemon(true);
        watcher.start();

        // Modify the watched file.
        FileUtils.writeStringToFile(file, "hello", StandardCharsets.UTF_8, true);

        watcher.join(2 * 1000); // Wait for max 2 seconds for the thread to die.

        assertTrue(isCallbackInvoked.get());

        if (file.exists()) {
            file.delete();
        }
    }
}
