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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;

public abstract class FileModificationMonitorTests {

    final static Path PATH_VALID_NONEXISTENT = Paths.get(File.pathSeparator +
            System.currentTimeMillis() + File.pathSeparator + System.currentTimeMillis());

    private final static Path PATH_EMPTY = Paths.get("");
    private final static Path PATH_NONEMPTY = Paths.get("non-empty");
    private final static Path PATH_NONEXISTENT = Paths.get(System.currentTimeMillis() + ".file");

    abstract FileModificationMonitor prepareObjectUnderTest(Path path) throws FileNotFoundException;

    abstract FileModificationMonitor prepareObjectUnderTest(Path path, boolean checkForFileExistence)
            throws FileNotFoundException;

    @Test
    public void testCtorRejectsNullInput() {
        assertThrows("Null fileToWatch argument wasn't rejected.",
                () -> prepareObjectUnderTest(null),
                e -> e instanceof NullPointerException);

        assertThrows("Null callback argument wasn't rejected.",
                () -> new FileModificationEventWatcher(PATH_NONEMPTY, null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void testCtorRejectsEmptyFileArgument() {
        assertThrows("Empty fileToWatch argument wasn't rejected.",
                () -> prepareObjectUnderTest(PATH_EMPTY),
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void testCtorRejectsNonExistentFileArgument() {
        assertThrows("Empty fileToWatch argument wasn't rejected.",
                () -> prepareObjectUnderTest(PATH_NONEXISTENT, true),
                e -> e instanceof FileNotFoundException);
    }

    @Test
    public void testStopWithNoStartCompletesGracefully() throws IOException {
        FileModificationMonitor monitor = prepareObjectUnderTest(PATH_VALID_NONEXISTENT, false);
        monitor.stopMonitoring();
    }

    static File createTempFile() throws IOException {
        Path dir = Files.createTempDirectory("fw-");
        return File.createTempFile("tf-", ".temp", dir.toFile());
    }

    static void cleanupTempFile(File file) throws IOException {
        Preconditions.checkNotNull(file);
        if (file.toPath() == null) {
            return;
        }
        Path dirPath = file.toPath().getParent();

        if (file.exists()) {
            file.delete();
        }
        if (dirPath != null) {
            FileUtils.deleteDirectory(dirPath.toFile());
        }
    }
}
