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

import org.junit.Test;

import java.io.FileNotFoundException;

import static io.pravega.test.common.AssertExtensions.assertThrows;

public abstract class FileModificationMonitorTests {

    private final static String PATH_NULL = null;
    private final static String PATH_EMPTY = "";
    private final static String PATH_NONEMPTY = "non-empty";
    private final static String PATH_NONEXISTENT = System.currentTimeMillis() + ".file";

    abstract FileModificationMonitor prepareObjectUnderTest(String path) throws FileNotFoundException;

    @Test
    public void testCtorRejectsNullInput() {
        assertThrows("Null fileToWatch argument wasn't rejected.",
                () -> prepareObjectUnderTest(PATH_NULL),
                e -> e instanceof NullPointerException);

        assertThrows("Null callback argument wasn't rejected.",
                () -> new FileModificationEventWatcher(PATH_NONEMPTY, null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void testCtorRejectsEmptyFileArgument() {
        assertThrows("Empty fileToWatch argument wasn't rejected.",
                () -> prepareObjectUnderTest(PATH_EMPTY),
                e -> e instanceof FileNotFoundException);
    }

    @Test
    public void testCtorRejectsNonExistentFileArgument() {
        assertThrows("Empty fileToWatch argument wasn't rejected.",
                () -> prepareObjectUnderTest(PATH_NONEXISTENT),
                e -> e instanceof FileNotFoundException);
    }
}
