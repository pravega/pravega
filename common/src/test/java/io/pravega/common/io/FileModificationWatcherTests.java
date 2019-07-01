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

import org.junit.Test;

import java.io.FileNotFoundException;

import static io.pravega.test.common.AssertExtensions.assertThrows;

public class FileModificationWatcherTests {

    private final static String PATH_EMPTY = "";
    private final static String PATH_NONEMPTY = "non-empty";
    private final static String PATH_NONEXISTENT = System.currentTimeMillis() + ".file";

    @Test
    public void testCtorRejectsNullInput() {
        assertThrows("Null fileToWatch argument wasn't rejected.",
                () -> new FileModificationWatcher(null, c -> System.out.println(c) ),
                e -> e instanceof NullPointerException);

        assertThrows("Null callback argument wasn't rejected.",
                () -> new FileModificationWatcher(PATH_NONEMPTY, null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void testCtorRejectsEmptyFileToWatchArgument() {
        assertThrows("Empty fileToWatch argument wasn't rejected.",
                () -> new FileModificationWatcher(PATH_EMPTY, c -> System.out.println(c) ),
                e -> e instanceof FileNotFoundException);
    }

    @Test
    public void testCtorRejectsNonExistentFileToWatchArgument() {
        assertThrows("Empty fileToWatch argument wasn't rejected.",
                () -> new FileModificationWatcher(PATH_NONEXISTENT, c -> System.out.println(c) ),
                e -> e instanceof FileNotFoundException);
    }
}
