/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.security;

import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;

public class TLSHelperTests {

    private final static String PATH_EMPTY = "";
    private final static String PATH_NONEMPTY = "non-empty";
    private final static String PATH_NONEXISTENT = System.currentTimeMillis() + ".file";

    @Test
    public void testNewServerSslContextFailsWhenInputIsNull() {
        assertThrows("Null pathToCertificateFile argument wasn't rejected.",
                () -> TLSHelper.newServerSslContext(null, PATH_NONEMPTY),
                e -> e instanceof NullPointerException);

        assertThrows("Null pathToServerKeyFile argument wasn't rejected.",
                () -> TLSHelper.newServerSslContext(PATH_NONEMPTY, null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void testNewServerSslContextFailsWhenInputIsEmpty() {
        assertThrows("Empty pathToCertificateFile argument wasn't rejected.",
                () -> TLSHelper.newServerSslContext(PATH_EMPTY, PATH_NONEMPTY),
                e -> e instanceof IllegalArgumentException);

        assertThrows("Empty pathToServerKeyFile argument wasn't rejected.",
                () -> TLSHelper.newServerSslContext(PATH_NONEMPTY, PATH_EMPTY),
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void testNewServerSslContextFailsWhenInputFilesDontExist() {
        assertThrows("Non-existent pathToCertificateFile wasn't rejected.",
                () -> TLSHelper.newServerSslContext(PATH_NONEXISTENT, PATH_NONEMPTY),
                e -> e instanceof IllegalStateException);

        assertThrows("Non-existent pathToServerKeyFile argument wasn't rejected.",
                () -> TLSHelper.newServerSslContext(PATH_NONEMPTY, PATH_NONEXISTENT),
                e -> e instanceof IllegalStateException);
    }

    //public void testNewServerSslContextFailsWhen
}
