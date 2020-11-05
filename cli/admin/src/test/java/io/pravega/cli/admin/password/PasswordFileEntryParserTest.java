/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.password;

import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PasswordFileEntryParserTest {

    @Test
    public void parseThrowsExceptionForInvalidInput() {
        AssertExtensions.assertThrows(NullPointerException.class, () -> PasswordFileEntryParser.parse(null));
        AssertExtensions.assertThrows(NullPointerException.class, () -> PasswordFileEntryParser.parse(null, true));
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> PasswordFileEntryParser.parse("username:hashedPassword"));
    }

    @Test
    public void parsesExpectedInput() {
        assertTrue(PasswordFileEntryParser.parse("username:hashedpassword:prn::*,READ_UPDATE").length == 3);
        assertTrue(PasswordFileEntryParser.parse("username:hashedpassword", false).length == 2);
        assertTrue(PasswordFileEntryParser.parse("username:hashedpassword:prn::/,READ_UPDATE;prn::/scope,READ_UPDATE").length == 3);
    }
}
