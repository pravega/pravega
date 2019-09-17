/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BooleanUtilsTests {

    @Test
    public void testExtractReturnsEmptyInstanceIfInputIsNullOrEmpty() {
        assertFalse(BooleanUtils.extract(null).isPresent());
        assertFalse(BooleanUtils.extract("").isPresent());
    }

    @Test
    public void testExtractReturnsEmptyInstanceIfInputIsNonBoolean() {
        assertFalse(BooleanUtils.extract("whatever").isPresent());
    }

    @Test
    public void testExtractReturnsTrueIfInputContainsPositiveValue() {
        assertTrue(BooleanUtils.extract("yes").get());
        assertTrue(BooleanUtils.extract("YES").get());
        assertTrue(BooleanUtils.extract("y").get());
        assertTrue(BooleanUtils.extract("true").get());
        assertTrue(BooleanUtils.extract("true ").get());
    }

    @Test
    public void testExtractReturnsTrueIfInputContainsNegativeValue() {
        assertFalse(BooleanUtils.extract("no").get());
        assertFalse(BooleanUtils.extract("nO ").get());
        assertFalse(BooleanUtils.extract("n").get());
        assertFalse(BooleanUtils.extract("false").get());
        assertFalse(BooleanUtils.extract("False").get());
    }
}
