/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.lang;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RandomStringUtilsTests {

    @Test
    public void testAlphanumeric() {
        String string = RandomStringUtils.randomAlphanumeric(10);
        assertEquals(10, string.length());
        string = RandomStringUtils.randomAlphanumeric(20000);
        assertTrue(string.contains("a"));
        assertTrue(string.contains("z"));
        assertTrue(string.contains("A"));
        assertTrue(string.contains("Z"));
        assertTrue(string.contains("0"));
        assertTrue(string.contains("9"));
        assertFalse(string.contains("@"));
        assertFalse(string.contains("["));
        assertFalse(string.contains("~"));
        assertFalse(string.contains("{"));
        assertFalse(string.contains("/"));
        assertFalse(string.contains(":"));
    }
    
    @Test
    public void testAlphabetic() {
        String string = RandomStringUtils.randomAlphabetic(10);
        assertEquals(10, string.length());
        string = RandomStringUtils.randomAlphabetic(20000);
        assertTrue(string.contains("a"));
        assertTrue(string.contains("z"));
        assertTrue(string.contains("A"));
        assertTrue(string.contains("Z"));
        assertFalse(string.contains("0"));
        assertFalse(string.contains("9"));
        assertFalse(string.contains("@"));
        assertFalse(string.contains("["));
        assertFalse(string.contains("~"));
        assertFalse(string.contains("{"));
        assertFalse(string.contains("/"));
        assertFalse(string.contains(":"));
    }
    
}
