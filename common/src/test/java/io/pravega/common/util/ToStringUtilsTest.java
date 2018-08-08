/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import io.pravega.test.common.AssertExtensions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import static io.pravega.common.util.ToStringUtils.compressToBase64;
import static io.pravega.common.util.ToStringUtils.decompressFromBase64;
import static org.junit.Assert.assertEquals;

public class ToStringUtilsTest {

    
    @Test
    public void testRoundTrip() {
        HashMap<String, Integer> m = new HashMap<>();
        m.put("a", 1);
        m.put("b", 2);
        m.put("c", 3);
        String string = ToStringUtils.mapToString(m);
        Map<String, Integer> m2 = ToStringUtils.stringToMap(string, s -> s, Integer::parseInt);
        assertEquals("String did not round trip: " + string, m, m2);
    }

    @Test
    public void testBadKeys() {
        HashMap<String, Integer> m = new HashMap<>();
        m.put("a,b\"", 1);
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> ToStringUtils.mapToString(m));
        m.clear();
        m.put("b, c", 2);
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> ToStringUtils.mapToString(m));
        m.clear();
        m.put("c  =4", 3);
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> ToStringUtils.mapToString(m));
    }

    @Test
    public void testCompressBase64() throws IOException {
        //generate a random string.
        byte[] array = new byte[10];
        new Random().nextBytes(array);
        String generatedString = new String(array, StandardCharsets.UTF_8);

        String compressedString = compressToBase64(generatedString);
        assertEquals(generatedString, decompressFromBase64(compressedString));
    }

    @Test
    public void testCompressInvalidInput() {
        AssertExtensions.assertThrows(NullPointerException.class, () -> compressToBase64(null));
        AssertExtensions.assertThrows(NullPointerException.class, () -> decompressFromBase64(null));
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> decompressFromBase64(""));
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> decompressFromBase64("Invalid base64 String"));
        AssertExtensions.assertThrows(IOException.class, () -> decompressFromBase64("H4sIAAAAAAAAAFvz"));
    }
}
