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
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

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
}
