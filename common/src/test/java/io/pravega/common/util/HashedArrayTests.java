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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the HashedArray class.
 */
public class HashedArrayTests {
    private static final int COUNT = 1000;
    private static final int MAX_LENGTH = 100;

    /**
     * Tests equals() and hashCode().
     */
    @Test
    public void testEqualsHashCode() {
        val data1 = generate();
        val data2 = copy(data1);
        HashedArray prev = null;
        for (int i = 0; i < data1.size(); i++) {
            val a1 = new HashedArray(data1.get(i));
            val a2 = new HashedArray(data2.get(i));
            Assert.assertEquals("Expecting hashCode() to be the same for the same array contents.", a1.hashCode(), a2.hashCode());
            Assert.assertTrue("Expecting equals() to return true for the same array contents.", a1.equals(a2) && a2.equals(a1));
            if (prev != null) {
                Assert.assertNotEquals("Expecting hashCode() to be different for different arrays.", prev.hashCode(), a1.hashCode());
                Assert.assertFalse("Expecting equals() to return false for different array contents.", prev.equals(a1) || a1.equals(prev));
            }
            prev = a1;
        }
    }

    private List<byte[]> copy(List<byte[]> source) {
        return source.stream().map(a -> Arrays.copyOf(a, a.length)).collect(Collectors.toList());
    }

    private List<byte[]> generate() {
        val rnd = new Random(0);
        val result = new ArrayList<byte[]>();
        result.add(new byte[0]); // Throw in an empty one too.
        int lastLength = 0;
        for (int i = 0; i < COUNT; i++) {
            int length = i % 2 == 0 ? lastLength : rnd.nextInt(MAX_LENGTH);
            byte[] array = new byte[length];
            rnd.nextBytes(array);
            lastLength = length;
        }

        return result;
    }
}
