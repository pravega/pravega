package com.emc.logservice.server.core;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * Unit tests for IteratorToEnumeration class.
 */
public class IteratorToEnumerationTests {
    /**
     * Tests basic functionality of the IteratorToEnumeration class.
     */
    @Test
    public void testFunctionality() {
        final int ItemCount = 100;
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < ItemCount; i++) {
            list.add(i);
        }

        IteratorToEnumeration<Integer> enumeration = new IteratorToEnumeration<>(list.iterator());

        for (int i = 0; i < ItemCount; i++) {
            Assert.assertTrue("unexpected value for hasMoreElements.", enumeration.hasMoreElements());
            Assert.assertEquals("unexpected next value for nextElement.", i, (int) enumeration.nextElement());
        }

        Assert.assertFalse("hasMoreElements indicates it has more elements when none should exist.", enumeration.hasMoreElements());
        try {
            enumeration.nextElement();
            Assert.fail("nextElement did not throw when hasMoreElements returned false");
        }
        catch (NoSuchElementException ex) {
            // OK.
        }
    }
}
