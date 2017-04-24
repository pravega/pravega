/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common;

import io.pravega.testcommon.AssertExtensions;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

/**
 * Tests the functionality of methods within the Exceptions class.
 */
public class ExceptionsTests {
    /**
     * Tests the checkNotNullOrEmpty method.
     */
    @Test
    public void testCheckNotNullOrEmpty() {
        AssertExtensions.assertThrows("Unexpected behavior for checkNotNullOrEmpty with null argument.",
                                      () -> Exceptions.checkNotNullOrEmpty((String) null, "null-arg"),
                                      ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows("Unexpected behavior for checkNotNullOrEmpty with null argument.",
                                      () -> Exceptions.checkNotNullOrEmpty((List<String>) null, "null-arg"),
                                      ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Unexpected behavior for checkNotNullOrEmpty with empty string argument.",
                                      () -> Exceptions.checkNotNullOrEmpty("", "empty-arg"),
                                      ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("Unexpected behavior for checkNotNullOrEmpty with empty string argument.",
                                      () -> Exceptions.checkNotNullOrEmpty(Collections.emptyList(), "empty-arg"),
                                      ex -> ex instanceof IllegalArgumentException);

        // This should not throw.
        Exceptions.checkNotNullOrEmpty("a", "valid-arg");
    }

    /**
     * Tests the checkArgument method.
     */
    @Test
    public void testCheckArgument() {
        AssertExtensions.assertThrows(
                "Unexpected behavior for checkArgument(arg, msg) with valid=false argument.",
                () -> Exceptions.checkArgument(false, "invalid-arg", "format msg %s", "foo"),
                ex -> ex instanceof IllegalArgumentException);

        // These should not throw.
        Exceptions.checkArgument(true, "valid-arg", "format msg %s", "foo");
    }

    /**
     * Tests the checkArrayRange method.
     */
    @Test
    public void testCheckArrayRange() {
        // Run a range of fixed size over an interval and verify conditions.

        int maxBound = 20;
        int length = 5;
        for (int i = -1; i <= maxBound + 1; i++) {
            boolean valid = i >= 0 && i + length <= maxBound;
            if (valid) {
                Exceptions.checkArrayRange(i, length, maxBound, "start", "length");
            } else {
                final int index = i;
                AssertExtensions.assertThrows(
                        String.format("Unexpected behavior for checkArrayRange(index = %d, length = %d, maxbound = %d).", index, length, maxBound),
                        () -> Exceptions.checkArrayRange(index, length, maxBound, "start", "length"),
                        ex -> ex instanceof ArrayIndexOutOfBoundsException);
            }
        }

        // Negative length.
        AssertExtensions.assertThrows(
                "Unexpected behavior for checkArrayRange() with negative length.",
                () -> Exceptions.checkArrayRange(10, -1, 20, "start", "length"),
                ex -> ex instanceof IllegalArgumentException);

        // Empty array with empty range (this is a valid case).
        Exceptions.checkArrayRange(0, 0, 0, "start", "length");

        // Empty array with non-empty range (not a valid case).
        AssertExtensions.assertThrows(
                "Unexpected behavior for checkArrayRange() with non-empty range in an empty array.",
                () -> Exceptions.checkArrayRange(0, 1, 0, "start", "length"),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);
    }

    /**
     * Tests the checkNotClosed method.
     */
    @Test
    public void testCheckNotClosed() {
        AssertExtensions.assertThrows(
                "Unexpected behavior for checkNotClosed() with closed=true argument.",
                () -> Exceptions.checkNotClosed(true, "object"),
                ex -> ex instanceof ObjectClosedException);

        // These should not throw.
        Exceptions.checkNotClosed(false, "object");
    }
}
