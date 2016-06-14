package com.emc.logservice.common;

import com.emc.nautilus.testcommon.AssertExtensions;
import org.junit.Test;

/**
 * Tests the functionality of methods within the Exceptions class.
 */
public class ExceptionsTests {
    /**
     * Tests the throwIfNull method.
     */
    @Test
    public void testThrowIfNull() {
        AssertExtensions.assertThrows(
                "Unexpected behavior for throwIfNull with null argument.",
                () -> Exceptions.throwIfNull(null, "null-arg"),
                ex -> ex instanceof NullPointerException);

        // This should not throw.
        Exceptions.throwIfNull(new Object(), "non-null-arg");
    }

    /**
     * Tests the throwIfNullOrEmpty method.
     */
    @Test
    public void testThrowIfNullOrEmpty() {
        AssertExtensions.assertThrows(
                "Unexpected behavior for throwIfNullOfEmpty with null argument.",
                () -> Exceptions.throwIfNullOfEmpty(null, "null-arg"),
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows(
                "Unexpected behavior for throwIfNullOfEmpty with empty string argument.",
                () -> Exceptions.throwIfNullOfEmpty("", "empty-arg"),
                ex -> ex instanceof IllegalArgumentException);

        // This should not throw.
        Exceptions.throwIfNullOfEmpty("a", "valid-arg");
    }

    /**
     * Tests the throwIfIllegalArgument method.
     */
    @Test
    public void testThrowIfIllegalArgument() {
        AssertExtensions.assertThrows(
                "Unexpected behavior for throwIfIllegalArgument(arg) with valid=false argument.",
                () -> Exceptions.throwIfIllegalArgument(false, "invalid-arg"),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "Unexpected behavior for throwIfIllegalArgument(arg, msg) with valid=false argument.",
                () -> Exceptions.throwIfIllegalArgument(false, "invalid-arg", "format msg %s", "foo"),
                ex -> ex instanceof IllegalArgumentException);

        // These should not throw.
        Exceptions.throwIfIllegalArgument(true, "valid-arg");
        Exceptions.throwIfIllegalArgument(true, "valid-arg", "format msg %s", "foo");
    }

    /**
     * Tests the throwIfIllegalArrayIndex method.
     */
    @Test
    public void testThrowIfIllegalArrayIndex() {
        int minBound = 5;
        int maxBound = 10;
        for (int i = minBound - 1; i <= maxBound + 1; i++) {
            boolean valid = i >= minBound && i < maxBound;
            if (valid) {
                // This should not throw.
                Exceptions.throwIfIllegalArrayIndex(i, minBound, maxBound, "i");
            }
            else {
                final int index = i;
                AssertExtensions.assertThrows(
                        String.format("Unexpected behavior for throwIfIllegalArrayIndex(index = %d, minbound = %d, maxbound = %d).", index, minBound, maxBound),
                        () -> Exceptions.throwIfIllegalArrayIndex(index, minBound, maxBound, "i"),
                        ex -> ex instanceof ArrayIndexOutOfBoundsException);
            }
        }

        // Check for invalid bounds.
        AssertExtensions.assertThrows(
                "Unexpected behavior for throwIfIllegalArrayIndex() with invalid bounds.",
                () -> Exceptions.throwIfIllegalArrayIndex(10, 10, 10, "i"),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);
    }

    /**
     * Tests the throwIfIllegalArrayRange method.
     */
    @Test
    public void testThrowIfIllegalArrayRange() {
        // Run a range of fixed size over an interval and verify conditions.

        int minBound = 10;
        int maxBound = 20;
        int length = 5;
        for (int i = minBound - length - 1; i <= maxBound + 1; i++) {
            boolean valid = i >= minBound && i + length <= maxBound;
            if (valid) {
                Exceptions.throwIfIllegalArrayRange(i, length, minBound, maxBound, "start", "length");
            }
            else {
                final int index = i;
                AssertExtensions.assertThrows(
                        String.format("Unexpected behavior for throwIfIllegalArrayRange(index = %d, length = %d, minbound = %d, maxbound = %d).", index, length, minBound, maxBound),
                        () -> Exceptions.throwIfIllegalArrayRange(index, length, minBound, maxBound, "start", "length"),
                        ex -> ex instanceof ArrayIndexOutOfBoundsException);
            }
        }

        // Negative length.
        AssertExtensions.assertThrows(
                "Unexpected behavior for throwIfIllegalArrayRange() with negative length.",
                () -> Exceptions.throwIfIllegalArrayRange(10, -1, 8, 20, "start", "length"),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests the throwIfClosed method.
     */
    @Test
    public void testThrowIfClosed() {
        AssertExtensions.assertThrows(
                "Unexpected behavior for throwIfClosed() with closed=true argument.",
                () -> Exceptions.throwIfClosed(true, "object"),
                ex -> ex instanceof ObjectClosedException);

        // These should not throw.
        Exceptions.throwIfClosed(false, "object");
    }

    /**
     * Tests the throwIfIllegalState method.
     */
    @Test
    public void testThrowIfIllegalState() {
        AssertExtensions.assertThrows(
                "Unexpected behavior for throwIfIllegalState() with validState=false argument.",
                () -> Exceptions.throwIfIllegalState(false, "format msg %s", "foo"),
                ex -> ex instanceof IllegalStateException);

        // These should not throw.
        Exceptions.throwIfIllegalState(true, "format msg %s", "foo");
    }
}
