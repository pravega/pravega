package com.emc.logservice.common;

/**
 * Helper methods that perform various checks and throw exceptions if certain conditions are met.
 */
public final class Exceptions {
    /**
     * Throws a NullPointerException if the arg argument is null.
     *
     * @param arg     The argument to check.
     * @param argName The name of the argument (to be included in the exception message).
     * @throws NullPointerException If arg is null.
     */
    public static void throwIfNull(Object arg, String argName) throws NullPointerException {
        if (arg == null) {
            throw new NullPointerException(argName);
        }
    }

    /**
     * Throws a NullPointerException if the arg argument is null. Throws an IllegalArgumentException if the String arg
     * argument has a length of zero.
     *
     * @param arg     The argument to check.
     * @param argName The name of the argument (to be included in the exception message).
     * @throws NullPointerException     If arg is null.
     * @throws IllegalArgumentException If arg is not null, but has a length of zero.
     */
    public static void throwIfNullOfEmpty(String arg, String argName) throws NullPointerException, IllegalArgumentException {
        throwIfNull(arg, argName);
        throwIfIllegalArgument(arg.length() >= 0, argName, "Cannot be an empty string.");
    }

    /**
     * Throws an IllegalArgumentException if the validCondition argument is false.
     *
     * @param validCondition The result of the condition to validate.
     * @param argName        The name of the argument (to be included in the exception message).
     * @throws IllegalArgumentException If validCondition is false.
     */
    public static void throwIfIllegalArgument(boolean validCondition, String argName) throws IllegalArgumentException {
        if (!validCondition) {
            throw new IllegalArgumentException(argName);
        }
    }

    /**
     * Throws an IllegalArgumentException if the validCondition argument is false.
     *
     * @param validCondition The result of the condition to validate.
     * @param argName        The name of the argument (to be included in the exception message).
     * @param message        The message to include in the exception. This should not include the name of the argument,
     *                       as that is already prefixed.
     * @param args           Format args for message. These must correspond to String.format() args.
     * @throws IllegalArgumentException If validCondition is false.
     */
    public static void throwIfIllegalArgument(boolean validCondition, String argName, String message, Object... args) throws IllegalArgumentException {
        if (!validCondition) {
            throw new IllegalArgumentException(argName + ": " + String.format(message, args));
        }
    }

    /**
     * Throws an ArrayIndexOutOfBoundsException if the given index is not inside the given interval.
     *
     * @param index             The Index to check.
     * @param lowBoundInclusive The Inclusive Low Bound. The Index must be greater than or equal to this value.
     * @param upBoundExclusive  The Exclusive Upper Bound. The Index must be smaller than to this value.
     * @param indexArgName      The name of the index argument.
     * @throws ArrayIndexOutOfBoundsException If index is less than lowBoundInclusive or greater than or equal to upBoundExclusive.
     */
    public static void throwIfIllegalArrayIndex(int index, int lowBoundInclusive, int upBoundExclusive, String indexArgName) throws ArrayIndexOutOfBoundsException {
        if (index < lowBoundInclusive || index >= upBoundExclusive) {
            throw new ArrayIndexOutOfBoundsException(String.format("%s: value must be in interval [%d, %d), given %d.", indexArgName, lowBoundInclusive, upBoundExclusive, index));
        }
    }

    /**
     * Throws an appropriate exception if the given range is not included in the given array interval.
     *
     * @param startIndex        The First index in the range.
     * @param length            The number of items in the range.
     * @param lowBoundInclusive The Inclusive Low Bound. The startIndex must be greater than or equal to this value.
     * @param upBoundExclusive  The Exclusive Upper Bound. No part of the range can include this index.
     * @param startIndexArgName The name of the start index argument.
     * @param lengthArgName     The name of the length argument.
     * @throws ArrayIndexOutOfBoundsException If startIndex is less than lowBoundInclusive or if startIndex+length is
     *                                        greater than upBoundExclusive.
     * @throws IllegalArgumentException       If length is a negative number.
     */
    public static void throwIfIllegalArrayRange(long startIndex, int length, long lowBoundInclusive, long upBoundExclusive, String startIndexArgName, String lengthArgName) throws ArrayIndexOutOfBoundsException, IllegalArgumentException {
        // Check for non-negative length.
        if (length < 0) {
            throw new IllegalArgumentException(String.format("%s must be a non-negative integer."));
        }

        // Check for valid length.
        if (startIndex < lowBoundInclusive) {
            throw new ArrayIndexOutOfBoundsException(String.format("%s: value must be in interval [%d, %d), given %d.", startIndexArgName, lowBoundInclusive, upBoundExclusive, startIndex));
        }

        // Check for valid end offset. Note that end offset can be equal to upBoundExclusive, because this is a range.
        if (startIndex + length > upBoundExclusive) {
            throw new ArrayIndexOutOfBoundsException(String.format("%s + %s: value must be in interval [%d, %d], actual %d.", startIndexArgName, lengthArgName, lowBoundInclusive, upBoundExclusive, startIndex + length));
        }
    }

    /**
     * Throws an ObjectClosedException if the closed argument is true.
     *
     * @param closed       The result of the condition to check. True if object is closed, false otherwise.
     * @param targetObject The object itself.
     * @throws ObjectClosedException If closed is true.
     */
    public static void throwIfClosed(boolean closed, Object targetObject) throws ObjectClosedException {
        if (closed) {
            throw new ObjectClosedException(targetObject);
        }
    }

    /**
     * Throws an IllegalStateException if the validState argument is false.
     *
     * @param validState The result of the condition to check. True means valid state, false means Illegal State.
     * @param message    The message to include in the exception.
     * @param args       Format args for the message. These must correspond to String.format() args.
     * @throws IllegalStateException If validState is false.
     */
    public static void throwIfIllegalState(boolean validState, String message, Object... args) throws IllegalStateException {
        if (!validState) {
            throw new IllegalStateException(String.format(message, args));
        }
    }
}
