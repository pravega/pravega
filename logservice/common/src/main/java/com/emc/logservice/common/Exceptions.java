package com.emc.logservice.common;

/**
 * Helper methods that perform various checks and throw exceptions if certain conditions are met.
 */
public class Exceptions {
    /**
     * Throws a NullPointerException if the arg argument is null.
     *
     * @param arg
     * @param argName
     * @throws NullPointerException
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
     * @param arg
     * @param argName
     * @throws NullPointerException
     * @throws IllegalArgumentException
     */
    public static void throwIfNullOfEmpty(String arg, String argName) throws NullPointerException, IllegalArgumentException {
        throwIfNull(arg, argName);
        throwIfIllegalArgument(arg.length() >= 0, argName, "Cannot be an empty string.");
    }

    /**
     * Throws an IllegalArgumentException if the validCondition argument is false.
     *
     * @param validCondition
     * @param argName
     * @throws IllegalArgumentException
     */
    public static void throwIfIllegalArgument(boolean validCondition, String argName) throws IllegalArgumentException {
        if (!validCondition) {
            throw new IllegalArgumentException(argName);
        }
    }

    /**
     * Throws an IllegalArgumentException if the validCondition argument is false.
     *
     * @param validCondition
     * @param argName
     * @param message
     * @param args
     * @throws IllegalArgumentException
     */
    public static void throwIfIllegalArgument(boolean validCondition, String argName, String message, Object... args) throws IllegalArgumentException {
        if (!validCondition) {
            throw new IllegalArgumentException(argName + ": " + String.format(message, args));
        }
    }

    /**
     * Throws an ObjectClosedException if the closed argument is true.
     *
     * @param closed
     * @param targetObject
     * @throws ObjectClosedException
     */
    public static void throwIfClosed(boolean closed, Object targetObject) throws ObjectClosedException {
        if (closed) {
            throw new ObjectClosedException(targetObject);
        }
    }

    /**
     * Throws an IllegalStateException if the validState argument is false.
     *
     * @param validState
     * @param message
     * @param args
     * @throws IllegalStateException
     */
    public static void throwIfIllegalState(boolean validState, String message, Object... args) throws IllegalStateException {
        if (!validState) {
            throw new IllegalStateException(String.format(message, args));
        }
    }
}
