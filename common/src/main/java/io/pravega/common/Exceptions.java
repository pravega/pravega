/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common;

import com.google.common.base.Preconditions;
import java.util.Collection;
<<<<<<< HEAD
import java.util.Map;
=======

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
>>>>>>> refs/heads/master
import lombok.SneakyThrows;

/**
 * Helper methods that perform various checks and throw exceptions if certain conditions are met.
 */
public final class Exceptions {

    /**
     * Determines if the given Throwable represents a fatal exception and cannot be handled.
     *
     * @param ex The Throwable to inspect.
     * @return True if a fatal error which must be rethrown, false otherwise (it can be handled in a catch block).
     */
    public static boolean mustRethrow(Throwable ex) {
        return ex instanceof VirtualMachineError;
    }

    /**
     * If the provided exception is a CompletionException or ExecutionException which need be unwrapped.
     *
     * @param ex The exception to be unwrapped.
     * @return The cause or the exception provided.
     */
    public static Throwable unwrap(Throwable ex) {
        if (canInspectCause(ex)) {
            Throwable cause = ex.getCause();
            if (cause != null) {
                return unwrap(cause);
            }
        }

        return ex;
    }

    /**
     * Returns true if the provided class is CompletionException or ExecutionException which need to be unwrapped.
     * @param c The class to be tested
     * @return True if {@link #unwrap(Throwable)} should be called on exceptions of this type
     */
    public static boolean shouldUnwrap(Class<? extends Exception> c) {
        return c.equals(CompletionException.class) || c.equals(ExecutionException.class);
    }

    private static boolean canInspectCause(Throwable ex) {
        return ex instanceof CompletionException
                || ex instanceof ExecutionException;
    }

    @FunctionalInterface
    public interface InterruptibleRun<ExceptionT extends Exception> {
        void run() throws InterruptedException, ExceptionT;
    }

    @FunctionalInterface
    public interface InterruptibleCall<ExceptionT extends Exception, ResultT> {
        ResultT call() throws InterruptedException, ExceptionT;
    }

    /**
     * Eliminates boilerplate code of catching and re-interrupting the thread.
     * <p>
     * NOTE: This method currently has the limitation that it can only handle functions that throw up to one additional
     * exception besides {@link InterruptedException}. This is a limitation of the Compiler.
     *
     * @param run          A method that should be run handling interrupts automatically
     * @param <ExceptionT> The type of exception.
     * @throws ExceptionT If thrown by run.
     */
    @SneakyThrows(InterruptedException.class)
    public static <ExceptionT extends Exception> void handleInterrupted(InterruptibleRun<ExceptionT> run)
            throws ExceptionT {
        try {
            run.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    /**
     * Eliminates boilerplate code of catching and re-interrupting the thread.
     * <p>
     * NOTE: This method currently has the limitation that it can only handle functions that throw up to one additional
     * exception besides {@link InterruptedException}. This is a limitation of the Compiler.
     *
     * @param call         A method that should be run handling interrupts automatically
     * @param <ExceptionT> The type of exception.
     * @param <ResultT>    The type of the result.
     * @throws ExceptionT If thrown by call.
     * @return The result of the call.
     */
    @SneakyThrows(InterruptedException.class)
    public static <ExceptionT extends Exception, ResultT> ResultT handleInterrupted(InterruptibleCall<ExceptionT, ResultT> call)
            throws ExceptionT {
        try {
            return call.call();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    /**
     * Throws a NullPointerException if the arg argument is null. Throws an IllegalArgumentException if the String arg
     * argument has a length of zero.
     *
     * @param arg     The argument to check.
     * @param argName The name of the argument (to be included in the exception message).
     * @return The arg.
     * @throws NullPointerException     If arg is null.
     * @throws IllegalArgumentException If arg is not null, but has a length of zero.
     */
    public static String checkNotNullOrEmpty(String arg, String argName) throws NullPointerException, IllegalArgumentException {
        Preconditions.checkNotNull(arg, argName);
        checkArgument(arg.length() > 0, argName, "Cannot be an empty string.");
        return arg;
    }
    
    /**
     * Throws a NullPointerException if the arg argument is null. Throws an IllegalArgumentException if the Collections arg
     * argument has a size of zero.
     *
     * @param <T>     The type of elements in the provided collection.
     * @param <V>     The actual type of the collection.
     * @param arg     The argument to check.
     * @param argName The name of the argument (to be included in the exception message).
     * @return The arg.
     * @throws NullPointerException     If arg is null.
     * @throws IllegalArgumentException If arg is not null, but has a length of zero.
     */
    public static <T, V extends Collection<T>> V checkNotNullOrEmpty(V arg, String argName) throws NullPointerException, IllegalArgumentException {
        Preconditions.checkNotNull(arg, argName);
        checkArgument(!arg.isEmpty(), argName, "Cannot be an empty collection.");
        return arg;
    }
    
    /**
     * Throws a NullPointerException if the arg argument is null. Throws an IllegalArgumentException if the Map arg
     * argument has a size of zero.
     *
     * @param <T>     The type of elements in the provided map.
     * @param arg     The argument to check.
     * @param argName The name of the argument (to be included in the exception message).
     * @return The arg.
     * @throws NullPointerException     If arg is null.
     * @throws IllegalArgumentException If arg is not null, but has a length of zero.
     */
    public static <K,V> Map<K,V> checkNotNullOrEmpty(Map<K,V> arg, String argName) throws NullPointerException, IllegalArgumentException {
        Preconditions.checkNotNull(arg, argName);
        checkArgument(!arg.isEmpty(), argName, "Cannot be an empty map.");
        return arg;
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
    public static void checkArgument(boolean validCondition, String argName, String message, Object... args) throws IllegalArgumentException {
        if (!validCondition) {
            throw new IllegalArgumentException(badArgumentMessage(argName, message, args));
        }
    }

    /**
     * Throws an appropriate exception if the given range is not included in the given array interval.
     *
     * @param startIndex        The First index in the range.
     * @param length            The number of items in the range.
     * @param arrayLength       The length of the array.
     * @param startIndexArgName The name of the start index argument.
     * @param lengthArgName     The name of the length argument.
     * @throws ArrayIndexOutOfBoundsException If startIndex is less than lowBoundInclusive or if startIndex+length is
     *                                        greater than upBoundExclusive.
     * @throws IllegalArgumentException       If length is a negative number.
     */
    public static void checkArrayRange(long startIndex, int length, long arrayLength, String startIndexArgName, String lengthArgName) throws ArrayIndexOutOfBoundsException, IllegalArgumentException {
        // Check for non-negative length.
        if (length < 0) {
            throw new IllegalArgumentException(badArgumentMessage(lengthArgName, "length must be a non-negative integer."));
        }

        // Check for valid start index.
        if (startIndex < 0 || startIndex >= arrayLength) {
            // The only valid case here is if the range has zero elements and the array bounds also has zero elements.
            if (!(startIndex == 0 && length == 0 && arrayLength == 0)) {
                throw new ArrayIndexOutOfBoundsException(badStartOffsetMessage(startIndex, arrayLength, startIndexArgName));
            }
        }

        // Check for valid end offset. Note that end offset can be equal to upBoundExclusive, because this is a range.
        if (startIndex + length > arrayLength) {
            throw new ArrayIndexOutOfBoundsException(badLengthMessage(startIndex, length, arrayLength, startIndexArgName, lengthArgName));
        }
    }

    /**
     * Throws an ObjectClosedException if the closed argument is true.
     *
     * @param closed       The result of the condition to check. True if object is closed, false otherwise.
     * @param targetObject The object itself.
     * @throws ObjectClosedException If closed is true.
     */
    public static void checkNotClosed(boolean closed, Object targetObject) throws ObjectClosedException {
        if (closed) {
            throw new ObjectClosedException(targetObject);
        }
    }

    private static String badArgumentMessage(String argName, String message, Object... args) {
        return argName + ": " + String.format(message, args);
    }

    private static String badStartOffsetMessage(long startIndex, long arrayLength, String startIndexArgName) {
        return String.format("%s: value must be in interval [0, %d), given %d.", startIndexArgName, arrayLength, startIndex);
    }

    private static String badLengthMessage(long startIndex, int length, long arrayLength, String startIndexArgName, String lengthArgName) {
        return String.format("%s + %s: value must be in interval [0, %d], actual %d.", startIndexArgName, lengthArgName, arrayLength, startIndex + length);
    }
}
