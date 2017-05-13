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

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Helper methods with respect to Exceptions.
 */
public class ExceptionHelpers {

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
     * Extracts the inner exception from any Exception that may have an inner exception.
     *
     * @param ex The exception to query.
     * @return Throwable corresponding to the inner exception.
     */
    public static Throwable getRealException(Throwable ex) {
        if (canInspectCause(ex)) {
            Throwable cause = ex.getCause();
            if (cause != null) {
                return getRealException(cause);
            }
        }

        return ex;
    }

    /**
     * Returns true if the provided class is CompletionException or ExecutionException which need to be unwrapped.
     * @param c The class to be tested
     * @return True if {@link #getRealException(Throwable)} should be called on exceptions of this type
     */
    public static boolean shouldUnwrap(Class<? extends Exception> c) {
        return c.equals(CompletionException.class) || c.equals(ExecutionException.class);
    }
    
    /**
     * If the provided exception is a CompletionException or ExecutionException which need be unwraped.
     * @param e The exception to be unwrapped.
     * @return The cause or the exception provided.
     */
    public static Throwable unwrapIfRequired(Throwable e) {
        if ((e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null) {
            return e.getCause();
        }
        return e;
    }
    
    private static boolean canInspectCause(Throwable ex) {
        return ex instanceof CompletionException
                || ex instanceof ExecutionException
                || ex instanceof IOException;
    }
}
