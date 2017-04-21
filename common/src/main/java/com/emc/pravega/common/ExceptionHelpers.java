/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common;

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
        return ex instanceof OutOfMemoryError
                || ex instanceof StackOverflowError;
    }

    /**
     * Extracts the inner exception from any Exception that may have an inner exception.
     *
     * @param ex The exception to query.
     * @return Exception.
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

    private static boolean canInspectCause(Throwable ex) {
        return ex instanceof CompletionException
                || ex instanceof ExecutionException
                || ex instanceof IOException;
    }
}
