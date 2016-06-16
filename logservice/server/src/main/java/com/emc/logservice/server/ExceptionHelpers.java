package com.emc.logservice.server;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Helper methods with respect to Exceptions.
 */
public class ExceptionHelpers {

    /**
     * Extracts the inner exception from any Exception that may have an inner exception.
     *
     * @param ex
     * @return
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
