package com.emc.logservice.server.logs;

/**
 * Defines a simple callback, that can either complete or fail.
 */
public interface SimpleCallback {

    /**
     * Completes the callback (no exception).
     */
    void complete();

    /**
     * Completes the callback with failure.
     *
     * @param ex The causing exception.
     */

    void fail(Throwable ex);

    /**
     * Gets a value indicating whether this SimpleCallback has finished, regardless of outcome.
     *
     * @return True if finished, false otherwise.
     */
    boolean isDone();
}
