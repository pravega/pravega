/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.util;

/**
 * Exception thrown by {@link Retry} utility class when all of the configured number of attempts have failed.
 * The cause for this exception will be set to the final failure.
 */
public class RetriesExhaustedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RetriesExhaustedException(Throwable last) {
        super(last);
    }
}
