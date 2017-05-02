/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.storage;

import io.pravega.server.segmentstore.contracts.StreamingException;

/**
 * General exception thrown by the Durable Data Log.
 */
public class DurableDataLogException extends StreamingException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the DurableDataLogException class.
     *
     * @param message The message to set.
     */
    public DurableDataLogException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the DurableDataLogException class.
     *
     * @param message The message to set.
     * @param cause   The triggering cause of this exception.
     */
    public DurableDataLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
