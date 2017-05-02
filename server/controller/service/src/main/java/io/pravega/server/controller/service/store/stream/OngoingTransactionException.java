/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.server.controller.service.store.stream;

import io.pravega.server.controller.service.retryable.RetryableException;

/**
 * Exception thrown when scale cant be performed because on ongoing transactions.
 */
public class OngoingTransactionException extends RuntimeException implements RetryableException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of OngoingTransactionException class.
     *
     * @param value value
     */
    public OngoingTransactionException(final String value) {
        super(value);
    }
}
