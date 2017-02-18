/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.RetryableException;

/**
 * Exception thrown when scale has blocked creation of new transactions.
 */
public class TransactionBlockedException extends RetryableException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of TransactionBlockedException class.
     *
     * @param value value
     */
    public TransactionBlockedException(final String value) {
        super(value);
    }
}
