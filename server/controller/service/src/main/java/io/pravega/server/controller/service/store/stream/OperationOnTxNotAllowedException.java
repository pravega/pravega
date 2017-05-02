/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.store.stream;

/**
 * Exception thrown when a tx is in incorrect state.
 */
public class OperationOnTxNotAllowedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "%s operation not allowed on Transaction %s.";

    /**
     * Creates a new instance of OperationOnTxNotAllowedException class.
     * @param op operation
     * @param txid transaction id
     */
    public OperationOnTxNotAllowedException(final String txid, final String op) {
        super(String.format(FORMAT_STRING, op, txid));
    }
}
