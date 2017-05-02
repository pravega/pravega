/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.store.stream;

public enum TxnStatus {
    UNKNOWN,
    OPEN,
    COMMITTING,
    COMMITTED,
    ABORTING,
    ABORTED
}
