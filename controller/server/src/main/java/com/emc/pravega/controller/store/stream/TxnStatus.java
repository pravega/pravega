/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

public enum TxnStatus {
    UNKNOWN,
    OPEN,
    COMMITTING,
    COMMITTED,
    ABORTING,
    ABORTED
}
