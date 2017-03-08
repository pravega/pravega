/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

/**
 * Transaction data including its version number.
 */
@Data
@AllArgsConstructor
public class VersionedTransactionData {
    private final UUID id;
    private final int version;
    private final TxnStatus status;
    private final long creationTime;
    private final long maxExecutionExpiryTime;
    private final long scaleGracePeriod;
}
