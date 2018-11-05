/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

/**
 * Transaction data including its version number.
 */
@Data
@AllArgsConstructor
public class VersionedTransactionData {
    public static final VersionedTransactionData EMPTY = new VersionedTransactionData(Integer.MIN_VALUE, new UUID(0, 0), null,
            TxnStatus.UNKNOWN, Long.MIN_VALUE, Long.MIN_VALUE);

    private final int epoch;
    private final UUID id;
    private final Version version;
    private final TxnStatus status;
    private final long creationTime;
    private final long maxExecutionExpiryTime;
}
