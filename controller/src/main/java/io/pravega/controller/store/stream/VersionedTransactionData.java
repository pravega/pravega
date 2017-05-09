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
    private final UUID id;
    private final int version;
    private final TxnStatus status;
    private final long creationTime;
    private final long maxExecutionExpiryTime;
    private final long scaleGracePeriod;
}
