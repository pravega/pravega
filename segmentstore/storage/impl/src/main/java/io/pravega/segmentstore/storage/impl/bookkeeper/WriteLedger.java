/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.client.LedgerHandle;

/**
 * LedgerHandle-LedgerMetadata pair.
 */
@RequiredArgsConstructor
class WriteLedger {
    final LedgerHandle ledger;
    final LedgerMetadata metadata;

    @Override
    public String toString() {
        return String.format("%s, Length = %d, Closed = %s", this.metadata, this.ledger.getLength(), this.ledger.isClosed());
    }
}
