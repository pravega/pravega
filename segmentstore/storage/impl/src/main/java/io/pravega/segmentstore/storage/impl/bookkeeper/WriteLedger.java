/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.bookkeeper.client.api.WriteHandle;

/**
 * LedgerHandle-LedgerMetadata pair.
 */
@RequiredArgsConstructor
class WriteLedger {
    final WriteHandle ledger;
    final LedgerMetadata metadata;

    /**
     * Whether this Ledger has been closed in a controlled way and rolled over into a new ledger. This value is not
     * serialized and hence it should only be relied upon on active (write) ledgers, and not on recovered ledgers.
     */
    @Getter
    @Setter
    private boolean rolledOver;

    @Override
    public String toString() {
        return String.format("%s, Length = %d, Closed = %s", this.metadata, this.ledger.getLength(), this.ledger.isClosed());
    }
}
