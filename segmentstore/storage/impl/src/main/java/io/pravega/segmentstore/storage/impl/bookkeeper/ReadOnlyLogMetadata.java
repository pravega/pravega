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

import java.util.List;

/**
 * Defines a read-only view of the BookKeeper Log Metadata.
 */
public interface ReadOnlyLogMetadata {
    /**
     * Gets a value indicating the current epoch of this LogMetadata. This changes upon every successful log initialization,
     * immediately after it was fenced.
     *
     * @return The current epoch.
     */
    long getEpoch();

    /**
     * Gets a value indicating the current version of the Metadata (this changes upon every successful metadata persist).
     * Note: this is different from getEpoch() - which gets incremented with every successful recovery.
     *
     * @return The current version.
     */
    int getUpdateVersion();

    /**
     * Gets a value indicating whether this log is enabled or not.
     *
     * @return True if enabled, false otherwise.
     */
    boolean isEnabled();

    /**
     * Gets a read-only ordered list of LedgerMetadata instances representing the Ledgers that currently make up this
     * Log Metadata.
     *
     * @return A new read-only list.
     */
    List<LedgerMetadata> getLedgers();

    /**
     * Gets a LedgerAddress representing the first location in the log that is accessible for reads.
     *
     * @return The Truncation Address.
     */
    LedgerAddress getTruncationAddress();

    /**
     * Determines whether this {@link ReadOnlyLogMetadata} is equivalent to the other one.
     *
     * @param other The other instance.
     * @return True if equivalent, false otherwise.
     */
    default boolean equals(ReadOnlyLogMetadata other) {
        if (other == null) {
            return false;
        }

        List<LedgerMetadata> ledgers = getLedgers();
        List<LedgerMetadata> otherLedgers = other.getLedgers();
        if (this.isEnabled() != other.isEnabled()
                || this.getEpoch() != other.getEpoch()
                || !this.getTruncationAddress().equals(other.getTruncationAddress())
                || ledgers.size() != otherLedgers.size()) {
            return false;
        }

        // Check each ledger.
        for (int i = 0; i < ledgers.size(); i++) {
            if (!ledgers.get(i).equals(otherLedgers.get(i))) {
                return false;
            }
        }

        // All tests have passed.
        return true;
    }
}
