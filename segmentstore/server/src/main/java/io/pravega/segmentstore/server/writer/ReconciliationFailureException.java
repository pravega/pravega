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
package io.pravega.segmentstore.server.writer;

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.SegmentMetadata;

/**
 * Exception thrown when an unrecoverable Reconciliation Failure has been detected.
 */
class ReconciliationFailureException extends DataCorruptionException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the ReconciliationFailureException.
     *
     * @param message         The message to include.
     * @param segmentMetadata The SegmentMetadata of the Segment for which reconciliation was attempted.
     * @param storageInfo     Information about the segment in Storage.
     */
    ReconciliationFailureException(String message, SegmentMetadata segmentMetadata, SegmentProperties storageInfo) {
        super(String.format(
                "%s Segment = %s, Storage: Length=%d(%s), Metadata: Length=%d(%s)",
                message,
                segmentMetadata.getName(),
                storageInfo.getLength(),
                getSealedMessage(storageInfo.isSealed()),
                segmentMetadata.getStorageLength(),
                getSealedMessage(segmentMetadata.isSealedInStorage())));
    }

    private static String getSealedMessage(boolean sealed) {
        return sealed ? "Sealed" : "Not Sealed";
    }
}
