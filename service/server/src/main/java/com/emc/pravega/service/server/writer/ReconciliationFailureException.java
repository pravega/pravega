/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.writer;

import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.SegmentMetadata;

/**
 * Exception thrown when an unrecoverable Reconciliation Failure has been detected.
 */
class ReconciliationFailureException extends DataCorruptionException {

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
