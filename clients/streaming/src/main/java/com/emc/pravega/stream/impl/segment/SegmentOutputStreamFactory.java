/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.stream.Segment;

import java.util.UUID;

/**
 * Creates {@link SegmentOutputStream} for segments and transactions.
 */
public interface SegmentOutputStreamFactory {
    /**
     * Creates a stream for an open transaction. This will fail if the segment does not exist or is sealed.
     * This may be called multiple times for the same transaction.
     *
     * @param segment The segment the transaction belongs to.
     * @param txId    The transaction id.
     * @return New instance of SegmentOutputStream with an open transaction.
     */
    SegmentOutputStream createOutputStreamForTransaction(Segment segment, UUID txId);

    /**
     * Creates a stream for an existing segment. This operation will fail if the segment does not
     * exist or is sealed.
     * This operation may be called multiple times on the same segment from the
     * same or different clients (i.e., there can be concurrent Stream Writers
     * in the same process space).
     *
     * @param segment The segment.
     * @throws SegmentSealedException If the segment is closed for modifications.
     * @return New instance of SegmentOutputStream for writing.
     */
    SegmentOutputStream createOutputStreamForSegment(Segment segment);
}
