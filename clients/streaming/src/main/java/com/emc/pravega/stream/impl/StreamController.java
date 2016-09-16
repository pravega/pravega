package com.emc.pravega.stream.impl;

import java.util.UUID;

import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.impl.segment.SegmentInputConfiguration;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputConfiguration;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;

public interface StreamController extends Router {
    
    /**
     * Creates a new segment with given name. Returns false if the segment already
     * existed
     */
    boolean createSegment(String segment);

    /**
     * Creates a new transaction
     */
    void createTransaction(Stream stream, UUID txId, long timeout);

    /**
     * Commits a transaction.
     */
    void commitTransaction(Stream stream, UUID txId) throws TxFailedException;

    /**
     * Drops a transaction (and all data written to it)
     */
    void dropTransaction(Stream stream, UUID txId);

    Transaction.Status checkTransactionStatus(Stream stream, UUID txId);
    
    /**
     * Opens a transaction for Appending to a segment.
     */
    SegmentOutputStream openTransactionForAppending(String segmentName, UUID txId);

    /**
     * Opens an existing segment for appending. this operation will fail if the segment does not
     * exist
     * This operation may be called multiple times on the same segment from the
     * same or different clients (i.e., there can be concurrent Stream Writers
     * in the same process space).
     */
    SegmentOutputStream openSegmentForAppending(String name, SegmentOutputConfiguration config) throws SegmentSealedException;

    /**
     * Opens an existing segment for reading. This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same stream from the
     * same client (i.e., there can be concurrent Stream Readers in the same
     * process space).
     */
    SegmentInputStream openSegmentForReading(String name, SegmentInputConfiguration config);

}
