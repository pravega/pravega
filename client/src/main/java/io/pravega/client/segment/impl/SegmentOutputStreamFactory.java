/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.stream.EventWriterConfig;
import java.util.UUID;
import java.util.function.Consumer;

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
     * @param config  The configuration for the writer
     * @param delegationToken token to pass on to segmentstore to authenticate access to the segment.
     * @return New instance of SegmentOutputStream with an open transaction.
     */
    SegmentOutputStream createOutputStreamForTransaction(Segment segment, UUID txId, EventWriterConfig config, String delegationToken);

    /**
     * Creates a stream for an existing segment. This operation will fail if the segment does not
     * exist or is sealed.
     * This operation may be called multiple times on the same segment from the
     * same or different clients (i.e., there can be concurrent Stream Writers
     * in the same process space).
     *
     * @param segment The segment.
     * @param segmentSealedCallback Method to be executed on receiving SegmentSealed from SSS.
     * @param config  The configuration for the writer
     * @param delegationToken token to pass on to segmentstore to authenticate access to the segment.
     * @return New instance of SegmentOutputStream for writing.
     */
    SegmentOutputStream createOutputStreamForSegment(Segment segment, Consumer<Segment> segmentSealedCallback, EventWriterConfig config, String delegationToken);
    
    /**
     * Creates a stream for an existing segment. This operation will fail if the segment does not
     * exist or is sealed.
     * This operation may be called multiple times on the same segment from the
     * same or different clients (i.e., there can be concurrent Stream Writers
     * in the same process space).
     *
     * @param segment The segment.
     * @param config  The configuration for the writer
     * @return New instance of SegmentOutputStream for writing.
     */
    SegmentOutputStream createOutputStreamForSegment(Segment segment, EventWriterConfig config);
}
