/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
     * @param config  The SegmentOutputConfiguration to use.
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    SegmentOutputStream createOutputStreamForSegment(Segment segment, SegmentOutputConfiguration config) throws SegmentSealedException;
}