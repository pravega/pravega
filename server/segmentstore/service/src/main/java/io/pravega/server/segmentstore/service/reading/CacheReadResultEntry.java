/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.service.reading;

import io.pravega.common.Exceptions;
import io.pravega.server.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.server.segmentstore.contracts.ReadResultEntryType;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Read Result Entry for data that is readily available for reading (in memory).
 */
class CacheReadResultEntry extends ReadResultEntryBase {
    /**
     * Creates a new instance of the CacheReadResultEntry class.
     *
     * @param streamSegmentOffset The offset within the StreamSegment where this ReadResultEntry starts at. NOTE: this is
     *                            not where the first byte of 'data' starts, rather it's where dataOffset points to in the
     *                            StreamSegment.
     * @param data                The data buffer that contains the data.
     * @param dataOffset          The offset within data where this ReadResultEntry starts at.
     * @param dataLength          The length of the data that this ReadResultEntry has.
     */
    CacheReadResultEntry(long streamSegmentOffset, byte[] data, int dataOffset, int dataLength) {
        super(ReadResultEntryType.Cache, streamSegmentOffset + dataOffset, dataLength);
        Exceptions.checkArrayRange(dataOffset, dataLength, data.length, "dataOffset", "dataLength");
        complete(new ReadResultEntryContents(new ByteArrayInputStream(data, dataOffset, dataLength), dataLength));
    }

    /**
     * Creates a new instance of the CacheReadResultEntry class.
     *
     * @param streamSegmentOffset The offset within the StreamSegment where this ReadResultEntry starts at.
     * @param data                An InputStream representing the data to be read.
     * @param dataLength          The length of the data that this ReadResultEntry has (length of the given InputStream).
     */
    CacheReadResultEntry(long streamSegmentOffset, InputStream data, int dataLength) {
        super(ReadResultEntryType.Cache, streamSegmentOffset, dataLength);
        complete(new ReadResultEntryContents(data, dataLength));
    }
}
