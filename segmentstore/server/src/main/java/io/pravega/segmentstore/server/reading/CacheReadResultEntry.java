/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
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
     *                            where the first byte of 'data' starts; internally it will set {@link #getStreamSegmentOffset()}
     *                            to the sum of this value and dataOffset.
     * @param data                The data buffer that contains the data.
     * @param dataOffset          The offset within data where this ReadResultEntry starts at.
     * @param dataLength          The length of the data that this ReadResultEntry has.
     */
    @VisibleForTesting
    CacheReadResultEntry(long streamSegmentOffset, byte[] data, int dataOffset, int dataLength) {
        this(streamSegmentOffset + dataOffset, new ByteArrayInputStream(data, dataOffset, dataLength), dataLength);
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
