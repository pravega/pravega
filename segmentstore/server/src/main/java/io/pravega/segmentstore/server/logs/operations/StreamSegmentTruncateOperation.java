/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs.operations;

import com.google.common.base.Preconditions;
import io.pravega.common.io.SerializationException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import lombok.Getter;

/**
 * Log Operation that indicates a StreamSegment is to be truncated.
 */
public class StreamSegmentTruncateOperation extends StorageOperation implements SegmentOperation {
    //region Members

    private static final byte CURRENT_VERSION = 0;
    @Getter
    private long streamSegmentId;
    /**
     * The Offset at which to truncate the StreamSegment.
     */
    @Getter
    private long streamSegmentOffset;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentTruncateOperation class.
     *
     * @param streamSegmentId The Id of the StreamSegment to truncate.
     * @param offset          The Offset at which to truncate.
     */
    public StreamSegmentTruncateOperation(long streamSegmentId, long offset) {
        super(streamSegmentId);
        Preconditions.checkArgument(offset >= 0, "offset must be a non-negative number.");
        this.streamSegmentId = streamSegmentId;
        this.streamSegmentOffset = offset;
    }

    protected StreamSegmentTruncateOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    @Override
    public long getLength() {
        return 0;
    }

    //endregion

    //region Operation Implementation

    @Override
    protected OperationType getOperationType() {
        return OperationType.Truncate;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        target.writeByte(CURRENT_VERSION);
        target.writeLong(getStreamSegmentId());
        target.writeLong(this.streamSegmentOffset);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        this.streamSegmentId = source.readLong();
        this.streamSegmentOffset = source.readLong();
    }

    @Override
    public String toString() {
        return String.format("%s, Offset = %s", super.toString(), this.streamSegmentOffset);
    }

    //endregion
}
