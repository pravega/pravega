/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.server.logs.operations;

import com.emc.logservice.contracts.SegmentProperties;
import com.emc.logservice.server.SegmentMetadataCollection;
import com.emc.logservice.server.logs.SerializationException;
import com.google.common.base.Preconditions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Log Operation that represents a mapping between a Batch Stream and its Parent Stream.
 */
public class BatchMapOperation extends MetadataOperation implements StreamSegmentMapping {
    //region Members

    public static final byte OPERATION_TYPE = 5;
    private static final byte CURRENT_VERSION = 0;
    private long parentStreamSegmentId;
    private long streamSegmentId;
    private String streamSegmentName;
    private long length;
    private boolean sealed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BatchMapOperation class.
     *
     * @param parentStreamSegmentId The Id of the Parent StreamSegment.
     * @param batchStreamInfo       SegmentProperties for Batch StreamSegment.
     */
    public BatchMapOperation(long parentStreamSegmentId, SegmentProperties batchStreamInfo) {
        super();
        Preconditions.checkArgument(parentStreamSegmentId != SegmentMetadataCollection.NO_STREAM_SEGMENT_ID, "parentStreamSegmentId must be defined.");
        this.parentStreamSegmentId = parentStreamSegmentId;
        this.streamSegmentId = SegmentMetadataCollection.NO_STREAM_SEGMENT_ID;
        this.streamSegmentName = batchStreamInfo.getName();
        this.length = batchStreamInfo.getLength();
        this.sealed = batchStreamInfo.isSealed();
    }

    protected BatchMapOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region BatchMapOperation Properties

    /**
     * Gets a value indicating the Id of the Parent StreamSegment.
     *
     * @return
     */
    public long getParentStreamSegmentId() {
        return this.parentStreamSegmentId;
    }

    /**
     * Sets the StreamSegmentId for this operation.
     *
     * @param value
     */
    public void setStreamSegmentId(long value) {
        Preconditions.checkState(this.streamSegmentId == SegmentMetadataCollection.NO_STREAM_SEGMENT_ID, "StreamSegmentId has already been assigned for this operation.");
        Preconditions.checkArgument(value != SegmentMetadataCollection.NO_STREAM_SEGMENT_ID, "Invalid StreamSegmentId");
        this.streamSegmentId = value;
    }

    //endregion

    //region StreamSegmentMapping Implementation

    @Override
    public long getStreamSegmentId() {
        return this.streamSegmentId;
    }

    @Override
    public String getStreamSegmentName() {
        return this.streamSegmentName;
    }

    @Override
    public long getLength() {
        return this.length;
    }

    @Override
    public boolean isSealed() {
        return this.sealed;
    }

    //endregion

    //region Operation Implementation

    @Override
    protected byte getOperationType() {
        return OPERATION_TYPE;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        ensureSerializationCondition(this.streamSegmentId != SegmentMetadataCollection.NO_STREAM_SEGMENT_ID, "BatchStreamSegment Id has not been assigned for this entry.");
        target.writeByte(CURRENT_VERSION);
        target.writeLong(this.parentStreamSegmentId);
        target.writeLong(this.streamSegmentId);
        target.writeUTF(this.streamSegmentName);
        target.writeLong(this.length);
        target.writeBoolean(this.sealed);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        byte version = readVersion(source, CURRENT_VERSION);
        this.parentStreamSegmentId = source.readLong();
        this.streamSegmentId = source.readLong();
        this.streamSegmentName = source.readUTF();
        this.length = source.readLong();
        this.sealed = source.readBoolean();
    }

    @Override
    public String toString() {
        return String.format(
                "%s, Id = %s, ParentId = %s, Name = %s, Length = %d, Sealed = %s",
                super.toString(),
                toString(getStreamSegmentId(), SegmentMetadataCollection.NO_STREAM_SEGMENT_ID),
                toString(getParentStreamSegmentId(), SegmentMetadataCollection.NO_STREAM_SEGMENT_ID),
                getStreamSegmentName(),
                getLength(),
                isSealed());
    }

    //endregion
}
