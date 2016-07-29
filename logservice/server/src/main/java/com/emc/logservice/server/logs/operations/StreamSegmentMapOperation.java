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
import com.emc.logservice.server.ContainerMetadata;
import com.emc.logservice.server.logs.SerializationException;
import com.google.common.base.Preconditions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Log Operation that represents a mapping of StreamSegment Name to a StreamSegment Id.
 */
public class StreamSegmentMapOperation extends MetadataOperation implements StreamSegmentMapping {
    //region Members

    public static final byte OPERATION_TYPE = 4;
    private static final byte CURRENT_VERSION = 0;
    private long streamSegmentId;
    private String streamSegmentName;
    private long length;
    private boolean sealed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentMapOperation class.
     *
     * @param streamSegmentProperties Information about the StreamSegment.
     */
    public StreamSegmentMapOperation(SegmentProperties streamSegmentProperties) {
        super();
        this.streamSegmentId = ContainerMetadata.NO_STREAM_SEGMENT_ID;
        this.streamSegmentName = streamSegmentProperties.getName();
        this.length = streamSegmentProperties.getLength();
        this.sealed = streamSegmentProperties.isSealed();
    }

    protected StreamSegmentMapOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region StreamSegmentMapping implementation.

    @Override
    public String getStreamSegmentName() {
        return this.streamSegmentName;
    }

    @Override
    public long getStreamSegmentId() {
        return this.streamSegmentId;
    }

    /**
     * Sets the StreamSegmentId for this operation.
     * @param value
     */
    public void setStreamSegmentId(long value) {
        Preconditions.checkState(this.streamSegmentId == ContainerMetadata.NO_STREAM_SEGMENT_ID, "StreamSegmentId has already been assigned for this operation.");
        Preconditions.checkArgument(value != ContainerMetadata.NO_STREAM_SEGMENT_ID, "Invalid StreamSegmentId");
        this.streamSegmentId = value;
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
        ensureSerializationCondition(this.streamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "StreamSegment Id has not been assigned for this entry.");
        target.writeByte(CURRENT_VERSION);
        target.writeLong(this.streamSegmentId);
        target.writeUTF(this.streamSegmentName);
        target.writeLong(this.length);
        target.writeBoolean(this.sealed);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        byte version = readVersion(source, CURRENT_VERSION);
        this.streamSegmentId = source.readLong();
        this.streamSegmentName = source.readUTF();
        this.length = source.readLong();
        this.sealed = source.readBoolean();
    }

    @Override
    public String toString() {
        return String.format(
                "%s, Id = %s, Name = %s, Length = %d, Sealed = %s",
                super.toString(),
                toString(getStreamSegmentId(), ContainerMetadata.NO_STREAM_SEGMENT_ID),
                getStreamSegmentName(),
                getLength(),
                isSealed());
    }

    //endregion
}
