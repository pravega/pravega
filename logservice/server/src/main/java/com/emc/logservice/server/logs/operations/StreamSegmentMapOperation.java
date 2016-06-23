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
import com.emc.logservice.server.logs.SerializationException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Log Operation that represents a mapping of StreamSegment Name to a StreamSegment Id.
 */
public class StreamSegmentMapOperation extends MetadataOperation {
    //region Members

    public static final byte OPERATION_TYPE = 4;
    private static final byte CURRENT_VERSION = 0;
    private long streamSegmentId;
    private String streamSegmentName;
    private long streamSegmentLength;
    private boolean sealed;
    private long lastModifiedTime;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentMapOperation class.
     *
     * @param streamSegmentId         The Id of the StreamSegment.
     * @param streamSegmentProperties Information about the StreamSegment.
     */
    public StreamSegmentMapOperation(long streamSegmentId, SegmentProperties streamSegmentProperties) {
        super();
        this.streamSegmentId = streamSegmentId;
        this.streamSegmentName = streamSegmentProperties.getName();
        this.streamSegmentLength = streamSegmentProperties.getLength();
        this.sealed = streamSegmentProperties.isSealed();
        this.lastModifiedTime = streamSegmentProperties.getLastModified().getTime();
    }

    protected StreamSegmentMapOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region StreamSegmentMapOperation Properties

    /**
     * Gets a value indicating the Name of the StreamSegment.
     *
     * @return
     */
    public String getStreamSegmentName() {
        return this.streamSegmentName;
    }

    /**
     * Gets a value indicating the Id of the StreamSegment.
     *
     * @return
     */
    public long getStreamSegmentId() {
        return this.streamSegmentId;
    }

    /**
     * Gets a value indicating the Length of the StreamSegment.
     *
     * @return
     */
    public long getStreamSegmentLength() {
        return this.streamSegmentLength;
    }

    /**
     * Gets a value indicating whether the StreamSegment is currently sealed.
     *
     * @return
     */
    public boolean isSealed() {
        return this.sealed;
    }

    /**
     * Gets a value indicating the Last Modified Time of the StreamSegment. TODO: figure out what data type this should be.
     *
     * @return
     */
    public long getLastModifiedTime() {
        return this.lastModifiedTime;
    }

    //endregion

    //region Operation Implementation

    @Override
    protected byte getOperationType() {
        return OPERATION_TYPE;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        target.writeByte(CURRENT_VERSION);
        target.writeLong(this.streamSegmentId);
        target.writeUTF(this.streamSegmentName);
        target.writeLong(this.streamSegmentLength);
        target.writeBoolean(this.sealed);
        target.writeLong(this.lastModifiedTime);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        byte version = readVersion(source, CURRENT_VERSION);
        this.streamSegmentId = source.readLong();
        this.streamSegmentName = source.readUTF();
        this.streamSegmentLength = source.readLong();
        this.sealed = source.readBoolean();
        this.lastModifiedTime = source.readLong();
    }

    @Override
    public String toString() {
        return String.format("%s, Id = %d, Name = %s, Length = %d, Sealed = %s", super.toString(), getStreamSegmentId(), getStreamSegmentName(), getStreamSegmentLength(), isSealed());
    }

    //endregion
}
