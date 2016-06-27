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
 * Log Operation that represents a mapping between a Batch Stream and its Parent Stream.
 */
public class BatchMapOperation extends MetadataOperation {
    //region Members

    public static final byte OPERATION_TYPE = 5;
    private static final byte CURRENT_VERSION = 0;
    private long parentStreamSegmentId;
    private long batchStreamSegmentId;
    private String batchStreamSegmentName;
    private long batchStreamSegmentLength;
    private boolean batchStreamSegmentSealed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BatchMapOperation class.
     *
     * @param parentStreamSegmentId The Id of the Parent StreamSegment.
     * @param batchStreamSegmentId  The Id of the Batch StreamSegment.
     * @param batchStreamInfo       SegmentProperties for Batch StreamSegment.
     */
    public BatchMapOperation(long parentStreamSegmentId, long batchStreamSegmentId, SegmentProperties batchStreamInfo) {
        super();
        this.parentStreamSegmentId = parentStreamSegmentId;
        this.batchStreamSegmentId = batchStreamSegmentId;
        this.batchStreamSegmentName = batchStreamInfo.getName();
        this.batchStreamSegmentLength = batchStreamInfo.getLength();
        this.batchStreamSegmentSealed = batchStreamInfo.isSealed();
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
     * Gets a value indicating the Id of the Batch StreamSegment.
     *
     * @return
     */
    public long getBatchStreamSegmentId() {
        return this.batchStreamSegmentId;
    }

    /**
     * Gets a value indicating the Name of the Batch StreamSegment.
     *
     * @return
     */
    public String getBatchStreamSegmentName() {
        return this.batchStreamSegmentName;
    }

    /**
     * Gets a value indicating the Length of the Batch StreamSegment.
     *
     * @return
     */
    public long getBatchStreamSegmentLength() {
        return this.batchStreamSegmentLength;
    }

    /**
     * Gets a value indicating whether the Batch StreamSegment is currently sealed.
     *
     * @return
     */
    public boolean isBatchSealed() {
        return this.batchStreamSegmentSealed;
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
        target.writeLong(this.parentStreamSegmentId);
        target.writeLong(this.batchStreamSegmentId);
        target.writeUTF(this.batchStreamSegmentName);
        target.writeLong(this.batchStreamSegmentLength);
        target.writeBoolean(this.batchStreamSegmentSealed);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        byte version = readVersion(source, CURRENT_VERSION);
        this.parentStreamSegmentId = source.readLong();
        this.batchStreamSegmentId = source.readLong();
        this.batchStreamSegmentName = source.readUTF();
        this.batchStreamSegmentLength = source.readLong();
        this.batchStreamSegmentSealed = source.readBoolean();
    }

    @Override
    public String toString() {
        return String.format("%s, ParentStreamSegmentId = %d, BatchStreamSegmentId = %d, BatchName = %s, BatchLength = %d, BatchSealed = %s", super.toString(), getParentStreamSegmentId(), getBatchStreamSegmentId(), getBatchStreamSegmentName(), getBatchStreamSegmentLength(), isBatchSealed());
    }

    //endregion
}
