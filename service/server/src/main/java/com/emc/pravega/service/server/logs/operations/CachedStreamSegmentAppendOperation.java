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

package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.logs.SerializationException;
import com.google.common.base.Preconditions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Log Operation that represents a StreamSegment Append. As opposed from StreamSegmentAppendOperation, this operation cannot
 * be serialized to a DurableLog. Its purpose is to be added to the In-Memory Transaction Log, where it binds a StreamSegmentAppendOperation
 * to its corresponding Cache entry.
 */
public class CachedStreamSegmentAppendOperation extends StorageOperation {
    //region Members

    private final int length;
    private final long streamSegmentOffset;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the CachedStreamSegmentAppendOperation based on the given StreamSegmentAppendOperation.
     * The created operation will have the same SequenceNumber, StreamSegmentId, Offset and Length as the base operation,
     * but it will not directly store the data (the contents of the Append is stored in the Cache, and will have to be
     * retrieved using properties of this object).
     *
     * @param baseOperation The StreamSegmentAppendOperation to use.
     */
    public CachedStreamSegmentAppendOperation(StreamSegmentAppendOperation baseOperation) {
        super(baseOperation.getStreamSegmentId());
        Preconditions.checkArgument(baseOperation.getStreamSegmentOffset() >= 0, "given baseOperation does not have an assigned StreamSegment Offset.");

        this.streamSegmentOffset = baseOperation.getStreamSegmentOffset();
        this.length = baseOperation.getData().length;
        if (baseOperation.getSequenceNumber() >= 0) {
            setSequenceNumber(baseOperation.getSequenceNumber());
        }
    }

    //endregion

    //region Properties

    /**
     * Creates a new CacheKey with information from this CachedStreamSegmentAppendOperation.
     */
    public CacheKey createCacheKey() {
        return new CacheKey(getStreamSegmentId(), getStreamSegmentOffset());
    }

    @Override
    public String toString() {
        return String.format(
                "%s, Offset = %d, Length = %d",
                super.toString(),
                this.streamSegmentOffset,
                this.length);
    }

    //endregion

    //region Operation Implementation

    @Override
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    @Override
    public long getLength() {
        return this.length;
    }

    @Override
    protected byte getOperationType() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " cannot be serialized, thus it does not have an Operation Type.");
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " cannot be serialized.");
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " cannot be deserialized.");
    }

    //endregion
}
