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

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

/**
 * Metadata for a particular Stream Segment.
 */
@Slf4j
public class StreamSegmentMetadata implements UpdateableSegmentMetadata {
    //region Members

    private final String traceObjectId;
    private final String name;
    private final long streamSegmentId;
    private final long parentStreamSegmentId;
    private final int containerId;
    private final AbstractMap<UUID, AppendContext> lastCommittedAppends;
    private long storageLength;
    private long durableLogLength;
    private boolean sealed;
    private boolean sealedInStorage;
    private boolean deleted;
    private boolean merged;
    private Date lastModified;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentMetadata class for a stand-alone StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param streamSegmentId   The Id of the StreamSegment.
     * @param containerId       The Id of the Container this StreamSegment belongs to.
     * @throws IllegalArgumentException If either of the arguments are invalid.
     */
    public StreamSegmentMetadata(String streamSegmentName, long streamSegmentId, int containerId) {
        this(streamSegmentName, streamSegmentId, ContainerMetadata.NO_STREAM_SEGMENT_ID, containerId);
    }

    /**
     * Creates a new instance of the StreamSegmentMetadata class for a child (Transaction) StreamSegment.
     *
     * @param streamSegmentName     The name of the StreamSegment.
     * @param streamSegmentId       The Id of the StreamSegment.
     * @param parentStreamSegmentId The Id of the Parent StreamSegment.
     * @param containerId           The Id of the Container this StreamSegment belongs to.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    public StreamSegmentMetadata(String streamSegmentName, long streamSegmentId, long parentStreamSegmentId, int containerId) {
        Exceptions.checkNotNullOrEmpty(streamSegmentName, "streamSegmentName");
        Preconditions.checkArgument(streamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "streamSegmentId");
        Preconditions.checkArgument(containerId >= 0, "containerId");

        this.traceObjectId = String.format("StreamSegment[%d]", streamSegmentId);
        this.name = streamSegmentName;
        this.streamSegmentId = streamSegmentId;
        this.parentStreamSegmentId = parentStreamSegmentId;
        this.containerId = containerId;
        this.sealed = false;
        this.sealedInStorage = false;
        this.deleted = false;
        this.merged = false;
        this.storageLength = -1;
        this.durableLogLength = -1;
        this.lastCommittedAppends = new HashMap<>();
        this.lastModified = new Date(); // TODO: figure out what is the best way to represent this, while taking into account PermanentStorage timestamps, timezones, etc.
    }

    //endregion

    //region SegmentProperties Implementation

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean isSealed() {
        return this.sealed;
    }

    @Override
    public boolean isDeleted() {
        return this.deleted;
    }

    @Override
    public long getLength() {
        return this.durableLogLength; // ReadableLength is essentially DurableLogLength.
    }

    @Override
    public Date getLastModified() {
        return this.lastModified;
    }

    //endregion

    //region SegmentMetadata Implementation

    @Override
    public long getId() {
        return this.streamSegmentId;
    }

    @Override
    public long getParentId() {
        return this.parentStreamSegmentId;
    }

    @Override
    public int getContainerId() {
        return this.containerId;
    }

    @Override
    public boolean isMerged() {
        return this.merged;
    }

    @Override
    public boolean isSealedInStorage() {
        return this.sealedInStorage;
    }

    @Override
    public long getStorageLength() {
        return this.storageLength;
    }

    @Override
    public long getDurableLogLength() {
        return this.durableLogLength;
    }

    @Override
    public AppendContext getLastAppendContext(UUID clientId) {
        return this.lastCommittedAppends.getOrDefault(clientId, null);
    }

    @Override
    public Collection<UUID> getKnownClientIds() {
        return this.lastCommittedAppends.keySet();
    }

    @Override
    public String toString() {
        return String.format(
                "Id = %d, StorageLength = %d, DLOffset = %d, Sealed(DL/S) = %s/%s, Deleted = %s, Name = %s",
                getId(),
                getStorageLength(),
                getDurableLogLength(),
                isSealed(),
                isSealedInStorage(),
                isDeleted(),
                getName());
    }

    //endregion

    //region UpdateableSegmentMetadata Implementation

    @Override
    public void setStorageLength(long value) {
        Exceptions.checkArgument(value >= 0, "value", "Storage Length must be a non-negative number.");
        Exceptions.checkArgument(value >= this.storageLength, "value", "New Storage Length cannot be smaller than the previous one.");

        log.trace("{}: StorageLength changed from {} to {}.", this.traceObjectId, this.storageLength, value);
        this.storageLength = value;
    }

    @Override
    public void setDurableLogLength(long value) {
        Exceptions.checkArgument(value >= 0, "value", "Durable Log Length must be a non-negative number.");
        Exceptions.checkArgument(value >= this.durableLogLength, "value", "New Durable Log Length cannot be smaller than the previous one.");

        log.trace("{}: DurableLogLength changed from {} to {}.", this.traceObjectId, this.durableLogLength, value);
        this.durableLogLength = value;
    }

    @Override
    public void markSealed() {
        log.trace("{}: Sealed = true.", this.traceObjectId);
        this.sealed = true;
    }

    @Override
    public void markSealedInStorage() {
        Preconditions.checkState(this.sealed, "Cannot mark SealedInStorage if not Sealed in DurableLog.");
        log.trace("{}: SealedInStorage = true.", this.traceObjectId);
        this.sealedInStorage = true;
    }

    @Override
    public void markDeleted() {
        log.trace("{}: Deleted = true.", this.traceObjectId);
        this.deleted = true;
    }

    @Override
    public void markMerged() {
        Preconditions.checkState(this.parentStreamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "Cannot merge a non-Transaction StreamSegment.");

        log.trace("{}: Merged = true.", this.traceObjectId);
        this.merged = true;
    }

    @Override
    public void setLastModified(Date date) {
        this.lastModified = date;
        log.trace("{}: LastModified = {}.", this.lastModified);
    }

    @Override
    public void recordAppendContext(AppendContext appendContext) {
        this.lastCommittedAppends.put(appendContext.getClientId(), appendContext);
    }

    @Override
    public void copyFrom(SegmentMetadata base) {
        Exceptions.checkArgument(this.getId() == base.getId(), "base", "Given SegmentMetadata refers to a different StreamSegment than this one (SegmentId).");
        Exceptions.checkArgument(this.getName().equals(base.getName()), "base", "Given SegmentMetadata refers to a different StreamSegment than this one (SegmentName).");
        Exceptions.checkArgument(this.getParentId() == base.getParentId(), "base", "Given SegmentMetadata has a different parent StreamSegment than this one.");

        log.debug("{}: copyFrom {}.", this.traceObjectId, base.getClass().getSimpleName());
        setStorageLength(base.getStorageLength());
        setDurableLogLength(base.getDurableLogLength());
        setLastModified(base.getLastModified());
        for (UUID clientId : base.getKnownClientIds()) {
            recordAppendContext(base.getLastAppendContext(clientId));
        }

        if (base.isSealed()) {
            markSealed();
            if (base.isSealedInStorage()) {
                markSealedInStorage();
            }
        }

        if (base.isMerged()) {
            markMerged();
        }

        if (base.isDeleted()) {
            markDeleted();
        }
    }

    //endregion
}
