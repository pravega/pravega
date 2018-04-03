/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.pravega.common.Exceptions;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * Metadata for a particular Stream Segment.
 */
@Slf4j
@ThreadSafe
public class StreamSegmentMetadata implements UpdateableSegmentMetadata {
    //region Members

    private final String traceObjectId;
    private final String name;
    private final long streamSegmentId;
    private final long parentStreamSegmentId;
    private final int containerId;
    @GuardedBy("this")
    private final Map<UUID, Long> coreAttributes;
    @GuardedBy("this")
    private final Map<UUID, ExtendedAttributeValue> extendedAttributes;
    @GuardedBy("this")
    private long storageLength;
    @GuardedBy("this")
    private long startOffset;
    @GuardedBy("this")
    private long length;
    @GuardedBy("this")
    private boolean sealed;
    @GuardedBy("this")
    private boolean sealedInStorage;
    @GuardedBy("this")
    private boolean deleted;
    @GuardedBy("this")
    private boolean merged;
    @GuardedBy("this")
    private ImmutableDate lastModified;
    @GuardedBy("this")
    private long lastUsed;
    @GuardedBy("this")
    private boolean active;

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
        this.startOffset = 0;
        this.storageLength = -1;
        this.length = -1;
        this.coreAttributes = new HashMap<>();
        this.extendedAttributes = new HashMap<>();
        this.lastModified = new ImmutableDate();
        this.lastUsed = 0;
        this.active = true;
    }

    //endregion

    //region SegmentProperties Implementation

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public synchronized boolean isSealed() {
        return this.sealed;
    }

    @Override
    public synchronized boolean isDeleted() {
        return this.deleted;
    }

    @Override
    public synchronized ImmutableDate getLastModified() {
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
    public synchronized boolean isMerged() {
        return this.merged;
    }

    @Override
    public synchronized boolean isSealedInStorage() {
        return this.sealedInStorage;
    }

    @Override
    public synchronized long getStorageLength() {
        return this.storageLength;
    }

    @Override
    public synchronized long getStartOffset() {
        return this.startOffset;
    }

    @Override
    public synchronized long getLength() {
        return this.length;
    }

    @Override
    public synchronized Map<UUID, Long> getAttributes() {
        return new AttributesView();
    }

    @Override
    public String toString() {
        return String.format(
                "Id = %d, Start = %d, Length = %d, StorageLength = %d, Sealed(M/S) = %s/%s, Deleted = %s, Name = %s",
                getId(),
                getStartOffset(),
                getLength(),
                getStorageLength(),
                isSealed(),
                isSealedInStorage(),
                isDeleted(),
                getName());
    }

    //endregion

    //region UpdateableSegmentMetadata Implementation

    @Override
    public synchronized void setStorageLength(long value) {
        Exceptions.checkArgument(value >= 0, "value", "Storage Length must be a non-negative number.");
        Exceptions.checkArgument(value >= this.storageLength, "value", "New Storage Length cannot be smaller than the previous one.");

        log.trace("{}: StorageLength changed from {} to {}.", this.traceObjectId, this.storageLength, value);
        this.storageLength = value;
    }

    @Override
    public synchronized void setStartOffset(long value) {
        if (this.startOffset == value) {
            // Nothing to do.
            return;
        }

        Preconditions.checkState(!isTransaction(), "Cannot set Start Offset for a Transaction.");
        Exceptions.checkArgument(value >= 0, "value", "StartOffset must be a non-negative number.");
        Exceptions.checkArgument(value >= this.startOffset, "value", "New StartOffset cannot be smaller than the previous one.");
        Exceptions.checkArgument(value <= this.length, "value", "New StartOffset cannot be larger than Length.");
        log.debug("{}: StartOffset changed from {} to {}.", this.traceObjectId, this.startOffset, value);
        this.startOffset = value;
    }

    @Override
    public synchronized void setLength(long value) {
        Exceptions.checkArgument(value >= 0, "value", "Length must be a non-negative number.");
        Exceptions.checkArgument(value >= this.length, "value", "New Length cannot be smaller than the previous one.");

        log.trace("{}: Length changed from {} to {}.", this.traceObjectId, this.length, value);
        this.length = value;
    }

    @Override
    public synchronized void markSealed() {
        log.debug("{}: Sealed = true.", this.traceObjectId);
        this.sealed = true;
    }

    @Override
    public synchronized void markSealedInStorage() {
        Preconditions.checkState(this.sealed, "Cannot mark SealedInStorage if not Sealed in DurableLog.");
        log.debug("{}: SealedInStorage = true.", this.traceObjectId);
        this.sealedInStorage = true;
    }

    @Override
    public synchronized void markDeleted() {
        log.debug("{}: Deleted = true.", this.traceObjectId);
        this.deleted = true;
    }

    @Override
    public synchronized void markMerged() {
        Preconditions.checkState(this.parentStreamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "Cannot merge a non-Transaction StreamSegment.");

        log.debug("{}: Merged = true.", this.traceObjectId);
        this.merged = true;
    }

    @Override
    public synchronized void setLastModified(ImmutableDate date) {
        this.lastModified = date;
        log.trace("{}: LastModified = {}.", this.lastModified);
    }

    @Override
    public synchronized void updateAttributes(Map<UUID, Long> attributes) {
        for (Map.Entry<UUID, Long> av : attributes.entrySet()) {
            UUID key = av.getKey();
            long value = av.getValue();
            if (Attributes.isCoreAttribute(key)) {
                if (value == SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
                    this.coreAttributes.remove(av.getKey());
                } else {
                    this.coreAttributes.put(av.getKey(), value);
                }
            } else {
                if (value == SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
                    this.extendedAttributes.remove(av.getKey());
                } else {
                    this.extendedAttributes.put(av.getKey(), new ExtendedAttributeValue(value, 0));
                }
            }
        }
    }

    @Override
    public synchronized void copyFrom(SegmentMetadata base) {
        Exceptions.checkArgument(this.getId() == base.getId(), "base", "Given SegmentMetadata refers to a different StreamSegment than this one (SegmentId).");
        Exceptions.checkArgument(this.getName().equals(base.getName()), "base", "Given SegmentMetadata refers to a different StreamSegment than this one (SegmentName).");
        Exceptions.checkArgument(this.getParentId() == base.getParentId(), "base", "Given SegmentMetadata has a different parent StreamSegment than this one.");

        log.debug("{}: copyFrom {}.", this.traceObjectId, base.getClass().getSimpleName());
        setStorageLength(base.getStorageLength());
        setLength(base.getLength());

        // Update StartOffset after (potentially) updating the length, since he Start Offset must be less than or equal to Length.
        setStartOffset(base.getStartOffset());
        setLastModified(base.getLastModified());
        updateAttributes(base.getAttributes());

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

        setLastUsed(base.getLastUsed());
    }

    @Override
    public synchronized void setLastUsed(long value) {
        this.lastUsed = Math.max(value, this.lastUsed);
    }

    @Override
    public synchronized long getLastUsed() {
        return this.lastUsed;
    }

    @Override
    public synchronized boolean isActive() {
        return this.active;
    }

    synchronized void markInactive() {
        this.active = false;
    }

    //endregion

    //region ExtendedAttributeValue

    private static class ExtendedAttributeValue {
        private final long value;
        private long lastTouchedSequenceNumber; // TODO: this is not available here. Perhaps use Length/StorageLength?

        ExtendedAttributeValue(long value, long currentSequenceNumber) {
            this.value = value;
            this.lastTouchedSequenceNumber = currentSequenceNumber;
        }

        @Override
        public String toString() {
            return String.format("%d (SN=%d)", this.value, this.lastTouchedSequenceNumber);
        }
    }

    //endregion

    //region AttributesView

    /**
     * Read-only view of this SegmentMetadata's Attributes (combines Core and Extended Attributes into one single Map).
     */
    private class AttributesView implements Map<UUID, Long> {
        @Override
        public int size() {
            synchronized (StreamSegmentMetadata.this) {
                return coreAttributes.size() + extendedAttributes.size();
            }
        }

        @Override
        public boolean isEmpty() {
            synchronized (StreamSegmentMetadata.this) {
                return coreAttributes.isEmpty() && extendedAttributes.isEmpty();
            }
        }

        @Override
        public boolean containsKey(Object o) {
            synchronized (StreamSegmentMetadata.this) {
                return coreAttributes.containsKey(o) || extendedAttributes.containsKey(o);
            }
        }

        @Override
        public Long get(Object o) {
            synchronized (StreamSegmentMetadata.this) {
                Long result = coreAttributes.get(o);
                if (result == null) {
                    ExtendedAttributeValue r = extendedAttributes.get(o);
                    if (r != null) {
                        // TODO: update lastTouched. Should we also do it for containsKey/keySet/values/entrySet ?
                        // TODO: how do we handle CAS operations, which operate on a different Map/Object.
                        result = r.value;
                    }
                }
                return result;
            }
        }

        @Override
        public Set<UUID> keySet() {
            synchronized (StreamSegmentMetadata.this) {
                return Sets.union(coreAttributes.keySet(), extendedAttributes.keySet());
            }
        }

        @Override
        public Collection<Long> values() {
            synchronized (StreamSegmentMetadata.this) {
                return Stream.concat(
                        coreAttributes.values().stream(),
                        extendedAttributes.values()
                                .stream().map(eav -> eav.value))
                        .collect(Collectors.toList());
            }
        }

        @Override
        public Set<Map.Entry<UUID, Long>> entrySet() {
            synchronized (StreamSegmentMetadata.this) {
                return Stream.concat(
                        coreAttributes.entrySet().stream(),
                        extendedAttributes.entrySet()
                                .stream().map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue().value)))
                        .collect(Collectors.toSet());
            }
        }

        @Override
        public String toString() {
            synchronized (StreamSegmentMetadata.this) {
                return String.format("Core: %s, Extended: %s", coreAttributes, extendedAttributes);
            }
        }

        //region Unsupported Methods

        @Override
        public boolean containsValue(Object o) {
            // Not a very useful one anyway.
            throw new UnsupportedOperationException();
        }

        @Override
        public Long put(UUID uuid, Long aLong) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(Map<? extends UUID, ? extends Long> map) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        //endregion
    }

    //endregion

}
