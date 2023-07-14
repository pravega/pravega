/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.CollectionHelpers;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Metadata for a particular Stream Segment.
 */
@Slf4j
@ThreadSafe
public class StreamSegmentMetadata implements UpdateableSegmentMetadata {
    //region Members

    private final String traceObjectId;
    @Getter
    private final String name;
    private final long streamSegmentId;
    @Getter
    private final int containerId;
    @Getter
    private volatile SegmentType type;
    @Getter
    private volatile int attributeIdLength;
    @GuardedBy("this")
    private final Map<AttributeId, Long> coreAttributes;
    @GuardedBy("this")
    private final Map<AttributeId, ExtendedAttributeValue> extendedAttributes;
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
    private boolean deletedInStorage;
    @GuardedBy("this")
    private boolean merged;
    @GuardedBy("this")
    private ImmutableDate lastModified;
    @GuardedBy("this")
    private long lastUsed;
    @GuardedBy("this")
    private boolean active;
    @GuardedBy("this")
    private boolean pinned;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentMetadata class for a StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param streamSegmentId   The Id of the StreamSegment.
     * @param containerId       The Id of the Container this StreamSegment belongs to.
     * @throws IllegalArgumentException If either of the arguments are invalid.
     */
    public StreamSegmentMetadata(String streamSegmentName, long streamSegmentId, int containerId) {
        Exceptions.checkNotNullOrEmpty(streamSegmentName, "streamSegmentName");
        Preconditions.checkArgument(streamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "streamSegmentId");
        Preconditions.checkArgument(containerId >= 0, "containerId");

        this.traceObjectId = String.format("StreamSegment[%d-%d]", containerId, streamSegmentId);
        this.name = streamSegmentName;
        this.streamSegmentId = streamSegmentId;
        this.containerId = containerId;
        this.sealed = false;
        this.sealedInStorage = false;
        this.deleted = false;
        this.deletedInStorage = false;
        this.merged = false;
        this.startOffset = 0;
        this.storageLength = -1;
        this.length = -1;
        this.coreAttributes = new ConcurrentHashMap<>(); // These could be iterated and modified concurrently.
        this.extendedAttributes = new ConcurrentHashMap<>();
        this.lastModified = new ImmutableDate();
        this.lastUsed = 0;
        this.active = true;
        this.type = SegmentType.STREAM_SEGMENT; // Uninitialized.
        this.attributeIdLength = -1;
    }

    //endregion

    //region SegmentProperties Implementation

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
    public synchronized boolean isMerged() {
        return this.merged;
    }

    @Override
    public synchronized boolean isDeletedInStorage() {
        return this.deletedInStorage;
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
    public synchronized Map<AttributeId, Long> getAttributes() {
        return new AttributesView();
    }

    @Override
    public synchronized Map<AttributeId, Long> getAttributes(BiPredicate<AttributeId, Long> filter) {
        return getAttributes().entrySet().stream()
                .filter(e -> filter.test(e.getKey(), e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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

        log.trace("{}: Length changed from {} to {} for {}", this.traceObjectId, this.length, value, this.name);
        this.length = value;
    }

    @Override
    public synchronized void markSealed() {
        log.debug("{}: Sealed = true.", this.traceObjectId);
        this.sealed = true;
    }

    @Override
    public synchronized void markSealedInStorage() {
        Preconditions.checkState(this.sealed, "Cannot mark SealedInStorage if not Sealed in Metadata.");
        log.debug("{}: SealedInStorage = true.", this.traceObjectId);
        this.sealedInStorage = true;
    }

    @Override
    public synchronized void markMerged() {
        log.debug("{}: Merged = true.", this.traceObjectId);
        this.merged = true;
    }

    @Override
    public synchronized void markDeleted() {
        log.debug("{}: Deleted = true.", this.traceObjectId);
        this.deleted = true;
    }

    @Override
    public synchronized void markDeletedInStorage() {
        Preconditions.checkState(this.deleted, "Cannot mark DeletedInStorage if not Deleted in Metadata.");
        log.debug("{}: DeletedInStorage = true.", this.traceObjectId);
        this.deletedInStorage = true;
    }

    @Override
    public synchronized void markPinned() {
        log.debug("{}: Pinned = true.", this.traceObjectId);
        this.pinned = true;
    }

    @Override
    public synchronized void setLastModified(ImmutableDate date) {
        this.lastModified = date;
        log.trace("{}: LastModified = {}.", this.traceObjectId, this.lastModified);
    }

    @Override
    public synchronized void updateAttributes(Map<AttributeId, Long> attributes) {
        attributes.forEach((id, value) -> {
            if (Attributes.isCoreAttribute(id)) {
                this.coreAttributes.put(id, value);
            } else {
                this.extendedAttributes.put(id, new ExtendedAttributeValue(value));
            }
        });
    }

    @Override
    public synchronized void refreshDerivedProperties() {
        this.type = SegmentType.fromAttributes(this.coreAttributes);
        if (this.type.intoAttributes(this.coreAttributes)) {
            log.info("{}: Updated Segment Type '{}' into Core Attributes.", this.traceObjectId, this.type);
        }

        this.attributeIdLength = (int) (long) this.coreAttributes.getOrDefault(Attributes.ATTRIBUTE_ID_LENGTH, -1L);
    }

    @Override
    public synchronized void copyFrom(SegmentMetadata base) {
        Exceptions.checkArgument(this.getId() == base.getId(), "base", "Given SegmentMetadata refers to a different StreamSegment than this one (SegmentId).");
        Exceptions.checkArgument(this.getName().equals(base.getName()), "base", "Given SegmentMetadata refers to a different StreamSegment than this one (SegmentName).");

        log.debug("{}: copyFrom {}.", this.traceObjectId, base.getClass().getSimpleName());
        setStorageLength(base.getStorageLength());
        setLength(base.getLength());

        // Update StartOffset after (potentially) updating the length, since he Start Offset must be less than or equal to Length.
        setStartOffset(base.getStartOffset());
        setLastModified(base.getLastModified());
        updateAttributes(base.getAttributes());
        refreshDerivedProperties();

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
            if (base.isDeletedInStorage()) {
                markDeletedInStorage();
            }
        }

        if (base.isPinned()) {
            markPinned();
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

    @Override
    public synchronized SegmentProperties getSnapshot() {
        return StreamSegmentInformation.from(this)
                .deletedInStorage(deletedInStorage)
                .sealedInStorage(sealedInStorage)
                .storageLength(storageLength)
                .attributes(new HashMap<>(getAttributes())).build();
    }

    @Override
    public synchronized boolean isPinned() {
        return this.pinned;
    }

    /**
     * Marks this SegmentMetadata as inactive.
     */
    @VisibleForTesting
    public synchronized void markInactive() {
        log.debug("{}: Inactive = true.", this.traceObjectId);
        this.active = false;
    }

    /**
     * Evicts those Extended Attributes from memory that have a LastUsed value prior to the given cutoff.
     *
     * @param maximumAttributeCount The maximum number of Extended Attributes per Segment. Cleanup will only be performed
     *                              if there are at least this many Extended Attributes in memory.
     * @param lastUsedCutoff        The cutoff value for LastUsed. Any Extended attributes with a value smaller than this
     *                              will be removed.
     * @return The number of removed attributes.
     */
    synchronized int cleanupAttributes(int maximumAttributeCount, long lastUsedCutoff) {
        if (this.extendedAttributes.size() <= maximumAttributeCount) {
            // Haven't reached the limit yet.
            return 0;
        }

        // Collect candidates and order them by lastUsed (oldest to newest).
        val candidates = this.extendedAttributes.entrySet().stream()
                                                .filter(e -> e.getValue().lastUsed < lastUsedCutoff)
                                                .sorted(Comparator.comparingLong(e -> e.getValue().lastUsed))
                                                .collect(Collectors.toList());

        // Start evicting candidates, beginning from the oldest, until we have either evicted all of them or brought the
        // total count to an acceptable limit.
        int count = 0;
        for (val e : candidates) {
            if (this.extendedAttributes.size() <= maximumAttributeCount) {
                break;
            }

            this.extendedAttributes.remove(e.getKey());
            count++;
        }

        log.debug("{}: Evicted {} attribute(s).", this.traceObjectId, count);
        return count;
    }

    //endregion

    //region ExtendedAttributeValue

    /**
     * Wrapper for the value of an Extended Attribute, which also keeps track of the last time this Attribute was used
     * (updated or retrieved).
     */
    private class ExtendedAttributeValue {
        private final long value;

        // We fetch the value from the SegmentMetadata's LastUsed field, which accurately keeps track of the Sequence Number
        // of the last Operation that either touched this Segment or requested information about it.
        @GuardedBy("StreamSegmentMetadata.this")
        private long lastUsed;

        ExtendedAttributeValue(long value) {
            this.value = value;
            this.lastUsed = StreamSegmentMetadata.this.getLastUsed();
        }

        @GuardedBy("StreamSegmentMetadata.this")
        long getValueAndTouch() {
            this.lastUsed = StreamSegmentMetadata.this.getLastUsed();
            return this.value;
        }

        @Override
        public String toString() {
            synchronized (StreamSegmentMetadata.this) {
                return String.format("%d (LastUsed=%d)", this.value, this.lastUsed);
            }
        }
    }

    //endregion

    //region AttributesView

    /**
     * Read-only view of this SegmentMetadata's Attributes (combines Core and Extended Attributes into one single Map).
     */
    private class AttributesView implements Map<AttributeId, Long> {
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
                        result = r.getValueAndTouch();
                    }
                }

                return result;
            }
        }

        @Override
        public Set<AttributeId> keySet() {
            synchronized (StreamSegmentMetadata.this) {
                return CollectionHelpers.joinSets(coreAttributes.keySet(), extendedAttributes.keySet());
            }
        }

        @Override
        public Collection<Long> values() {
            synchronized (StreamSegmentMetadata.this) {
                return CollectionHelpers.joinCollections(
                        coreAttributes.values(), Callbacks::identity,
                        extendedAttributes.values(), e -> e.value);
            }
        }

        @Override
        public Set<Map.Entry<AttributeId, Long>> entrySet() {
            synchronized (StreamSegmentMetadata.this) {
                return CollectionHelpers.joinSets(
                        coreAttributes.entrySet(), Callbacks::identity,
                        extendedAttributes.entrySet(), e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue().value));
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
        public Long put(AttributeId attributeId, Long aLong) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(Map<? extends AttributeId, ? extends Long> map) {
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
