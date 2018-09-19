/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeReference;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateByReference;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

/**
 * {@link DirectSegmentAccess} implementation that only handles attribute updates/retrievals and segment reads. This
 * accurately mocks the behavior of the entire Segment Container with respect to Attributes and reading, without dealing
 * with all the complexities behind the actual implementation.
 */
@ThreadSafe
@RequiredArgsConstructor
class SegmentMock implements DirectSegmentAccess {
    @GuardedBy("this")
    private final UpdateableSegmentMetadata metadata;
    @GuardedBy("this")
    private final EnhancedByteArrayOutputStream contents = new EnhancedByteArrayOutputStream();
    private final ScheduledExecutorService executor;

    SegmentMock(ScheduledExecutorService executor) {
        this(new StreamSegmentMetadata("Mock", 0, 0), executor);
    }

    long getTableNodeId() {
        synchronized (this) {
            return this.metadata.getAttributes().getOrDefault(Attributes.TABLE_NODE_ID, Attributes.NULL_ATTRIBUTE_VALUE);
        }
    }

    /**
     * Gets the number of non-deleted attributes.
     */
    int getAttributeCount() {
        return getAttributeCount((k, v) -> v != Attributes.NULL_ATTRIBUTE_VALUE);
    }

    /**
     * Gets the number of attributes that match the given filter.
     */
    int getAttributeCount(BiPredicate<UUID, Long> tester) {
        synchronized (this) {
            return (int) this.metadata.getAttributes().entrySet().stream().filter(e -> tester.test(e.getKey(), e.getValue())).count();
        }
    }

    @Override
    public CompletableFuture<Long> append(byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> {
            // Note that this append is not atomic (data & attributes) - but for testing purposes it does not matter as
            // this method should only be used for constucting the test data.
            long offset;
            synchronized (this) {
                offset = this.contents.size();
                this.contents.write(data);
                if (attributeUpdates != null) {
                    val updatedValues = new HashMap<UUID, Long>();
                    attributeUpdates.forEach(update -> collectAttributeValue(update, updatedValues));
                    this.metadata.updateAttributes(updatedValues);
                }

                this.metadata.setLength(this.contents.size());
            }
            return offset;
        }, this.executor);
    }

    @Override
    public ReadResult read(long offset, int maxLength, Duration timeout) {
        // We actually get a view of the data frozen in time, as any changes to the contents field after exiting from the
        // synchronized block may create a new buffer, but we don't care as the data we already have won't change.
        ByteArraySegment dataView;
        synchronized (this) {
            dataView = this.contents.getData();
        }

        // We get a slice of the data view, and return a ReadResultMock with entry lengths of 3.
        return new ReadResultMock(dataView.subSegment((int) offset, dataView.getLength() - (int) offset), maxLength, 3);
    }

    @Override
    public CompletableFuture<Map<UUID, Long>> getAttributes(Collection<UUID> attributeIds, boolean cache, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> {
            synchronized (this) {
                return attributeIds.stream()
                                   .collect(Collectors.toMap(id -> id, id -> this.metadata.getAttributes().getOrDefault(id, Attributes.NULL_ATTRIBUTE_VALUE)));
            }
        }, this.executor);
    }

    @Override
    public CompletableFuture<Void> updateAttributes(Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        return CompletableFuture.runAsync(() -> {
            synchronized (this) {
                val updatedValues = new HashMap<UUID, Long>();
                attributeUpdates.forEach(update -> collectAttributeValue(update, updatedValues));
                this.metadata.updateAttributes(updatedValues);
            }
        }, this.executor);
    }

    @GuardedBy("this")
    @SneakyThrows(BadAttributeUpdateException.class)
    private void collectAttributeValue(AttributeUpdate update, Map<UUID, Long> values) {
        if (update instanceof AttributeUpdateByReference) {
            AttributeUpdateByReference updateByRef = (AttributeUpdateByReference) update;
            AttributeReference<UUID> idRef = updateByRef.getIdReference();
            if (idRef != null) {
                updateByRef.setAttributeId(getReferenceValue(idRef, updateByRef));
            }

            AttributeReference<Long> valueRef = updateByRef.getValueReference();
            if (valueRef != null) {
                updateByRef.setValue(getReferenceValue(valueRef, updateByRef));
            }
        }

        long newValue = update.getValue();
        boolean hasValue = false;
        long previousValue = Attributes.NULL_ATTRIBUTE_VALUE;
        if (this.metadata.getAttributes().containsKey(update.getAttributeId())) {
            hasValue = true;
            previousValue = this.metadata.getAttributes().get(update.getAttributeId());
        }

        switch (update.getUpdateType()) {
            case ReplaceIfGreater:
                if (hasValue && newValue <= previousValue) {
                    throw new BadAttributeUpdateException("Segment", update, false, "GreaterThan");
                }

                break;
            case ReplaceIfEquals:
                if (update.getComparisonValue() != previousValue || !hasValue) {
                    throw new BadAttributeUpdateException("Segment", update, !hasValue,
                            String.format("ReplaceIfEquals (E=%s, A=%s)", previousValue, update.getComparisonValue()));
                }

                break;
            case None:
                if (hasValue) {
                    throw new BadAttributeUpdateException("Segment", update, false, "NoUpdate");
                }

                break;
            case Accumulate:
                if (hasValue) {
                    newValue += previousValue;
                    update.setValue(newValue);
                }

                break;
            case Replace:
                break;
            default:
                throw new BadAttributeUpdateException("Segment", update, !hasValue, "Unsupported");
        }

        values.put(update.getAttributeId(), update.getValue());
    }

    @GuardedBy("this")
    private <T> T getReferenceValue(AttributeReference<T> ref, AttributeUpdateByReference updateByRef) throws BadAttributeUpdateException {
        UUID attributeId = ref.getAttributeId();
        if (this.metadata.getAttributes().containsKey(attributeId)) {
            return ref.getTransformation().apply(this.metadata.getAttributes().get(attributeId));
        } else {
            throw new BadAttributeUpdateException("Segment", updateByRef, true, "AttributeRef");
        }
    }

    //region Unimplemented methods

    @Override
    public synchronized long getSegmentId() {
        return this.metadata.getId();
    }

    @Override
    public synchronized SegmentProperties getInfo() {
        return this.metadata.getSnapshot();
    }

    @Override
    public CompletableFuture<Long> seal(Duration timeout) {
        throw new UnsupportedOperationException("seal");
    }

    @Override
    public CompletableFuture<Void> truncate(long offset, Duration timeout) {
        throw new UnsupportedOperationException("offset");
    }

    //endregion
}
