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
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

/**
 * {@link DirectSegmentAccess} implementation that only handles attribute updates/retrievals and segment reads. This
 * accurately mocks the behavior of the entire Segment Container with respect to Attributes and reading, without dealing
 * with all the complexities behind the actual implementation.
 */
@ThreadSafe
@RequiredArgsConstructor
class SegmentMock implements DirectSegmentAccess {
    @GuardedBy("this")
    private final HashMap<UUID, Long> attributes = new HashMap<>();
    private final EnhancedByteArrayOutputStream contents = new EnhancedByteArrayOutputStream();
    private final ScheduledExecutorService executor;

    long getTableNodeId() {
        synchronized (this) {
            return this.attributes.getOrDefault(Attributes.TABLE_NODE_ID, Attributes.NULL_ATTRIBUTE_VALUE);
        }
    }

    int getAttributeCount() {
        synchronized (this) {
            return this.attributes.size();
        }
    }

    int getAttributeCount(Predicate<UUID> tester) {
        synchronized (this) {
            return (int) this.attributes.keySet().stream().filter(tester).count();
        }
    }

    @Override
    public CompletableFuture<Long> append(byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> {
            // Note that this append is not atomic (data & attributes) - but for testing purposes it does not matter as
            // this method should only be used for constucting the test data.
            long offset = this.contents.size();
            this.contents.write(data);
            if (attributeUpdates != null) {
                attributeUpdates.forEach(this::updateAttribute);
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
                                   .collect(Collectors.toMap(id -> id, id -> this.attributes.getOrDefault(id, Attributes.NULL_ATTRIBUTE_VALUE)));
            }
        }, this.executor);
    }

    @Override
    public CompletableFuture<Void> updateAttributes(Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        return CompletableFuture.runAsync(() -> {
            synchronized (this.attributes) {
                attributeUpdates.forEach(this::updateAttribute);
            }
        }, this.executor);
    }

    @GuardedBy("this")
    @SneakyThrows(BadAttributeUpdateException.class)
    private void updateAttribute(AttributeUpdate update) {
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
        if (this.attributes.containsKey(update.getAttributeId())) {
            hasValue = true;
            previousValue = this.attributes.get(update.getAttributeId());
        }

        switch (update.getUpdateType()) {
            case ReplaceIfGreater:
                if (hasValue && newValue <= previousValue) {
                    throw new BadAttributeUpdateException("Segment", update, false, "GreaterThan");
                }

                break;
            case ReplaceIfEquals:
                if (update.getComparisonValue() != previousValue || !hasValue) {
                    throw new BadAttributeUpdateException("Segment", update, !hasValue, "ReplaceIfEquals");
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

        if (update.getValue() == Attributes.NULL_ATTRIBUTE_VALUE) {
            this.attributes.remove(update.getAttributeId());
        } else {
            this.attributes.put(update.getAttributeId(), update.getValue());
        }
    }

    @GuardedBy("this")
    private <T> T getReferenceValue(AttributeReference<T> ref, AttributeUpdateByReference updateByRef) throws BadAttributeUpdateException {
        UUID attributeId = ref.getAttributeId();
        if (this.attributes.containsKey(attributeId)) {
            return ref.getTransformation().apply(this.attributes.get(attributeId));
        } else {
            throw new BadAttributeUpdateException("Segment", updateByRef, true, "AttributeRef");
        }
    }

    //region Unimplemented methods

    @Override
    public long getSegmentId() {
        return 0;
    }

    @Override
    public SegmentProperties getInfo() {
        throw new UnsupportedOperationException("getInfo");
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
