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
package io.pravega.segmentstore.server.tables;

import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.AttributeIterator;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.shared.protocol.netty.WireCommands;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
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
    @Getter
    private final UpdateableSegmentMetadata metadata;
    @GuardedBy("this")
    private final ByteBufferOutputStream contents = new ByteBufferOutputStream();
    private final ScheduledExecutorService executor;
    @GuardedBy("this")
    private BiConsumer<Long, Integer> appendCallback;

    SegmentMock(ScheduledExecutorService executor) {
        this(new StreamSegmentMetadata("Mock", 0, 0), executor);
        this.metadata.setLength(0);
        this.metadata.setStorageLength(0);
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
    synchronized int getAttributeCount(BiPredicate<UUID, Long> tester) {
        return (int) this.metadata.getAttributes().entrySet().stream().filter(e -> tester.test(e.getKey(), e.getValue())).count();
    }

    /**
     * Sets a callback that will be invoked (synchronously) every time a successful call to {@link #append} completes.
     *
     * @param appendCallback The callback to register.
     */
    synchronized void setAppendCallback(BiConsumer<Long, Integer> appendCallback) {
        this.appendCallback = appendCallback;
    }

    @Override
    public CompletableFuture<Long> append(BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        // Similarly to the append below, we assume this is only for data construction, so offsets are not considered.
        return append(data, attributeUpdates, WireCommands.NULL_TABLE_SEGMENT_OFFSET, timeout);
    }

    @Override
    public CompletableFuture<Long> append(BufferView data, Collection<AttributeUpdate> attributeUpdates, long tableSegmentOffset, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> {
            // Note that this append is not atomic (data & attributes) - but for testing purposes it does not matter as
            // this method should only be used for constructing the test data.
            long offset;
            BiConsumer<Long, Integer> appendCallback;
            synchronized (this) {
                offset = this.contents.size();
                try {
                    data.copyTo(this.contents);
                } catch (IOException ex) {
                    throw new CompletionException(ex);
                }
                if (attributeUpdates != null) {
                    val updatedValues = new HashMap<UUID, Long>();
                    attributeUpdates.forEach(update -> collectAttributeValue(update, updatedValues));
                    this.metadata.updateAttributes(updatedValues);
                }

                this.metadata.setLength(this.contents.size());
                appendCallback = this.appendCallback;
            }

            if (appendCallback != null) {
                appendCallback.accept(offset, data.getLength());
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
        return new TruncateableReadResultMock(offset, dataView.slice((int) offset, dataView.getLength() - (int) offset), maxLength, 3);
    }

    @Override
    public CompletableFuture<Void> truncate(long offset, Duration timeout) {
        return CompletableFuture.runAsync(() -> {
            synchronized (this) {
                this.metadata.setStartOffset(offset);
            }
        }, this.executor);
    }

    @Override
    public CompletableFuture<Map<UUID, Long>> getAttributes(Collection<UUID> attributeIds, boolean cache, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> {
            synchronized (this) {
                return attributeIds.stream()
                        .distinct()
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

    synchronized void updateAttributes(Map<UUID, Long> attributeValues) {
        this.metadata.updateAttributes(attributeValues);
    }

    @Override
    public CompletableFuture<AttributeIterator> attributeIterator(UUID fromId, UUID toId, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> new AttributeIteratorImpl(this.metadata, fromId, toId), this.executor);
    }

    @GuardedBy("this")
    @SneakyThrows(BadAttributeUpdateException.class)
    private void collectAttributeValue(AttributeUpdate update, Map<UUID, Long> values) {
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

    //region Unimplemented methods

    @Override
    public synchronized long getSegmentId() {
        return this.metadata.getId();
    }

    @Override
    public synchronized SegmentProperties getInfo() {
        return this.metadata;
    }

    @Override
    public CompletableFuture<Long> seal(Duration timeout) {
        throw new UnsupportedOperationException("seal");
    }

    //endregion

    //region AttributeIterator

    private class AttributeIteratorImpl implements AttributeIterator {
        private final int maxBatchSize = 5;
        @GuardedBy("attributes")
        private final ArrayDeque<Map.Entry<UUID, Long>> attributes;

        AttributeIteratorImpl(SegmentMetadata metadata, UUID fromId, UUID toId) {
            this.attributes = metadata
                    .getAttributes().entrySet().stream()
                    .filter(e -> fromId.compareTo(e.getKey()) <= 0 && toId.compareTo(e.getKey()) >= 0)
                    .sorted(Comparator.comparing(Map.Entry::getKey, UUID::compareTo))
                    .collect(Collectors.toCollection(ArrayDeque::new));
        }

        @Override
        public CompletableFuture<List<Map.Entry<UUID, Long>>> getNext() {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.attributes) {
                    val result = new ArrayList<Map.Entry<UUID, Long>>();
                    while (!this.attributes.isEmpty() && result.size() < maxBatchSize) {
                        result.add(this.attributes.removeFirst());
                    }

                    return result.isEmpty() ? null : result;
                }
            }, executor);
        }
    }

    //endregion

    //region TruncateableReadResultMock

    private class TruncateableReadResultMock extends ReadResultMock {
        private TruncateableReadResultMock(long streamSegmentStartOffset, ArrayView data, int maxResultLength, int entryLength) {
            super(streamSegmentStartOffset, data, maxResultLength, entryLength);
        }

        @Override
        protected long getSegmentStartOffset() {
            synchronized (SegmentMock.this) {
                return SegmentMock.this.metadata.getStartOffset();
            }
        }
    }

    //endregion
}
