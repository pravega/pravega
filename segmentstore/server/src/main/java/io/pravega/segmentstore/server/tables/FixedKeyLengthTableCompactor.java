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

import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.Retry;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.DynamicAttributeUpdate;
import io.pravega.segmentstore.contracts.DynamicAttributeValue;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.val;

/**
 * {@link TableCompactor} for {@link FixedKeyLengthTableSegmentLayout} implementations.
 */
class FixedKeyLengthTableCompactor extends TableCompactor {
    private static final Retry.RetryAndThrowBase<Exception> RETRY_POLICY = Retry.withExpBackoff(1, 5, 5)
            .retryingOn(BadAttributeUpdateException.class)
            .throwingOn(Exception.class);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    //region Constructor

    FixedKeyLengthTableCompactor(@NonNull DirectSegmentAccess segment, @NonNull Config config, @NonNull ScheduledExecutorService executor) {
        super(segment, config, executor);
    }

    //endregion

    //region TableCompactor Implementation

    @Override
    protected long getLastIndexedOffset() {
        return this.metadata.getLength(); // Segment is always fully indexed.
    }

    @Override
    protected CompletableFuture<Long> getUniqueEntryCount() {
        return this.segment.getExtendedAttributeCount(DEFAULT_TIMEOUT);
    }

    @Override
    protected CompletableFuture<Void> excludeObsolete(CompactionArgs args, TimeoutTimer timer) {
        val byAttributeId = args.getAll().stream()
                .collect(Collectors.toMap(c -> AttributeId.from(c.entry.getKey().getKey().getCopy()), c -> c));
        return this.segment.getAttributes(byAttributeId.keySet(), false, timer.getRemaining())
                .thenAcceptAsync(attributeValues -> excludeObsolete(args, byAttributeId, attributeValues), this.executor);
    }

    private void excludeObsolete(CompactionArgs args, Map<AttributeId, Candidate> candidatesByAttributeId, Map<AttributeId, Long> indexValues) {
        // Exclude all those Table Entries which no longer exist (they have been deleted).
        val entryIterator = candidatesByAttributeId.entrySet().iterator();
        while (entryIterator.hasNext()) {
            val e = entryIterator.next();
            val exists = indexValues.getOrDefault(e.getKey(), Attributes.NULL_ATTRIBUTE_VALUE) != Attributes.NULL_ATTRIBUTE_VALUE;
            if (!exists) {
                args.remove(e.getValue());
                entryIterator.remove();
            }
        }

        // For all those Keys that still exist, figure out if our candidates are still eligible for compaction. They are
        // not eligible if the same Key currently exists in the index but with a higher offset.
        for (val e : indexValues.entrySet()) {
            if (e.getValue() != Attributes.NULL_ATTRIBUTE_VALUE) {
                val existingKey = TableKey.versioned(e.getKey().toBuffer(), e.getValue());
                args.handleExistingKey(existingKey, e.getValue());
            }
        }
    }

    @Override
    protected int calculateTotalEntryDelta(CompactionArgs candidates) {
        int delta = candidates.getCount() - candidates.getCopyCandidateCount();
        assert delta >= 0 : "compaction cannot increase total entry count";
        return -delta;
    }

    @Override
    protected void generateIndexUpdates(Candidate c, int batchOffset, AttributeUpdateCollection indexUpdates) {
        // For each Candidate we're about to copy, create an index update that:
        // - Updates the Index Key Conditionally to point to the new location of the Key/Entry.
        // - The new location is dynamically calculated based on the append offset + offset within the append (batchOffset)
        // - The condition is that the Key we're about to copy has not changed since we read it (so we condition on the
        // index entry value pointing to its original Segment Offset.
        val au = new DynamicAttributeUpdate(AttributeId.from(c.entry.getKey().getKey().getCopy()),
                AttributeUpdateType.ReplaceIfEquals,
                DynamicAttributeValue.segmentLength(batchOffset),
                c.segmentOffset);
        indexUpdates.add(au);
    }

    @Override
    protected Retry.RetryAndThrowBase<Exception> getRetryPolicy() {
        return RETRY_POLICY;
    }

    //endregion
}
