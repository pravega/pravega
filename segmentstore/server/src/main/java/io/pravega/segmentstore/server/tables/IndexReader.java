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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Provides read-only access to a Hash Array Mapped Tree implementation over Extended Attributes.
 *
 * Index Structure:
 * - The index is organized as a collection of {@link TableBucket} instances.
 * - Each {@link TableBucket} corresponds to exactly one Key Hash, and may contain one or more entries.
 * - A Table Key may occur at most once in a {@link TableBucket}.
 * - Multiple Table Keys in the same {@link TableBucket} are linked by means of Backpointers.
 * - Both {@link TableBucket}s and Backpointers are stored in the Segment's Extended Attributes, as described below.
 *
 * {@link TableBucket} Extended Attribute Implementation:
 * - The Key Hash (a {@link UUID}) is used as a direct Extended Attribute entry. The {@link KeyHasher}, when generating
 * this value, resolves any collisions with core attributes or Backpointer attributes.
 * - The value of this attribute is the Segment Offset of the Table Entry with the highest offset of the {@link TableBucket}.
 *
 * Backpointers Extended Attribute Implementation:
 * - Backpointers are used to resolve collisions for those Table Entries whose Keys have the same Key Hash.
 * - Backpointers are links from the Offset of a Table Entry to the Offset of a previous Table Entry in the same {@link TableBucket}.
 * -- They begin from the {@link TableBucket}'s SegmentOffset and are chained up until there are no more Table Entries left.
 */
@RequiredArgsConstructor
class IndexReader {
    //region Members

    @NonNull
    protected final ScheduledExecutorService executor;

    //endregion

    //region Index Access Operations

    /**
     * Gets the offset (from the given {@link SegmentProperties}'s Attributes up to which all Table Entries have been indexed.
     *
     * @param segmentInfo A {@link SegmentProperties} from which to extract the information.
     * @return The offset.
     */
    static long getLastIndexedOffset(SegmentProperties segmentInfo) {
        return segmentInfo.getAttributes().getOrDefault(TableAttributes.INDEX_OFFSET, 0L);
    }

    /**
     * Gets the number of Table Entries indexed in the Segment for the given {@link SegmentProperties}.
     *
     * @param segmentInfo A {@link SegmentProperties} from which to extract the information.
     * @return The count.
     */
    static long getEntryCount(SegmentProperties segmentInfo) {
        return segmentInfo.getAttributes().getOrDefault(TableAttributes.ENTRY_COUNT, 0L);
    }

    /**
     * Gets the total number of Table Entries in the Segment for the given {@link SegmentProperties}. This includes Entries
     * that are no longer active (which have been replaced by other entries with the same Table Key).
     *
     * @param segmentInfo A {@link SegmentProperties} from which to extract the information.
     * @return The count.
     */
    static long getTotalEntryCount(SegmentProperties segmentInfo) {
        return segmentInfo.getAttributes().getOrDefault(TableAttributes.TOTAL_ENTRY_COUNT, 0L);
    }

    /**
     * Gets the number of Table Buckets in the Segment for the given {@link SegmentProperties}.
     *
     * @param segmentInfo A {@link SegmentProperties} from which to extract the information.
     * @return The count.
     */
    static long getBucketCount(SegmentProperties segmentInfo) {
        return segmentInfo.getAttributes().getOrDefault(TableAttributes.BUCKET_COUNT, 0L);
    }

    /**
     * Gets the index in the Segment until which compaction has previously run.
     *
     * @param segmentInfo A {@link SegmentProperties} from which to extract the information.
     * @return The index.
     */
    static long getCompactionOffset(SegmentProperties segmentInfo) {
        return segmentInfo.getAttributes().getOrDefault(TableAttributes.COMPACTION_OFFSET, 0L);
    }

    /**
     * Gets the Segment Utilization (as a percentage) under which a compaction may be required.
     *
     * @param segmentInfo A {@link SegmentProperties} from which to extract the information.
     * @return The result.
     */
    static long getCompactionUtilizationThreshold(SegmentProperties segmentInfo) {
        return segmentInfo.getAttributes().getOrDefault(TableAttributes.MIN_UTILIZATION, 0L);
    }

    /**
     * Locates the {@link TableBucket}s for the given Key Hashes in the given Segment's Extended Attribute Index.
     *
     * @param segment   A {@link DirectSegmentAccess} providing access to the Segment to look into.
     * @param keyHashes A Collection of Key Hashes to look up {@link TableBucket}s for.
     * @param timer     Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the requested Bucket information.
     */
    CompletableFuture<Map<UUID, TableBucket>> locateBuckets(DirectSegmentAccess segment, Collection<UUID> keyHashes, TimeoutTimer timer) {
        val attributeIds = keyHashes.stream().map(AttributeId::fromUUID).collect(Collectors.toList());
        return segment
                .getAttributes(attributeIds, false, timer.getRemaining())
                .thenApply(attributes -> attributes.entrySet().stream()
                        .map(e -> new TableBucket(((AttributeId.UUID) e.getKey()).toUUID(), e.getValue())) // These should always be UUID.
                        .collect(Collectors.toMap(TableBucket::getHash, b -> b)));
    }

    /**
     * Looks up a Backpointer offset.
     *
     * @param segment A DirectSegmentAccess providing access to the Segment to search in.
     * @param offset  The offset to find a backpointer from.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the backpointer offset, or -1 if no such pointer exists.
     */
    CompletableFuture<Long> getBackpointerOffset(DirectSegmentAccess segment, long offset, Duration timeout) {
        AttributeId key = getBackpointerAttributeKey(offset);
        return segment.getAttributes(Collections.singleton(key), false, timeout)
                      .thenApply(attributes -> {
                          long result = attributes.getOrDefault(key, Attributes.NULL_ATTRIBUTE_VALUE);
                          return result == Attributes.NULL_ATTRIBUTE_VALUE ? -1 : result;
                      });
    }

    /**
     * Gets the offsets for all the Table Entries in the given {@link TableBucket}.
     *
     * @param segment A {@link DirectSegmentAccess}
     * @param bucket  The {@link TableBucket} to get offsets for.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain a List of offsets, with the first offset being the
     * {@link TableBucket}'s offset itself, then descending down in the order of backpointers. If the {@link TableBucket}
     * is partial, then the list will be empty.
     */
    @VisibleForTesting
    CompletableFuture<List<Long>> getBucketOffsets(DirectSegmentAccess segment, TableBucket bucket, TimeoutTimer timer) {
        val result = new ArrayList<Long>();
        AtomicLong offset = new AtomicLong(bucket.getSegmentOffset());
        return Futures.loop(
                () -> offset.get() >= 0,
                () -> {
                    result.add(offset.get());
                    return getBackpointerOffset(segment, offset.get(), timer.getRemaining());
                },
                offset::set,
                this.executor)
                      .thenApply(v -> result);
    }

    /**
     * Generates a 16-byte AttributeId that encodes the given Offset as a Backpointer (to some other offset).
     * Format {0(64)}{Offset}
     * - MSB is 0
     * - LSB is Offset.
     *
     * @param offset The offset to generate a backpointer from.
     * @return An AttributeId representing the Attribute Key.
     */
    protected AttributeId getBackpointerAttributeKey(long offset) {
        Preconditions.checkArgument(offset >= 0, "offset must be a non-negative number.");
        return AttributeId.uuid(TableBucket.BACKPOINTER_PREFIX, offset);
    }

    /**
     * Determines if the given Attribute Key is Backpointer.
     *
     * @param key The Key to test.
     * @return True if backpointer, false otherwise.
     */
    @VisibleForTesting
    static boolean isBackpointerAttributeKey(AttributeId key) {
        return key.getBitGroup(0) == TableBucket.BACKPOINTER_PREFIX;
    }

    //endregion
}