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
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.val;

/**
 * {@link TableCompactor} for {@link HashTableSegmentLayout} implementations.
 */
class HashTableCompactor extends TableCompactor {
    //region Members

    private final KeyHasher hasher;
    private final IndexReader indexReader;

    //endregion

    //region Constructor

    HashTableCompactor(DirectSegmentAccess segment, Config config, @NonNull IndexReader indexReader, @NonNull KeyHasher hasher, ScheduledExecutorService executor) {
        super(segment, config, executor);
        this.hasher = hasher;
        this.indexReader = indexReader;
    }

    //endregion

    //region TableCompactor Implementation

    @Override
    protected long getLastIndexedOffset() {
        return IndexReader.getLastIndexedOffset(this.metadata);
    }

    @Override
    protected CompletableFuture<Long> getUniqueEntryCount() {
        return CompletableFuture.completedFuture(IndexReader.getEntryCount(this.metadata));
    }

    @Override
    protected IndexedCompactionArgs newCompactionArgs(long startOffset) {
        return new IndexedCompactionArgs(startOffset);
    }

    @Override
    protected CompletableFuture<Void> excludeObsolete(CompactionArgs args, TimeoutTimer timer) {
        val indexedArgs = (IndexedCompactionArgs) args;
        return this.indexReader
                .locateBuckets(this.segment, indexedArgs.candidatesByHash.keySet(), timer)
                .thenComposeAsync(buckets -> excludeObsolete(indexedArgs, buckets, timer), this.executor);
    }

    /**
     * Processes the given {@link CompactionArgs} and eliminates all {@link Candidate}s that meet at least one of the
     * following criteria:
     * - The Key's Table Bucket is no longer part of the index (removal)
     * - The Key exists in the Index, but the Index points to a newer version of it.
     *
     * @param args    A {@link CompactionArgs} representing the set of {@link Candidate}s for compaction. This set
     *                will be modified based on the outcome of this method.
     * @param buckets The Buckets retrieved via the {@link IndexReader} for the {@link Candidate}s.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation has finished.
     */
    private CompletableFuture<Void> excludeObsolete(IndexedCompactionArgs args, Map<UUID, TableBucket> buckets, TimeoutTimer timer) {
        // Exclude all those Table Entries whose buckets altogether do not exist.
        val deletedBuckets = args.candidatesByHash.keySet().stream()
                .filter(k -> {
                    val bucket = buckets.get(k);
                    return bucket == null || !bucket.exists();
                }).collect(Collectors.toList());

        // Do this in a separate loop since we are modifying args.candidatesByHash with removeBucket().
        for (val bucket : deletedBuckets) {
            args.removeBucket(bucket);
        }

        // For every Bucket that still exists, find all its Keys and match with our candidates and figure out if our
        // candidates are still eligible for compaction.
        val br = TableBucketReader.key(this.segment, this.indexReader::getBackpointerOffset, this.executor);
        val candidateBuckets = args.candidatesByHash.keySet().iterator();
        return Futures.loop(
                candidateBuckets::hasNext,
                () -> {
                    val bucketId = candidateBuckets.next();
                    long bucketOffset = buckets.get(bucketId).getSegmentOffset();
                    return br.findAll(bucketOffset, args::handleExistingKey, timer);
                },
                this.executor);
    }

    @Override
    protected int calculateTotalEntryDelta(CompactionArgs candidates) {
        return -candidates.getCount();
    }

    //endregion

    //region CompactionArgs Override

    private class IndexedCompactionArgs extends CompactionArgs {
        private final Map<UUID, Map<BufferView, Candidate>> candidatesByHash;

        IndexedCompactionArgs(long startOffset) {
            super(startOffset);
            this.candidatesByHash = new HashMap<>();
        }

        @Override
        boolean add(Candidate c) {
            boolean added = super.add(c);
            if (added) {
                val hash = hasher.hash(c.entry.getKey().getKey());
                val hashCandidates = this.candidatesByHash.computeIfAbsent(hash, h -> new HashMap<>());
                hashCandidates.put(c.entry.getKey().getKey(), c);
            }

            return added;
        }

        void removeBucket(UUID bucketId) {
            val candidates = this.candidatesByHash.remove(bucketId);
            if (candidates != null) {
                super.removeAll(candidates.values());
            }
        }
    }

    //endregion
}
