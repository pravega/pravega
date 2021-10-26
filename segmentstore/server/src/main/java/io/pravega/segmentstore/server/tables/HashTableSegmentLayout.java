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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Runnables;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.IllegalDataFormatException;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.IteratorState;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.contracts.tables.TableSegmentInfo;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Hash-based Table Segment Layout.
 *
 * NOTE: this is the original Table Segment Implementation, which is currently used by all Segment Store and Controller
 * Metadata. As such, it must be made backwards compatible for all Pravega releases prior to 0.10.0.
 *
 * This Layout supports Variable-length Keys (up to 8KB), but there is no order between keys.
 */
class HashTableSegmentLayout extends TableSegmentLayout {
    //region Members

    private final KeyHasher hasher;
    private final ContainerKeyIndex keyIndex;

    //endregion

    //region Constructor

    HashTableSegmentLayout(Connector connector, @NonNull CacheManager cacheManager, KeyHasher hasher,
                           TableExtensionConfig config, ScheduledExecutorService executorService) {
        super(connector, config, executorService);
        this.hasher = hasher;
        this.keyIndex = new ContainerKeyIndex(connector.getContainerId(), config, cacheManager, this.hasher, this.executor);
    }

    //endregion

    //region TableSegmentLayout Implementation

    @Override
    public void close() {
        this.keyIndex.close();
    }

    @Override
    Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata) {
        ensureSegmentType(metadata.getName(), metadata.getType());
        return Collections.singletonList(new WriterTableProcessor(new TableWriterConnectorImpl(metadata), this.executor));
    }

    @Override
    Map<AttributeId, Long> getNewSegmentAttributes(@NonNull TableSegmentConfig config) {
        Preconditions.checkArgument(config.getKeyLength() == 0, "Segment KeyLength must be 0 for HashTableSegments; actual %s.", config.getKeyLength());
        val result = new HashMap<AttributeId, Long>();
        result.putAll(this.config.getDefaultCompactionAttributes());
        if (config.getRolloverSizeBytes() > 0) {
            result.put(Attributes.ROLLOVER_SIZE, config.getRolloverSizeBytes());
        }
        return result;
    }

    @Override
    CompletableFuture<Void> deleteSegment(@NonNull String segmentName, boolean mustBeEmpty, Duration timeout) {
        if (mustBeEmpty) {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.connector.getSegment(segmentName, timer.getRemaining())
                    .thenComposeAsync(segment -> this.keyIndex.executeIfEmpty(segment,
                            () -> this.connector.deleteSegment(segmentName, timer.getRemaining()), timer),
                            this.executor);
        }

        return this.connector.deleteSegment(segmentName, timeout);
    }

    @Override
    CompletableFuture<List<Long>> put(@NonNull DirectSegmentAccess segment, @NonNull List<TableEntry> entries, long tableSegmentOffset, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());

        // Generate an Update Batch for all the entries (since we need to know their Key Hashes and relative
        // offsets in the batch itself).
        val updateBatch = batch(entries, TableEntry::getKey, this.serializer::getUpdateLength, TableKeyBatch.update());
        logRequest("put", segmentInfo.getName(), updateBatch.isConditional(), tableSegmentOffset, entries.size(), updateBatch.getLength());
        return this.keyIndex.update(segment, updateBatch,
                () -> commit(entries, this.serializer::serializeUpdate, segment, tableSegmentOffset, timer.getRemaining()), timer);
    }

    @Override
    CompletableFuture<Void> remove(@NonNull DirectSegmentAccess segment, @NonNull Collection<TableKey> keys, long tableSegmentOffset, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val removeBatch = batch(keys, key -> key, this.serializer::getRemovalLength, TableKeyBatch.removal());
        logRequest("remove", segmentInfo.getName(), removeBatch.isConditional(), removeBatch.isRemoval(),
                keys.size(), removeBatch.getLength());
        return this.keyIndex.update(segment, removeBatch,
                () -> commit(keys, this.serializer::serializeRemoval, segment, tableSegmentOffset, timer.getRemaining()), timer)
                .thenRun(Runnables.doNothing());
    }

    @Override
    CompletableFuture<List<TableEntry>> get(@NonNull DirectSegmentAccess segment, @NonNull List<BufferView> keys, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        logRequest("get", segmentInfo.getName(), keys.size());
        val resultBuilder = new GetResultBuilder(keys, this.hasher);
        return this.keyIndex.getBucketOffsets(segment, resultBuilder.getHashes(), timer)
                .thenComposeAsync(offsets -> get(segment, resultBuilder, offsets, timer), this.executor);
    }

    private CompletableFuture<List<TableEntry>> get(DirectSegmentAccess segment, GetResultBuilder builder,
                                                    Map<UUID, Long> bucketOffsets, TimeoutTimer timer) {
        val bucketReader = TableBucketReader.entry(segment, this.keyIndex::getBackpointerOffset, this.executor);
        int resultSize = builder.getHashes().size();
        for (int i = 0; i < resultSize; i++) {
            UUID keyHash = builder.getHashes().get(i);
            long offset = bucketOffsets.get(keyHash);
            if (offset == TableKey.NOT_EXISTS) {
                // Bucket does not exist, hence neither does the key.
                builder.includeResult(CompletableFuture.completedFuture(null));
            } else {
                // Find the sought entry in the segment, based on its key.
                BufferView key = builder.getKeys().get(i);
                builder.includeResult(this.keyIndex.findBucketEntry(segment, bucketReader, key, offset, timer).thenApply(this::maybeDeleted));
            }
        }

        return builder.getResultFutures();
    }

    @Override
    CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        logRequest("keyIterator", segmentInfo.getName(), args);
        return newIterator(segment, args, TableBucketReader::key);
    }

    @Override
    CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        logRequest("entryIterator", segmentInfo.getName(), args);
        return newIterator(segment, args, TableBucketReader::entry);
    }

    @Override
    AsyncIterator<IteratorItem<TableEntry>> entryDeltaIterator(@NonNull DirectSegmentAccess segment, long fromPosition, Duration fetchTimeout) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        Preconditions.checkArgument(fromPosition <= segmentInfo.getLength(), "fromPosition (%s) can not exceed the length (%s) of the TableSegment.",
                fromPosition, segmentInfo.getLength());

        logRequest("entryDeltaIterator", segment.getSegmentId(), fromPosition);

        long compactionOffset = segmentInfo.getAttributes().getOrDefault(TableAttributes.COMPACTION_OFFSET, 0L);
        // All of the most recent keys will exist beyond the compactionOffset.
        long startOffset = Math.max(fromPosition, compactionOffset);
        // We should clear if the starting position may have been truncated out due to compaction.
        boolean shouldClear = fromPosition < compactionOffset;
        // Maximum length of the TableSegment we want to read until.
        int maxBytesToRead = (int) (segmentInfo.getLength() - startOffset);
        TableEntryDeltaIterator.ConvertResult<IteratorItem<TableEntry>> converter = item ->
                CompletableFuture.completedFuture(new IteratorItemImpl<TableEntry>(
                        item.getKey().serialize(),
                        Collections.singletonList(item.getValue())));
        return TableEntryDeltaIterator.<IteratorItem<TableEntry>>builder()
                .segment(segment)
                .entrySerializer(serializer)
                .executor(executor)
                .maxBytesToRead(maxBytesToRead)
                .startOffset(startOffset)
                .currentBatchOffset(fromPosition)
                .fetchTimeout(fetchTimeout)
                .resultConverter(converter)
                .shouldClear(shouldClear)
                .build();
    }

    @Override
    CompletableFuture<TableSegmentInfo> getInfo(@NonNull DirectSegmentAccess segment, Duration timeout) {
        val m = segment.getInfo();
        return CompletableFuture.completedFuture(TableSegmentInfo.builder()
                .name(m.getName())
                .length(m.getLength())
                .startOffset(m.getStartOffset())
                .type(m.getType())
                .entryCount(this.keyIndex.getUniqueEntryCount(m))
                .keyLength(0) // Variable key length.
                .build());
    }

    private <T> CompletableFuture<AsyncIterator<IteratorItem<T>>> newIterator(@NonNull DirectSegmentAccess segment, @NonNull IteratorArgs args,
                                                                              @NonNull GetBucketReader<T> createBucketReader) {
        Preconditions.checkArgument(args.getFrom() == null && args.getTo() == null, "Range Iterators not supported for HashTableSegments.");
        UUID fromHash;
        BufferView serializedState = args.getContinuationToken();
        try {
            fromHash = KeyHasher.getNextHash(serializedState == null ? null : IteratorStateImpl.deserialize(serializedState).getKeyHash());
        } catch (IOException ex) {
            // Bad IteratorState serialization.
            throw new IllegalDataFormatException("Unable to deserialize `serializedState`.", ex);
        }

        if (fromHash == null) {
            // Nothing to iterate on.
            return CompletableFuture.completedFuture(TableIterator.empty());
        }

        // Create a converter that will use a TableBucketReader to fetch all requested items in the iterated Buckets.
        val bucketReader = createBucketReader.apply(segment, this.keyIndex::getBackpointerOffset, this.executor);
        TableIterator.ConvertResult<IteratorItem<T>> converter = bucket ->
                bucketReader.findAllExisting(bucket.getSegmentOffset(), new TimeoutTimer(args.getFetchTimeout()))
                        .thenApply(result -> new IteratorItemImpl<>(new IteratorStateImpl(bucket.getHash()).serialize(), result));

        // Fetch the Tail (Unindexed) Hashes, then create the TableIterator.
        return this.keyIndex.getUnindexedKeyHashes(segment)
                .thenComposeAsync(cacheHashes -> TableIterator.<IteratorItem<T>>builder()
                        .segment(segment)
                        .cacheHashes(cacheHashes)
                        .firstHash(fromHash)
                        .executor(executor)
                        .resultConverter(converter)
                        .fetchTimeout(args.getFetchTimeout())
                        .build(), this.executor);
    }

    private <T> TableKeyBatch batch(Collection<T> toBatch, Function<T, TableKey> getKey, Function<T, Integer> getLength, TableKeyBatch batch) {
        for (T item : toBatch) {
            val length = getLength.apply(item);
            val key = getKey.apply(item);
            batch.add(key, this.hasher.hash(key.getKey()), length);
        }

        if (batch.getLength() > this.config.getMaxBatchSize()) {
            throw new UpdateBatchTooLargeException(batch.getLength(), this.config.getMaxBatchSize());
        }
        return batch;
    }

    private <T> CompletableFuture<Long> commit(Collection<T> toCommit, Function<Collection<T>, BufferView> serializer,
                                               DirectSegmentAccess segment, long tableSegmentOffset, Duration timeout) {
        BufferView s = serializer.apply(toCommit);
        if (tableSegmentOffset == NO_OFFSET) {
            return segment.append(s, null, timeout);
        } else {
            return segment.append(s, null, tableSegmentOffset, timeout);
        }
    }

    private void ensureSegmentType(String segmentName, SegmentType segmentType) {
        Preconditions.checkArgument(segmentType.isTableSegment() && !segmentType.isFixedKeyLengthTableSegment(),
                "HashTableSegment can only be used for variable-key Table Segments; Segment '%s' is '%s;.", segmentName, segmentType);
    }

    //endregion

    //region TableWriterConnector

    @RequiredArgsConstructor
    private class TableWriterConnectorImpl implements TableWriterConnector {
        @Getter
        private final SegmentMetadata metadata;

        @Override
        public EntrySerializer getSerializer() {
            return HashTableSegmentLayout.this.serializer;
        }

        @Override
        public KeyHasher getKeyHasher() {
            return HashTableSegmentLayout.this.hasher;
        }

        @Override
        public CompletableFuture<DirectSegmentAccess> getSegment(Duration timeout) {
            return HashTableSegmentLayout.this.connector.getSegment(this.metadata.getName(), OperationPriority.Critical, timeout);
        }

        @Override
        public void notifyIndexOffsetChanged(long lastIndexedOffset, int processedSizeBytes) {
            HashTableSegmentLayout.this.keyIndex.notifyIndexOffsetChanged(this.metadata.getId(), lastIndexedOffset, processedSizeBytes);
        }

        @Override
        public int getMaxCompactionSize() {
            return HashTableSegmentLayout.this.config.getMaxCompactionSize();
        }

        @Override
        public void close() {
            // Tell the KeyIndex that it's ok to clear any tail-end cache.
            HashTableSegmentLayout.this.keyIndex.notifyIndexOffsetChanged(this.metadata.getId(), -1L, 0);
        }
    }

    //endregion

    //region GetResultBuilder

    /**
     * Helps build Result for the {@link #get} method.
     */
    private static class GetResultBuilder {
        /**
         * Sought keys.
         */
        @Getter
        private final List<BufferView> keys;
        /**
         * Sought keys's hashes, in the same order as the keys.
         */
        @Getter
        private final List<UUID> hashes;

        /**
         * A list of Futures with the results for each key, in the same order as the keys.
         */
        private final List<CompletableFuture<TableEntry>> resultFutures;

        GetResultBuilder(List<BufferView> keys, KeyHasher hasher) {
            this.keys = keys;
            this.hashes = keys.stream().map(hasher::hash).collect(Collectors.toList());
            this.resultFutures = new ArrayList<>();
        }

        void includeResult(CompletableFuture<TableEntry> entryFuture) {
            this.resultFutures.add(entryFuture);
        }

        CompletableFuture<List<TableEntry>> getResultFutures() {
            return Futures.allOfWithResults(this.resultFutures);
        }
    }

    //endregion

    //region GetBucketReader

    @FunctionalInterface
    private interface GetBucketReader<T> {
        TableBucketReader<T> apply(DirectSegmentAccess segment, TableBucketReader.GetBackpointer getBackpointer, ScheduledExecutorService executor);
    }

    //endregion

    //region IteratorState

    /**
     * Represents the state of a resumable iterator for use with {@link HashTableSegmentLayout}.
     */
    public static class IteratorStateImpl implements IteratorState {
        private static final Serializer SERIALIZER = new Serializer();

        /**
         * Gets the Key Hash of the last TableBucket contained in the iteration so far. When sorted lexicographically, all
         * TableBuckets with Hashes smaller than or equal to this one have been included.
         */
        @Getter
        @NonNull
        private final UUID keyHash;

        /**
         * Creates a new instance of the IteratorState class.
         *
         * @param keyHash The Key Hash to use.
         */
        IteratorStateImpl(@NonNull UUID keyHash) {
            Preconditions.checkArgument(KeyHasher.isValid(keyHash), "keyHash must be at least IteratorState.MIN_HASH and at most IteratorState.MAX_HASH.");
            this.keyHash = keyHash;
        }

        @Override
        public String toString() {
            return String.format("Hash = %s", this.keyHash);
        }

        //region Serialization

        /**
         * Creates a new instance of the IteratorState class from the given array.
         *
         * @param data The serialization of an IteratorState. This must have been generated using {@link #serialize()}.
         * @return As new instance of the IteratorState class.
         * @throws IOException If unable to deserialize.
         */
        static IteratorStateImpl deserialize(BufferView data) throws IOException {
            return SERIALIZER.deserialize(data);
        }

        /**
         * Serializes this IteratorState instance into an {@link ArrayView}.
         *
         * @return The {@link ArrayView} that was used for serialization.
         */
        @Override
        @SneakyThrows(IOException.class)
        public ArrayView serialize() {
            return SERIALIZER.serialize(this);
        }

        private static class IteratorStateBuilder implements ObjectBuilder<IteratorStateImpl> {
            private UUID keyHash;

            @Override
            public IteratorStateImpl build() {
                return new IteratorStateImpl(keyHash);
            }
        }

        private static class Serializer extends VersionedSerializer.WithBuilder<IteratorStateImpl, IteratorStateBuilder> {
            @Override
            protected IteratorStateBuilder newBuilder() {
                return new IteratorStateBuilder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput revisionDataInput, IteratorStateBuilder builder) throws IOException {
                builder.keyHash = revisionDataInput.readUUID();
            }

            private void write00(IteratorStateImpl state, RevisionDataOutput revisionDataOutput) throws IOException {
                revisionDataOutput.length(RevisionDataOutput.UUID_BYTES);
                revisionDataOutput.writeUUID(state.keyHash);
            }
        }

        //endregion
    }

    //endregion
}
