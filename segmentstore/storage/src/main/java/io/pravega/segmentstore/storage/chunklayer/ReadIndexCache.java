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
package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import static io.pravega.shared.MetricsNames.SLTS_READ_INDEX_CHUNK_INDEX_SIZE;
import static io.pravega.shared.MetricsNames.SLTS_READ_INDEX_SEGMENT_INDEX_SIZE;
import static io.pravega.shared.MetricsNames.SLTS_READ_INDEX_SEGMENT_MISS_RATE;

/**
 * An in-memory implementation of cache for read index that maps chunk start offset to chunk name for recently used segments.
 * The least accessed segments are removed entirely as well as removing chunks that are least recently used.
 */
class ReadIndexCache implements StatsReporter {
    /**
     * Keeps track of all per segment ReadIndex.
     */
    @Getter
    private final Cache<String, SegmentReadIndex> segmentsReadIndexCache;

    /**
     * Keeps track of all read index entries.
     */
    @Getter
    private final Cache<IndexEntry, Boolean> indexEntryCache;

    /**
     * Constructor.
     *
     * @param maxIndexedSegments Max number of cached indexed segments.
     * @param maxIndexedChunks   Max number of cached indexed chunks.
     */
    public ReadIndexCache(int maxIndexedSegments, int maxIndexedChunks) {
        Preconditions.checkArgument(maxIndexedSegments >= 0, "maxIndexedSegments must be non negative");
        Preconditions.checkArgument(maxIndexedChunks >= 0, "maxIndexedChunks must be non negative");

        segmentsReadIndexCache = CacheBuilder.newBuilder()
                .maximumSize(maxIndexedSegments)
                .removalListener(this::removeSegment)
                .build();
        indexEntryCache = CacheBuilder.newBuilder()
                .maximumSize(maxIndexedChunks)
                .removalListener(this::removeChunk)
                .build();
    }

    /**
     * Retrieves the read index for given segment.
     *
     * @param streamSegmentName Name of the segment.
     * @return Read index corresponding to the given segment. A new empty index is created if it doesn't already exist.
     */
    private SegmentReadIndex getSegmentReadIndex(String streamSegmentName, boolean createIfNotPresent) {
        SegmentReadIndex readIndex = segmentsReadIndexCache.getIfPresent(streamSegmentName);
        if (null == readIndex && createIfNotPresent) {
            synchronized (segmentsReadIndexCache) {
                // Some other thread may have added the index.
                readIndex = segmentsReadIndexCache.getIfPresent(streamSegmentName);
                if (null == readIndex) {
                    readIndex = new SegmentReadIndex();
                    segmentsReadIndexCache.put(streamSegmentName, readIndex);
                }
            }
        }

        return readIndex;
    }

    /**
     * Updates read index for given segment with new entries.
     *
     * @param streamSegmentName   Name of the segment.
     * @param newReadIndexEntries List of {@link ChunkNameOffsetPair} for new entries.
     */
    public void addIndexEntries(String streamSegmentName, List<ChunkNameOffsetPair> newReadIndexEntries) {
        Preconditions.checkArgument(null != streamSegmentName, "streamSegmentName must not be null");
        Preconditions.checkArgument(null != newReadIndexEntries, "newReadIndexEntries must not be null");
        val segmentReadIndex = getSegmentReadIndex(streamSegmentName, true);
        for (val entry : newReadIndexEntries) {
            addIndexEntry(segmentReadIndex, streamSegmentName, entry.getChunkName(), entry.getOffset());
        }
    }

    /**
     * Adds a new index entry for a given chunk in index for the segment.
     *
     * @param streamSegmentName Name of the segment.
     * @param chunkName         Name of the chunk.
     * @param startOffset       Start offset of the chunk.
     */
    public void addIndexEntry(String streamSegmentName, String chunkName, long startOffset) {
        Preconditions.checkArgument(null != streamSegmentName, "streamSegmentName must not be null");
        Preconditions.checkArgument(null != chunkName, "chunkName must not be null. Segment=%s", streamSegmentName);
        Preconditions.checkArgument(startOffset >= 0, "startOffset must be non-negative. Segment=%s startOffset=%s", streamSegmentName, startOffset);
        val segmentReadIndex = getSegmentReadIndex(streamSegmentName, true);
        addIndexEntry(segmentReadIndex, streamSegmentName, chunkName, startOffset);
    }

    private void addIndexEntry(SegmentReadIndex segmentReadIndex, String streamSegmentName, String chunkName, long startOffset) {
        val indexEntry = IndexEntry.builder()
                .streamSegmentName(streamSegmentName)
                .chunkName(chunkName)
                .startOffset(startOffset)
                .build();
        val existing = segmentReadIndex.offsetToChunkNameIndex.putIfAbsent(startOffset, indexEntry);
        if (null == existing) {
            indexEntryCache.put(indexEntry, true);
        } else {
            Preconditions.checkState(existing.equals(indexEntry), indexEntry.toString() + " != " + existing);
        }
    }

    /**
     * Removes the given segment from cache.
     *
     * @param streamSegmentName Name of the segment to remove.
     */
    public void remove(String streamSegmentName) {
        Preconditions.checkArgument(null != streamSegmentName, "streamSegmentName must not be null");
        val readIndex = segmentsReadIndexCache.getIfPresent(streamSegmentName);
        if (null != readIndex) {
            indexEntryCache.invalidateAll(readIndex.offsetToChunkNameIndex.values());
            readIndex.offsetToChunkNameIndex.clear();
            segmentsReadIndexCache.invalidate(streamSegmentName);
        }
    }

    /**
     * Handles removal of an entry from segmentsReadIndexCache.
     *
     * @param notification Removal notification.
     */
    void removeSegment(RemovalNotification<String, SegmentReadIndex> notification) {
        if (notification.getCause() != RemovalCause.REPLACED) {
            val readIndex = notification.getValue();
            indexEntryCache.invalidateAll(readIndex.offsetToChunkNameIndex.values());
            readIndex.offsetToChunkNameIndex.clear();
        }
    }

    /**
     * Handles removal of an entry from indexEntryCache.
     *
     * @param notification Removal notification.
     */
    void removeChunk(RemovalNotification<IndexEntry, Boolean> notification) {
        if (notification.getCause() != RemovalCause.REPLACED) {
            val indexEntry = notification.getKey();
            val segmentReadIndex = getSegmentReadIndex(indexEntry.getStreamSegmentName(), false);
            if (null != segmentReadIndex) {
                segmentReadIndex.getOffsetToChunkNameIndex().remove(indexEntry.getStartOffset());
            }
        }
    }

    /**
     * Finds a chunk that is floor to the given offset.
     *
     * @param streamSegmentName Name of the segment.
     * @param offset            Offset for which to search.
     * @return                  {@link ChunkNameOffsetPair} containing information about a chunk that is floor to the given offset.
     */
    public ChunkNameOffsetPair findFloor(String streamSegmentName, long offset) {
        Preconditions.checkArgument(null != streamSegmentName, "streamSegmentName");
        Preconditions.checkArgument(offset >= 0, "offset must be non-negative. Segment=%s offset=%s", streamSegmentName, offset);

        val segmentReadIndex = getSegmentReadIndex(streamSegmentName, false);
        if (null != segmentReadIndex && segmentReadIndex.offsetToChunkNameIndex.size() > 0) {
            val floorEntry = segmentReadIndex.offsetToChunkNameIndex.floorEntry(offset);
            if (null != floorEntry) {
                // Update cache if required.
                val indexEntry = floorEntry.getValue();
                indexEntryCache.put(indexEntry, true);

                return ChunkNameOffsetPair.builder()
                        .chunkName(floorEntry.getValue().getChunkName())
                        .offset(floorEntry.getValue().getStartOffset())
                        .build();
            }
        }
        return null;
    }

    /**
     * Truncates the read index for given segment by removing all the chunks that are below given offset.
     *  @param streamSegmentName Name of the segment to truncate.
     * @param startOffset       Start offset after truncation.
     */
    public void truncateReadIndex(String streamSegmentName, long startOffset) {
        Preconditions.checkArgument(null != streamSegmentName, "streamSegmentName");
        Preconditions.checkArgument(startOffset >= 0, "startOffset must be non-negative. Segment=%s startOffset=%s", streamSegmentName, startOffset);

        val segmentReadIndex = getSegmentReadIndex(streamSegmentName, false);
        if (null != segmentReadIndex) {
            if (segmentReadIndex.offsetToChunkNameIndex.size() > 0) {
                val headMap = segmentReadIndex.offsetToChunkNameIndex.headMap(startOffset);
                if (null != headMap) {
                    val keysToRemove = new ArrayList<Long>(headMap.keySet());
                    for (val keyToRemove : keysToRemove) {
                        // Remove entry from cache
                        val indexEntry = segmentReadIndex.offsetToChunkNameIndex.get(keyToRemove);
                        indexEntryCache.invalidate(indexEntry);
                        // Remove entry from the skip-list.
                        segmentReadIndex.offsetToChunkNameIndex.remove(keyToRemove);
                    }
                }
            }
        }
    }

    /**
     * Runs {@link Cache#cleanUp()} operations on the caches.
     */
    public void cleanUp() {
        segmentsReadIndexCache.cleanUp();
        indexEntryCache.cleanUp();
    }

    @Override
    public void report() {
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(SLTS_READ_INDEX_SEGMENT_INDEX_SIZE, segmentsReadIndexCache.size());
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(SLTS_READ_INDEX_SEGMENT_MISS_RATE, segmentsReadIndexCache.stats().missRate());
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(SLTS_READ_INDEX_CHUNK_INDEX_SIZE, indexEntryCache.size());
    }

    /**
     * Per segment read index.
     * The index contains mapping from start offset to entries containing chunk name.
     * The underlying data structure uses ConcurrentSkipListMap.
     */
    @Builder
    @RequiredArgsConstructor
    static class SegmentReadIndex {
        @Getter
        private final ConcurrentSkipListMap<Long, IndexEntry> offsetToChunkNameIndex = new ConcurrentSkipListMap<>();
    }

    /**
     * Index Entry.
     */
    @Builder
    @Data
    static class IndexEntry {
        @NonNull
        String streamSegmentName;
        @NonNull
        String chunkName;
        long startOffset;
    }
}
