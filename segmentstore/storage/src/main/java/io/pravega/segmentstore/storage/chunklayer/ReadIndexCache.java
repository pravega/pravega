/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import lombok.Builder;
import lombok.Data;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An in-memory implementation of cache for read index that maps chunk start offset to chunk name for recently used segments.
 * Eviction is performed only when number of segments or chunks exceeds certain given limits.
 * Each time items are evicted the generation is incremented.
 * For each entry in cache, we keep track of the generation in which it was last accessed.
 * During eviction items with oldest generation are evicted first until enough objects are evicted.
 * The least accessed segments are removed entirely before removing chunks from more recently used segments.
 * This calculation is "best effort" and need not be accurate.
 */
class ReadIndexCache {

    /**
     * Max number of indexed segments to keep in cache.
     */
    private final int maxIndexedSegments;

    /**
     * Max number of indexed chunks to keep in cache.
     */
    private final int maxIndexedChunks;

    /**
     * Max number of indexed chunks to keep per segment in cache.
     */
    private final int maxIndexedChunksPerSegment;

    /**
     * Current generation of cache entries.
     */
    private final AtomicLong currentGeneration = new AtomicLong();

    /**
     * Lowest generation of cache entries.
     */
    private final AtomicLong oldestGeneration = new AtomicLong();

    /**
     * Total number of chunks in the cache.
     */
    private final AtomicInteger totalChunkCount = new AtomicInteger();

    /**
     * Index of chunks for a segment by their start offsets.
     */
    private final ConcurrentHashMap<String, SegmentReadIndex> segmentsToReadIndexMap = new ConcurrentHashMap<String, SegmentReadIndex>();

    /**
     * Constructor.
     *
     * @param maxIndexedSegments
     * @param maxIndexedChunksPerSegment
     * @param maxIndexedChunks
     */
    public ReadIndexCache(int maxIndexedSegments, int maxIndexedChunksPerSegment, int maxIndexedChunks) {
        this.maxIndexedChunksPerSegment = maxIndexedChunksPerSegment;
        this.maxIndexedSegments = maxIndexedSegments;
        this.maxIndexedChunks = maxIndexedChunks;
    }

    /**
     * Retrieves the read index for given segment.
     *
     * @param streamSegmentName Name of the segment.
     * @return Read index corresponding to the given segment. A new empty index is created if it doesn't already exist.
     */
    private SegmentReadIndex getSegmentReadIndex(String streamSegmentName) {
        SegmentReadIndex readIndex = segmentsToReadIndexMap.get(streamSegmentName);

        if (null == readIndex) {
            // Evict segments if required.
            if (maxIndexedSegments < segmentsToReadIndexMap.size() + 1 || maxIndexedChunks < totalChunkCount.get() + 1) {
                evictSegmentsFromOldestGeneration();
            }
            val newReadIndex = SegmentReadIndex.builder()
                    .chunkIndex(new ConcurrentSkipListMap<Long, SegmentReadIndexEntry>())
                    .generation(new AtomicLong(currentGeneration.get()))
                    .build();
            val oldReadIndex = segmentsToReadIndexMap.putIfAbsent(streamSegmentName, newReadIndex);
            readIndex = null != oldReadIndex ? oldReadIndex : newReadIndex;
        }

        return readIndex;
    }

    /**
     * Gets total number of chunks in cache.
     *
     * @return
     */
    public int getTotalChunksCount() {
        return totalChunkCount.get();
    }

    /**
     * Gets total number of segments in cache.
     *
     * @return
     */
    public int getTotalSegmentCount() {
        return segmentsToReadIndexMap.size();
    }

    /**
     * Gets oldest generation in cache.
     *
     * @return
     */
    public long getOldestGeneration() {
        return oldestGeneration.get();
    }

    /**
     * Gets current generation of cache.
     *
     * @return
     */
    public long getCurrentGeneration() {
        return currentGeneration.get();
    }

    /**
     * Adds a new index entry for a given chunk in index for the segment.
     *
     * @param streamSegmentName Name of the segment.
     * @param chunkName         Name of the chunk.
     * @param startOffset       Start offset of the chunk.
     */
    public void addIndexEntry(String streamSegmentName, String chunkName, long startOffset) {
        if (null != chunkName) {
            val segmentReadIndex = getSegmentReadIndex(streamSegmentName);

            // Evict chunks if required.
            if (maxIndexedChunksPerSegment < segmentReadIndex.chunkIndex.size() + 1
                    || maxIndexedChunks < totalChunkCount.get() + 1) {
                evictChunks(streamSegmentName, 1);
            }

            segmentReadIndex.chunkIndex.put(startOffset,
                    SegmentReadIndexEntry.builder()
                            .chunkName(chunkName)
                            .generation(new AtomicLong(currentGeneration.get()))
                            .build());
            segmentReadIndex.generation.set(currentGeneration.get());
            totalChunkCount.incrementAndGet();
        }
    }

    /**
     * Updates read index for given segment with new entries.
     *
     * @param streamSegmentName
     * @param newReadIndexEntries List of {@link ChunkNameOffsetPair} for new entries.
     */
    public void addIndexEntries(String streamSegmentName, List<ChunkNameOffsetPair> newReadIndexEntries) {
        val segmentReadIndex = getSegmentReadIndex(streamSegmentName);
        // Evict chunks if required.
        if (maxIndexedChunksPerSegment < segmentReadIndex.chunkIndex.size() + newReadIndexEntries.size()
                || maxIndexedChunks < totalChunkCount.get() + newReadIndexEntries.size()) {
            evictChunks(streamSegmentName, newReadIndexEntries.size());
        }

        for (val entry : newReadIndexEntries) {
            segmentReadIndex.chunkIndex.put(entry.getOffset(),
                    SegmentReadIndexEntry.builder()
                            .chunkName(entry.getChunkName())
                            .generation(new AtomicLong(currentGeneration.get()))
                            .build());
            segmentReadIndex.generation.set(currentGeneration.get());

        }
        totalChunkCount.getAndAdd(newReadIndexEntries.size());
    }

    /**
     * Removes the given segment from cache.
     *
     * @param streamSegmentName
     */
    public void remove(String streamSegmentName) {
        val readIndex = segmentsToReadIndexMap.get(streamSegmentName);
        if (null != readIndex) {
            segmentsToReadIndexMap.remove(streamSegmentName);
            totalChunkCount.getAndAdd(-1 * readIndex.chunkIndex.size());
        }
    }

    /**
     * Finds a chunk that is floor to the given offset.
     *
     * @param streamSegmentName Name of the segment.
     * @param offset            Offset for which to search.
     * @return
     */
    public ChunkNameOffsetPair findFloor(String streamSegmentName, long offset) {
        val segmentReadIndex = getSegmentReadIndex(streamSegmentName);
        if (segmentReadIndex.chunkIndex.size() > 0) {
            val floorEntry = segmentReadIndex.chunkIndex.floorEntry(offset);
            if (null != floorEntry) {
                // mark with current generation
                segmentReadIndex.generation.set(currentGeneration.get());
                floorEntry.getValue().generation.set(currentGeneration.get());
                // return value.
                return new ChunkNameOffsetPair(floorEntry.getKey(), floorEntry.getValue().getChunkName());
            }
        }
        return null;
    }

    /**
     * Truncates the read index for given segment by removing all the chunks that are below given offset.
     *
     * @param streamSegmentName
     * @param startOffset
     */
    public void truncateReadIndex(String streamSegmentName, long startOffset) {
        val segmentReadIndex = getSegmentReadIndex(streamSegmentName);
        if (null != segmentReadIndex) {
            if (segmentReadIndex.chunkIndex.size() > 0) {
                val headMap = segmentReadIndex.chunkIndex.headMap(startOffset);
                if (null != headMap) {
                    int removed = 0;
                    ArrayList<Long> keysToRemove = new ArrayList<Long>();
                    keysToRemove.addAll(headMap.keySet());
                    for (val keyToRemove : keysToRemove) {
                        segmentReadIndex.chunkIndex.remove(keyToRemove);
                        removed++;
                    }
                    if (removed > 0) {
                        totalChunkCount.getAndAdd(-1 * removed);
                    }
                }
            }
            if (segmentReadIndex.chunkIndex.size() > 0) {
                segmentReadIndex.generation.set(currentGeneration.get());
            }
        }
    }

    /**
     * Evicts segments from the cache.
     */
    private void evictSegmentsFromOldestGeneration() {
        val oldGen = oldestGeneration.get();
        currentGeneration.getAndIncrement();

        val iterator = segmentsToReadIndexMap.entrySet().iterator();
        int total = totalChunkCount.get();
        int removed = 0;
        while (iterator.hasNext() && (segmentsToReadIndexMap.size() >= maxIndexedSegments || total >= maxIndexedChunks)) {
            val entry = iterator.next();
            if (entry.getValue().generation.get() <= oldGen) {
                val size = entry.getValue().chunkIndex.size();
                removed += size;
                total -= size;
                iterator.remove();
            }
        }
        if (removed > 0) {
            oldestGeneration.compareAndSet(oldGen, oldGen + 1);
            totalChunkCount.getAndAdd(-1 * removed);
        }
    }

    /**
     * Evicts chunks from the cache.
     */
    private void evictChunks(String streamSegmentName, long toRemoveCount) {
        val segmentReadIndex = segmentsToReadIndexMap.get(streamSegmentName);
        evictChunks(segmentReadIndex, toRemoveCount);
    }

    /**
     * Evicts chunks from the cache.
     */
    private void evictChunks(SegmentReadIndex segmentReadIndex, long toRemoveCount) {
        // Increment generation.
        val previousGen = currentGeneration.getAndIncrement();
        val oldGen = oldestGeneration.get();

        // Step 1 : Go through all entries once to record counts per each generation.
        TreeMap<Long, Integer> counts = new TreeMap<Long, Integer>();
        val iterator = segmentReadIndex.chunkIndex.entrySet().iterator();
        while (iterator.hasNext()) {
            val entry = iterator.next();
            long generation = entry.getValue().generation.get();
            val cnt = counts.get(generation);
            if (null == cnt) {
                counts.put(generation, 1);
            } else {
                counts.put(generation, cnt + 1);
            }
        }

        // Step 2 : Determine up to what generation to delete.
        long deletedUpToGen = 0;
        int runningCount = 0;
        for (val entry : counts.entrySet()) {
            runningCount += entry.getValue();
            deletedUpToGen = entry.getKey();
            if (runningCount >= toRemoveCount) {
                break;
            }
        }

        // Step 3: Now remove keys.
        int removed = 0;
        val iterator2 = segmentReadIndex.chunkIndex.entrySet().iterator();
        while (iterator2.hasNext() && removed < toRemoveCount) {
            val entry = iterator2.next();
            val gen = entry.getValue().generation.get();
            if (gen <= deletedUpToGen) {
                iterator2.remove();
                removed++;
                counts.put(gen, counts.get(gen) - 1);
            }
            if (removed == toRemoveCount) {
                break;
            }
        }

        if (removed > 0) {
            long newOldGenvalue = previousGen + 1;
            for (val entry : counts.entrySet()) {
                if (entry.getValue() != 0) {
                    newOldGenvalue = entry.getKey();
                    break;
                }
            }
            if (newOldGenvalue != oldGen) {
                oldestGeneration.compareAndSet(oldGen, newOldGenvalue);
            }
            totalChunkCount.getAndAdd(-1 * removed);
        }
    }

    /**
     * Per segment read index.
     * The index contains mapping from start offset to entries containing chunk names.
     * The underlying data structure uses ConcurrentSkipListMap.
     * The generation is used to cleanup unused entries.
     */
    @Builder
    @Data
    private static class SegmentReadIndex {
        private final ConcurrentSkipListMap<Long, SegmentReadIndexEntry> chunkIndex;
        private final AtomicLong generation;
    }

    /**
     * Entry for chunk in a segment read index.
     * The generation is used to cleanup unused entries.
     */
    @Builder
    @Data
    private static class SegmentReadIndexEntry {
        private final String chunkName;
        private final AtomicLong generation;
    }
}
