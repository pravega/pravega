/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import lombok.val;

public class TableContext implements AutoCloseable {

    private static final int DEFAULT_COMPACTION_SIZE = -1; // Inherits from parent.

    private final static int CONTAINER_ID = 1;
    private final static long SEGMENT_ID = 2L;
    private final static String SEGMENT_NAME = "TableSegment";

    final KeyHasher hasher;
    final ContainerMock container;
    final CacheStorage cacheStorage;
    final CacheManager cacheManager;
    final ContainerTableExtensionImpl ext;
    final ScheduledExecutorService executorService;
    final int defaultCompactionSize;
    final Random random;


    TableContext(ScheduledExecutorService executorService) {
        this(KeyHashers.DEFAULT_HASHER, DEFAULT_COMPACTION_SIZE, executorService);
    }

    TableContext(int defaultCompactionSize, ScheduledExecutorService executorService) {
        this(KeyHashers.DEFAULT_HASHER, defaultCompactionSize, executorService);
    }

    TableContext(KeyHasher hasher, int maxCompactionSize, ScheduledExecutorService executorService) {
        this.hasher = hasher;
        this.executorService = executorService;
        this.defaultCompactionSize = maxCompactionSize;
        this.container = new ContainerMock(() -> new SegmentMock(createSegmentMetadata(), executorService), CONTAINER_ID, executorService);
        this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
        this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, executorService);
        this.ext = createExtension(maxCompactionSize);
        this.random = new Random(0);
    }

    @Override
    public void close() {
        this.ext.close();
        this.cacheManager.close();
        this.container.close();
        this.cacheStorage.close();
    }

    ContainerTableExtensionImpl createExtension() {
        return createExtension(defaultCompactionSize);
    }

    ContainerTableExtensionImpl createExtension(int maxCompactionSize) {
        return new TestTableExtensionImpl(this.container, this.cacheManager, this.hasher, this.executorService, maxCompactionSize);
    }

    UpdateableSegmentMetadata createSegmentMetadata() {
        val result = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, CONTAINER_ID);
        result.setLength(0);
        result.setStorageLength(0);
        return result;
    }

    SegmentMock segment() {
        return this.container.getSegment();
    }

    class TestTableExtensionImpl extends ContainerTableExtensionImpl {
        private final int maxCompactionSize;

        TestTableExtensionImpl(SegmentContainer segmentContainer, CacheManager cacheManager,
                               KeyHasher hasher, ScheduledExecutorService executor, int maxCompactionSize) {
            super(segmentContainer, cacheManager, hasher, executor);
            this.maxCompactionSize = maxCompactionSize;
        }

        @Override
        protected int getMaxCompactionSize() {
            return this.maxCompactionSize == DEFAULT_COMPACTION_SIZE ? super.getMaxCompactionSize() : this.maxCompactionSize;
        }
    }
}
