package io.pravega.segmentstore.server.tables;

import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.*;
import io.pravega.segmentstore.server.*;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import lombok.val;
import org.junit.Assert;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class TableContext implements AutoCloseable {

    private static final int DEFAULT_COMPACTION_SIZE = -1; // Inherits from parent.

    final KeyHasher hasher;
    final ContainerMock container;
    final CacheStorage cacheStorage;
    final CacheManager cacheManager;
    final ContainerTableExtensionImpl ext;
    final ScheduledExecutorService executorService;
    final int defaultCompactionSize;
    final Random random;

    final static String SEGMENT_NAME = "TableSegment";
    final static long SEGMENT_ID = 2L;
    final static int CONTAINER_ID = 1;

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
