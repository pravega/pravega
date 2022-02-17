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

import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.ExtendedChunkInfo;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.SegmentMock;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.val;
import org.junit.Assert;

/**
 * {@link TableContext} is a helper class that provides the necessary components to interact with a
 * {@link ContainerTableExtension} (TableStore) service.
 */
public class TableContext implements AutoCloseable {
    private final static int CONTAINER_ID = 1;
    private final static long SEGMENT_ID = 2L;
    private final static String SEGMENT_NAME = "TableSegment";

    final KeyHasher hasher;
    final MockSegmentContainer container;
    final CacheStorage cacheStorage;
    final CacheManager cacheManager;
    final ContainerTableExtensionImpl ext;
    final ScheduledExecutorService executorService;
    final TableExtensionConfig config;
    final Random random;


    TableContext(ScheduledExecutorService executorService) {
        this(KeyHashers.DEFAULT_HASHER, executorService);
    }

    TableContext(TableExtensionConfig config, ScheduledExecutorService executorService) {
        this(config, KeyHashers.DEFAULT_HASHER, executorService);
    }

    TableContext(KeyHasher hasher, ScheduledExecutorService executorService) {
        this(TableExtensionConfig.builder().build(), hasher, executorService);
    }

    TableContext(TableExtensionConfig config, KeyHasher hasher, ScheduledExecutorService executorService) {
        this.hasher = hasher;
        this.executorService = executorService;
        this.config = config;
        this.container = new MockSegmentContainer(() -> new SegmentMock(createSegmentMetadata(), executorService), CONTAINER_ID, executorService);
        this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
        this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, executorService);
        this.ext = createExtension();
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
        return createExtension(this.config);
    }

    ContainerTableExtensionImpl createExtension(TableExtensionConfig config) {
        return new ContainerTableExtensionImpl(config, this.container, this.cacheManager, this.hasher, this.executorService);
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

    private static class MockSegmentContainer implements SegmentContainer {

        private final AtomicReference<SegmentMock> segment;
        private final Supplier<SegmentMock> segmentCreator;
        private final ExecutorService executorService;
        private final AtomicBoolean closed;
        private final int containerId;

        MockSegmentContainer(Supplier<SegmentMock> segmentCreator, int containerId, ScheduledExecutorService executorService) {
            this.segmentCreator = segmentCreator;
            this.containerId = containerId;
            this.executorService = executorService;
            this.segment = new AtomicReference<>();
            this.closed = new AtomicBoolean();
        }

        SegmentMock getSegment() {
            return segment.get();
        }

        @Override
        public int getId() {
            return containerId;
        }

        @Override
        public void close() {
            this.closed.set(true);
        }

        @Override
        public CompletableFuture<DirectSegmentAccess> forSegment(String segmentName, OperationPriority priority, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            SegmentMock segment = this.segment.get();
            if (segment == null) {
                return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
            }

            Assert.assertEquals("Unexpected segment name.", segment.getInfo().getName(), segmentName);
            return CompletableFuture.supplyAsync(() -> segment, executorService);
        }

    @Override
    public CompletableFuture<Void> createStreamSegment(String segmentName, SegmentType segmentType,
                                                       Collection<AttributeUpdate> attributes, Duration timeout) {
        Assert.assertTrue("Expected Table Segment type.", segmentType.isTableSegment());
        if (this.segment.get() != null) {
            return Futures.failedFuture(new StreamSegmentExistsException(segmentName));
        }

        if (attributes == null || attributes.stream().noneMatch(au -> au.getAttributeId().equals(Attributes.ATTRIBUTE_SEGMENT_TYPE))) {
            attributes = attributes == null ? new ArrayList<>() : new ArrayList<>(attributes);
            attributes.add(new AttributeUpdate(Attributes.ATTRIBUTE_SEGMENT_TYPE, AttributeUpdateType.Replace, segmentType.getValue()));
        }

        val uc = AttributeUpdateCollection.from(attributes);
        return CompletableFuture
                .runAsync(() -> {
                    SegmentMock segment = this.segmentCreator.get();
                    Assert.assertTrue(this.segment.compareAndSet(null, segment));
                }, executorService)
                .thenCompose(v -> this.segment.get().updateAttributes(uc, timeout))
                .thenRun(() -> this.segment.get().getMetadata().refreshDerivedProperties());
    }

        @Override
        public CompletableFuture<Void> deleteStreamSegment(String segmentName, Duration timeout) {
            SegmentMock segment = this.segment.get();
            if (segment == null) {
                return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
            }
            Assert.assertEquals("Unexpected segment name.", segment.getInfo().getName(), segmentName);
            Assert.assertTrue(this.segment.compareAndSet(segment, null));
            return CompletableFuture.completedFuture(null);
        }

        //region Not Implemented Methods

        @Override
        public Collection<SegmentProperties> getActiveSegments() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public <T extends SegmentContainerExtension> T getExtension(Class<T> extensionClass) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Void> flushToStorage(Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<List<ExtendedChunkInfo>> getExtendedChunkInfo(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public Service startAsync() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public boolean isRunning() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public State state() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public Service stopAsync() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitRunning() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitRunning(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitTerminated() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitTerminated(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public Throwable failureCause() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void addListener(Listener listener, Executor executor) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Long> append(String streamSegmentName, BufferView data, AttributeUpdateCollection attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Long> append(String streamSegmentName, long offset, BufferView data, AttributeUpdateCollection attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Void> updateAttributes(String streamSegmentName, AttributeUpdateCollection attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Map<AttributeId, Long>> getAttributes(String streamSegmentName, Collection<AttributeId> attributeIds, boolean cache, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetSegmentName, String sourceSegmentName,
                                                                              AttributeUpdateCollection attributes, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetSegmentName, String sourceSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public boolean isOffline() {
            throw new UnsupportedOperationException("Not Expected");
        }

        //endregion
    }
}
