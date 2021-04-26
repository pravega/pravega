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
package io.pravega.segmentstore.server.containers;

import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link MetadataCleaner} class.
 */
public class MetadataCleanerTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 1;
    private static final int MAX_ACTIVE_COUNT = 1000;
    private static final int ATTRIBUTES_PER_SEGMENT = 10;
    private static final ContainerConfig CONFIG = ContainerConfig.builder()
            .with(ContainerConfig.MAX_CACHED_EXTENDED_ATTRIBUTE_COUNT, ATTRIBUTES_PER_SEGMENT / 2)
            .build();
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests {@link MetadataCleaner#runOnce()}.
     */
    @Test
    public void testCleanup() throws Exception {
        @Cleanup
        val context = new TestContext();
        Assert.assertNotEquals(0, context.metadata.getActiveSegmentCount());

        // Cleanup #1. We expect half of the deleted segments to be evicted (due to how they're set up).
        val expected1 = context.metadata.getEvictionCandidates(0, 1000);
        AssertExtensions.assertGreaterThan("Expected at least one eligible segment.", 0, expected1.size());
        context.cleaner.runOnce().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        AssertExtensions.assertContainsSameElements("Unexpected evicted segments on the first round.",
                expected1, context.cleanedUpMetadata, Comparator.comparingLong(SegmentMetadata::getId));

        // Cleanup #2. We expect all the remaining evictable segments to be evicted.
        context.cleanedUpMetadata.clear();
        val expected2 = context.metadata.getEvictionCandidates(context.metadata.getOperationSequenceNumber(), 1000);
        AssertExtensions.assertGreaterThan("Expected at least one eligible segment.", 0, expected2.size());
        context.cleaner.runOnce().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        AssertExtensions.assertContainsSameElements("Unexpected evicted segments on the second round.",
                expected2, context.cleanedUpMetadata, Comparator.comparingLong(SegmentMetadata::getId));

        // Verify that we have properly evicted the attributes that we should have.
        val nonEvictedSegmentIds = context.metadata.getAllStreamSegmentIds();
        AssertExtensions.assertGreaterThan("", 0, nonEvictedSegmentIds.size());
        for (val segmentId : nonEvictedSegmentIds) {
            val sm = context.metadata.getStreamSegmentMetadata(segmentId);
            if (sm.isDeleted() || sm.isMerged()) {
                continue;
            }

            val attributeCount = sm.getAttributes((k, v) -> !Attributes.isCoreAttribute(k)).size();
            Assert.assertEquals("Unexpected number of remaining non-core attributes.", CONFIG.getMaxCachedExtendedAttributeCount(), attributeCount);
        }
    }

    /**
     * Tests {@link MetadataCleaner#persistAll}.
     */
    @Test
    public void testPersistAll() throws Exception {
        @Cleanup
        val context = new TestContext();
        Assert.assertNotEquals(0, context.metadata.getActiveSegmentCount());
        context.cleaner.persistAll(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        val expectedSegments = context.metadata.getAllStreamSegmentIds().stream()
                .map(context.metadata::getStreamSegmentMetadata)
                .filter(Objects::nonNull)
                .filter(sm -> !sm.isDeleted() && !sm.isMerged())
                .collect(Collectors.toList());
        AssertExtensions.assertGreaterThan("Expected at least one eligible segment.", 0, expectedSegments.size());
        Assert.assertEquals("Unexpected number of segments persisted.", expectedSegments.size(), context.metadataStore.getSegmentCount());

        for (val sm : expectedSegments) {
            val info = context.metadataStore.getSegmentInfo(sm.getName(), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assert.assertNotNull("No persisted info for " + sm.getName(), info);
            Assert.assertEquals("Unexpected length for " + sm.getName(), sm.getLength(), info.getLength());
        }
    }

    private void populateMetadata(UpdateableContainerMetadata metadata, int deletedCount, int mergedCount, int evictableCount,
                                  int nonEvictableCount, int attributeCount) {
        val truncationSeqNo = 10000L;
        metadata.removeTruncationMarkers(truncationSeqNo);

        val segmentId = new AtomicLong(metadata.getAllStreamSegmentIds().stream().max(Long::compareTo).orElse(1L));
        for (int i = 0; i < deletedCount; i++) {
            val id = segmentId.incrementAndGet();
            val name = String.format("%s_Deleted", id);
            val m = metadata.mapStreamSegmentId(name, id);
            m.setLength(1000 + i);
            m.markDeleted();
            m.setLastUsed(truncationSeqNo + (i % 2 == 0 ? 1 : -1));
        }

        for (int i = 0; i < mergedCount; i++) {
            val id = segmentId.incrementAndGet();
            val name = String.format("%s_Merged", id);
            val m = metadata.mapStreamSegmentId(name, id);
            m.setLength(2000 + i);
            m.markMerged();
            m.setLastUsed(truncationSeqNo + (i % 2 == 0 ? 1 : -1));
        }

        for (int i = 0; i < evictableCount; i++) {
            val id = segmentId.incrementAndGet();
            val name = String.format("%s_Evictable", id);
            val m = metadata.mapStreamSegmentId(name, id);
            m.setLength(3000 + i);
            m.setLastUsed(truncationSeqNo - 1);
        }

        for (int i = 0; i < nonEvictableCount; i++) {
            val id = segmentId.incrementAndGet();
            val name = String.format("%s_NonEvictable", id);
            val m = metadata.mapStreamSegmentId(name, id);
            m.setLength(4000 + i);
            m.setLastUsed(truncationSeqNo - 1); // So we can evict some attributes.
            for (int j = 0; j < CONFIG.getMaxCachedExtendedAttributeCount(); j++) {
                m.updateAttributes(Collections.singletonMap(UUID.randomUUID(), (long) j));
            }

            // This will make the segment non-evictable, and the same with the rest of the attributes.
            m.setLastUsed(truncationSeqNo + 1);
            for (int j = CONFIG.getMaxCachedExtendedAttributeCount(); j < attributeCount; j++) {
                m.updateAttributes(Collections.singletonMap(UUID.randomUUID(), attributeCount + (long) j));
            }
        }

        metadata.enterRecoveryMode();
        metadata.setOperationSequenceNumber(truncationSeqNo + 1);
        metadata.exitRecoveryMode();
    }

    private class TestContext implements AutoCloseable {
        final StreamSegmentContainerMetadata metadata;
        final MetadataStore.Connector connector;
        final TestMetadataStore metadataStore;
        final MetadataCleaner cleaner;
        final List<SegmentMetadata> cleanedUpMetadata = Collections.synchronizedList(new ArrayList<>());

        TestContext() {
            this.metadata = new StreamSegmentContainerMetadata(CONTAINER_ID, MAX_ACTIVE_COUNT);

            this.connector = new MetadataStore.Connector(metadata,
                    (s, sp, pin, timeout) -> Futures.failedFuture(new UnsupportedOperationException()),
                    (s, timeout) -> Futures.failedFuture(new UnsupportedOperationException()),
                    (s, timeout) -> Futures.failedFuture(new UnsupportedOperationException()),
                    () -> Futures.failedFuture(new UnsupportedOperationException()));

            this.metadataStore = new TestMetadataStore(connector);
            this.cleaner = new MetadataCleaner(CONFIG, this.metadata, this.metadataStore, this.cleanedUpMetadata::addAll, executorService(), "");
            populateMetadata(this.metadata, 10, 20, 30, 40, ATTRIBUTES_PER_SEGMENT);
        }

        @Override
        public void close() {
            this.cleaner.close();
            this.metadataStore.close();
        }
    }

    private class TestMetadataStore extends MetadataStore {
        @GuardedBy("segments")
        private final HashMap<String, BufferView> segments;

        TestMetadataStore(@NonNull Connector connector) {
            super(connector, executorService());
            this.segments = new HashMap<>();
        }

        int getSegmentCount() {
            synchronized (this.segments) {
                return this.segments.size();
            }
        }

        @Override
        CompletableFuture<Void> initialize(Duration timeout) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected CompletableFuture<Void> createSegment(String segmentName, ArrayView segmentInfo, TimeoutTimer timer) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.segments) {
                    if (this.segments.containsKey(segmentName)) {
                        throw new CompletionException(new StreamSegmentExistsException(segmentName));
                    } else {
                        this.segments.put(segmentName, new ByteArraySegment(segmentInfo.getCopy()));
                    }
                }
            }, executorService());
        }

        @Override
        public CompletableFuture<Boolean> clearSegmentInfo(String segmentName, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.segments) {
                    return this.segments.remove(segmentName) != null;
                }
            }, executorService());
        }

        @Override
        protected CompletableFuture<BufferView> getSegmentInfoInternal(String segmentName, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.segments) {
                    if (this.segments.containsKey(segmentName)) {
                        return new ByteArraySegment(this.segments.get(segmentName).getCopy());
                    } else {
                        throw new CompletionException(new StreamSegmentNotExistsException(segmentName));
                    }
                }
            }, executorService());
        }

        @Override
        protected CompletableFuture<Void> updateSegmentInfo(String segmentName, ArrayView segmentInfo, Duration timeout) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.segments) {
                    this.segments.put(segmentName, new ByteArraySegment(segmentInfo.getCopy()));
                }
            }, executorService());
        }
    }
}
