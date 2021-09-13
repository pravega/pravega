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

import com.google.common.collect.Maps;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.server.AttributeIterator;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMock;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link TableIterator} class.
 */
public class TableIteratorTests extends ThreadPooledTestSuite {
    private static final int INDEX_COUNT = 20;
    private static final int CACHE_COUNT = 100;
    private static final double HASH_OVERLAP_RATIO = 0.4; // How many attributes in Cache overlap Index attributes.
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    /**
     * Tests the {@link TableIterator} when Key Hashes are only available from the Cache (nothing from the index).
     */
    @Test
    public void testFromCacheOnly() {
        val testData = createTestData(0, CACHE_COUNT);
        test(testData);
    }

    /**
     * Tests the {@link TableIterator} when Key Hashes are only available from the Index (nothing from the cache).
     */
    @Test
    public void testFromIndexOnly() {
        val testData = createTestData(INDEX_COUNT, 0);
        test(testData);
    }

    /**
     * Tests the {@link TableIterator} when Key Hashes are available from both the Cache and the index.
     */
    @Test
    public void testCacheAndIndex() {
        val testData = createTestData(INDEX_COUNT, CACHE_COUNT);
        test(testData);
    }

    /**
     * Tests the {@link TableIterator#empty()} method.
     */
    @Test
    public void testEmpty() throws Exception {
        val empty = TableIterator.empty();
        val next = empty.getNext().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Empty iterator should return null from getNext().", next);
    }

    @SneakyThrows
    private void test(TestData testData) {
        // Try a combination of different start hashes.
        List<UUID> startHashes;
        if (testData.baseIteratorHashes.isEmpty()) {
            startHashes = testData.cacheHashes.keySet().stream().sorted().collect(Collectors.toList());
        } else {
            startHashes = testData.baseIteratorHashes
                    .stream()
                    .map(hashList -> hashList.isEmpty() ? KeyHasher.MIN_HASH : hashList.get(0).getKey())
                    .collect(Collectors.toList());
        }

        for (val firstHash : startHashes) {
            // We convert TableBuckets into BucketWrappers to verify that the conversion actually is executed.
            val iterator = TableIterator
                    .<BucketWrapper>builder()
                    .segment(testData.segment)
                    .cacheHashes(testData.cacheHashes)
                    .firstHash(firstHash)
                    .executor(executorService())
                    .fetchTimeout(TIMEOUT)
                    .resultConverter(bucket -> CompletableFuture.completedFuture(new BucketWrapper(bucket)))
                    .build().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Keeps track of all Iterator States we encounter, and their offsets.
            val actualBuckets = new ArrayList<TableBucket>();
            val mainIteration = iterator.forEachRemaining(item -> actualBuckets.add(item.bucket), executorService());
            mainIteration.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Verify output matches.
            val expectedResult = filter(testData.expectedResult, TableBucket::getHash, firstHash, KeyHasher.MAX_HASH);
            AssertExtensions.assertListEquals("Unexpected result when first hash is " + firstHash,
                    expectedResult, actualBuckets, (b1, b2) -> b1.equals(b2) && b1.getSegmentOffset() == b2.getSegmentOffset());
        }
    }

    private TestData createTestData(int indexHashCount, int cacheHashCount) {
        val rnd = new Random(0);
        val builder = TestData.builder();
        val expectedResult = new HashMap<UUID, TableBucket>();
        val allHashes = new ArrayList<UUID>();
        int nextHash = 0;

        // Create index hashes.
        int iteratorSize = 0;
        for (int i = 0; i < indexHashCount; i++) {
            val indexItems = new ArrayList<Map.Entry<UUID, Long>>(iteratorSize);
            for (int j = 0; j < iteratorSize; j++) {
                UUID hash = new UUID(nextHash, nextHash);
                long segmentOffset = rnd.nextLong();
                indexItems.add(Maps.immutableEntry(hash, segmentOffset));
                allHashes.add(hash);
                expectedResult.put(hash, new TableBucket(hash, segmentOffset));
                nextHash += CACHE_COUNT + 1;
            }

            builder.baseIteratorHash(indexItems);
            iteratorSize++; // Next iterator will have one more item.
        }

        // Create cache hashes.
        for (int i = 0; i < cacheHashCount; i++) {
            UUID hash = indexHashCount <= 1 ? null : allHashes.get(rnd.nextInt(allHashes.size()));
            if (hash == null) {
                hash = new UUID(nextHash, nextHash);
                nextHash += CACHE_COUNT + 1;
            } else if (rnd.nextDouble() > HASH_OVERLAP_RATIO) {
                // Do not reuse hash, but choose one nearby.
                do {
                    hash = new UUID(hash.getMostSignificantBits() + 1, hash.getLeastSignificantBits() - 1);
                } while (expectedResult.containsKey(hash));
            }

            long segmentOffset = rnd.nextInt(Integer.MAX_VALUE);
            expectedResult.put(hash, new TableBucket(hash, segmentOffset));
            builder.cacheHash(hash, new CacheBucketOffset(segmentOffset, false));
        }

        return builder.expectedResult(expectedResult.values().stream()
                                                    .sorted(Comparator.comparing(TableBucket::getHash))
                                                    .collect(Collectors.toList()))
                .executor(executorService())
                .build();
    }

    private static <T> List<T> filter(List<T> items, Function<T, UUID> getHash, UUID fromId, UUID toId) {
        return items.stream()
                .filter(e -> getHash.apply(e).compareTo(fromId) >= 0 && getHash.apply(e).compareTo(toId) <= 0)
                .collect(Collectors.toList());
    }

    private static List<Map.Entry<AttributeId, Long>> toAttributeIterator(List<Map.Entry<UUID, Long>> items, AttributeId fromId, AttributeId toId) {
        return items.stream()
                .map(e -> new AbstractMap.SimpleImmutableEntry<>(AttributeId.fromUUID(e.getKey()), e.getValue()))
                .filter(e -> e.getKey().compareTo(fromId) >= 0 && e.getKey().compareTo(toId) <= 0)
                .collect(Collectors.toList());
    }

    @RequiredArgsConstructor
    private static class BucketWrapper {
        final TableBucket bucket;
    }

    @Builder
    @NotThreadSafe
    private static class TestData {
        @Singular
        private final List<List<Map.Entry<UUID, Long>>> baseIteratorHashes;
        @Singular
        private final Map<UUID, CacheBucketOffset> cacheHashes;
        private final List<TableBucket> expectedResult;
        private final DirectSegmentAccess segment = new TestSegment();
        private final ScheduledExecutorService executor;

        private class TestSegment extends SegmentMock {
            TestSegment() {
                super(executor);
            }

            @Override
            public CompletableFuture<AttributeIterator> attributeIterator(AttributeId fromId, AttributeId toId, Duration timeout) {
                Assert.assertEquals("toId must always be KeyHasher.MAX_HASH.", KeyHasher.MAX_HASH, ((AttributeId.UUID) toId).toUUID());
                val iterator = baseIteratorHashes.iterator();
                return CompletableFuture.completedFuture(() -> CompletableFuture.completedFuture(
                        iterator.hasNext() ? toAttributeIterator(iterator.next(), fromId, toId) : null));
            }
        }
    }
}
