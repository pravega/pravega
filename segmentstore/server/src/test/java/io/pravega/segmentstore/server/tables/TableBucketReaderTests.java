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
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMock;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the {@link TableBucketReader} class.
 */
public class TableBucketReaderTests extends ThreadPooledTestSuite {
    private static final int COUNT = 10;
    private static final int KEY_LENGTH = 16;
    private static final int VALUE_LENGTH = 16;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

    @Override
    protected int getThreadPoolSize() {
        return 2;
    }

    /**
     * Tests the ability to locate Table Keys in a Table Bucket using {@link TableBucketReader#key}.
     */
    @Test
    public void testFindKey() throws Exception {
        val segment = new SegmentMock(executorService());

        // Generate our test data and append it to the segment.
        val data = generateData();
        segment.append(new ByteArraySegment(data.serialization), null, TIMEOUT).join();
        val reader = TableBucketReader.key(segment,
                (s, offset, timeout) -> CompletableFuture.completedFuture(data.getBackpointer(offset)),
                executorService());

        // Check a valid result.
        val validKey = data.entries.get(1).getKey();
        val validResult = reader.find(validKey.getKey(), data.getBucketOffset(), new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected version from valid key.", data.getEntryOffset(1), validResult.getVersion());
        Assert.assertEquals("Unexpected 'valid' key returned.", validKey.getKey(), validResult.getKey());

        // Check a key that does not exist.
        val invalidKey = data.unlinkedEntry.getKey();
        val invalidResult = reader.find(invalidKey.getKey(), data.getBucketOffset(), new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Not expecting any result for key that does not exist.", invalidResult);
    }

    /**
     * Tests the ability to locate Table Entries in a Table Bucket using {@link TableBucketReader#key}.
     */
    @Test
    public void testFindEntry() throws Exception {
        val segment = new SegmentMock(executorService());

        // Generate our test data and append it to the segment.
        val data = generateData();
        segment.append(new ByteArraySegment(data.serialization), null, TIMEOUT).join();
        val reader = TableBucketReader.entry(segment,
                (s, offset, timeout) -> CompletableFuture.completedFuture(data.getBackpointer(offset)),
                executorService());

        // Check a valid result.
        val validEntry = data.entries.get(1);
        val validResult = reader.find(validEntry.getKey().getKey(), data.getBucketOffset(), new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected version from valid key.", data.getEntryOffset(1), validResult.getKey().getVersion());
        Assert.assertEquals("Unexpected 'valid' key returned.", validEntry.getKey().getKey(), validResult.getKey().getKey());
        Assert.assertEquals("Unexpected 'valid' key returned.", validEntry.getValue(), validResult.getValue());

        // Check a key that does not exist.
        val invalidKey = data.unlinkedEntry.getKey();
        val invalidResult = reader.find(invalidKey.getKey(), data.getBucketOffset(), new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Not expecting any result for key that does not exist.", invalidResult);
    }

    /**
     * Tests the ability to (not) locate Table Entries in a Table Bucket for deleted and inexistent keys.
     */
    @Test
    public void testFindEntryNotExists() throws Exception {
        val segment = new SegmentMock(executorService());

        // Generate our test data.
        val es = new EntrySerializer();
        val entries = generateEntries(es);

        // Deleted key (that was previously indexed).
        val deletedKey = entries.get(0).getKey();
        val data = es.serializeRemoval(Collections.singleton(deletedKey));
        segment.append(data, null, TIMEOUT).join();
        val reader = TableBucketReader.entry(segment,
                (s, offset, timeout) -> CompletableFuture.completedFuture(-1L), // No backpointers.
                executorService());

        val deletedResult = reader.find(deletedKey.getKey(), 0L, new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Expecting a TableEntry with null value for deleted key.", deletedResult.getValue());

        // Inexistent key (that did not exist previously).
        val inexistentKey = entries.get(1).getKey();
        val inexistentResult = reader.find(inexistentKey.getKey(), 0L, new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Not expecting any result for key that was never present.", inexistentResult);
    }

    /**
     * Tests the ability to locate all non-deleted Table Keys in a Table Bucket.
     */
    @Test
    public void testFindAllKeys() {
        testFindAll(TableBucketReader::key, TableEntry::getKey, TableKey::equals);
    }

    /**
     * Tests the ability to locate all non-deleted Table Entries in a Table Bucket.
     */
    @Test
    public void testFindAllEntries() {
        testFindAll(TableBucketReader::entry, e -> e, TableEntry::equals);
    }

    @SneakyThrows
    private <T> void testFindAll(GetBucketReader<T> createReader, Function<TableEntry, T> getItem, BiPredicate<T, T> areEqual) {
        val segment = new SegmentMock(executorService());

        // Generate our test data and append it to the segment.
        val data = generateData();
        segment.append(new ByteArraySegment(data.serialization), null, TIMEOUT).join();

        // Generate a deleted key and append it to the segment.
        val deletedKey = data.entries.get(0).getKey();
        val es = new EntrySerializer();
        val deletedData = es.serializeRemoval(Collections.singleton(deletedKey));
        long newBucketOffset = segment.append(deletedData, null, TIMEOUT).join();
        data.backpointers.put(newBucketOffset, data.getBucketOffset());

        // Create a new TableBucketReader and get all the requested items for this bucket. We pass the offset of the
        // deleted entry to make sure its data is not included.
        val reader = createReader.apply(segment, (s, offset, timeout) -> CompletableFuture.completedFuture(data.getBackpointer(offset)), executorService());
        val result = reader.findAllExisting(newBucketOffset, new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // We expect to find all non-deleted Table Items that are linked.
        val expectedResult = data.entries.stream()
                .filter(e -> data.backpointers.containsValue(e.getKey().getVersion()))
                .map(getItem)
                .collect(Collectors.toList());

        AssertExtensions.assertContainsSameElements("Unexpected result from findAll().", expectedResult, result,
                (i1, i2) -> areEqual.test(i1, i2) ? 0 : 1);
    }

    private TestData generateData() {
        val s = new EntrySerializer();
        val entries = generateEntries(s);
        val length = entries.stream().mapToInt(s::getUpdateLength).sum();
        val serialization = s.serializeUpdate(entries).getCopy();
        val backpointers = new HashMap<Long, Long>();

        // The first entry is not linked; we use that to search for inexistent keys.
        for (int i = 2; i < entries.size(); i++) {
            backpointers.put(entries.get(i).getKey().getVersion(), entries.get(i - 1).getKey().getVersion());
        }

        return new TestData(entries, serialization, backpointers, entries.get(0));
    }

    private List<TableEntry> generateEntries(EntrySerializer s) {
        val rnd = new Random(0);
        val result = new ArrayList<TableEntry>();
        long version = 0;
        for (int i = 0; i < COUNT; i++) {
            byte[] keyData = new byte[KEY_LENGTH];
            rnd.nextBytes(keyData);
            byte[] valueData = new byte[VALUE_LENGTH];
            rnd.nextBytes(valueData);
            result.add(TableEntry.versioned(new ByteArraySegment(keyData), new ByteArraySegment(valueData), version));
            version += s.getUpdateLength(result.get(result.size() - 1));
        }

        return result;
    }

    @RequiredArgsConstructor
    private static class TestData {
        final List<TableEntry> entries;
        final byte[] serialization;
        final Map<Long, Long> backpointers;
        final TableEntry unlinkedEntry;

        long getEntryOffset(int index) {
            return this.entries.get(index).getKey().getVersion();
        }

        long getBucketOffset() {
            return getEntryOffset(this.entries.size() - 1);
        }

        long getBackpointer(long source) {
            return this.backpointers.getOrDefault(source, -1L);
        }
    }

    @FunctionalInterface
    private interface GetBucketReader<T> {
        TableBucketReader<T> apply(DirectSegmentAccess segment, TableBucketReader.GetBackpointer getBackpointer, ScheduledExecutorService executor);
    }
}
