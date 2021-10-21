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
package io.pravega.client.tables.impl;

import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.stream.Serializer;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.Insert;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.Put;
import io.pravega.client.tables.Remove;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.TableModification;
import io.pravega.client.tables.Version;
import io.pravega.common.util.BitConverter;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.LeakDetectorTestSuite;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Base test suite for anything testing {@link KeyValueTable}s. This covers core functionality for {@link KeyValueTable}s
 * and currently applies both to {@link KeyValueTableImplTests} (using mocked Controller and Segment Store) and
 * `io.pravega.test.integration.KeyValueTableImplTests` (using real Segment Store and Wire Protocol).
 */
public abstract class KeyValueTableTestBase extends LeakDetectorTestSuite {
    private static final Serializer<Long> PK_SERIALIZER = new LongSerializer();
    private static final Serializer<Integer> SK_SERIALIZER = new IntegerSerializer();
    private static final KeyValueTableIteratorImpl.TableKeyComparator KEY_COMPARATOR = new KeyValueTableIteratorImpl.TableKeyComparator();
    private static final int DEFAULT_SEGMENT_COUNT = 4;
    private static final int DEFAULT_PRIMARY_KEY_COUNT = 100;
    private static final int DEFAULT_SECONDARY_KEY_COUNT = 10; // Per Primary Key
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Getter
    @Rule
    public final Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
    @Getter(AccessLevel.PROTECTED)
    private List<ByteBuffer> primaryKeys;

    @Before
    public void setup() throws Exception {
        int count = getPrimaryKeyCount();
        val rnd = new Random(0);
        this.primaryKeys = IntStream.range(0, count)
                .mapToLong(i -> rnd.nextLong())
                .mapToObj(PK_SERIALIZER::serialize)
                .collect(Collectors.toList());
    }

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    protected abstract KeyValueTable createKeyValueTable();

    protected abstract KeyValueTable createKeyValueTable(KeyValueTableInfo kvt, KeyValueTableConfiguration configuration);

    protected int getPrimaryKeyLength() {
        return Long.BYTES;
    }

    protected int getSecondaryKeyLength() {
        return Integer.BYTES;
    }

    protected int getPrimaryKeyCount() {
        return DEFAULT_PRIMARY_KEY_COUNT;
    }

    protected int getSegmentCount() {
        return DEFAULT_SEGMENT_COUNT;
    }

    protected int getSecondaryKeyCount() {
        return DEFAULT_SECONDARY_KEY_COUNT;
    }

    /**
     * Tests the ability to perform single-key conditional insertions. These methods are exercised:
     * - {@link KeyValueTable#update} with {@link Insert} instances.
     * - {@link KeyValueTable#get} and {@link KeyValueTable#getAll}.
     */
    @Test
    public void testSingleKeyConditionalInserts() {
        val versions = new Versions();
        @Cleanup
        val kvt = createKeyValueTable();

        // Put (conditional insert)
        val iteration = new AtomicInteger(0);
        forEveryKey((pk, sk) -> {
            val value = getValue(pk, sk, iteration.get());

            // First one should work.
            val insert = new Insert(new TableKey(pk, sk), value);
            Version kv = kvt.update(insert).join();
            versions.add(getUniqueKeyId(pk, sk), kv);

            // Second one should throw.
            AssertExtensions.assertSuppliedFutureThrows(
                    "insert() did not throw for already existing key.",
                    () -> kvt.update(insert),
                    ex -> ex instanceof BadKeyVersionException);
        });
        checkValues(iteration.get(), versions, kvt);
    }

    /**
     * Tests the ability to perform single-key updates and replacements. These methods are exercised:
     * - {@link KeyValueTable#update} with {@link Put} instances.
     * - {@link KeyValueTable#get} and {@link KeyValueTable#getAll}.
     */
    @Test
    public void testSingleKeyUpdates() {
        val versions = new Versions();
        @Cleanup
        val kvt = createKeyValueTable();

        // Put (unconditional update).
        val iteration = new AtomicInteger(0);
        forEveryKey((pk, sk) -> {
            val value = getValue(pk, sk, iteration.get());

            Version kv = kvt.update(new Put(new TableKey(pk, sk), value)).join();
            versions.add(getUniqueKeyId(pk, sk), kv);
        });
        checkValues(iteration.get(), versions, kvt);

        // Put (conditional update (not insertion)).
        iteration.incrementAndGet();
        forEveryKey((pk, sk) -> {
            val value = getValue(pk, sk, iteration.get());
            val keyId = getUniqueKeyId(pk, sk);
            val existingVersion = versions.get(keyId);

            // Verify that conditions are checked both for segment names and their versions.
            val pkValue = Math.abs(PK_SERIALIZER.deserialize(pk));
            Version badVersion = alterVersion(existingVersion, pkValue % 2 == 0, pkValue % 2 == 1);
            AssertExtensions.assertSuppliedFutureThrows(
                    "update(Put-Conditional) did not throw for bad version.",
                    () -> kvt.update(new Put(new TableKey(pk, sk), value, badVersion)),
                    ex -> ex instanceof BadKeyVersionException);

            Version kv = kvt.update(new Put(new TableKey(pk, sk), value, existingVersion)).join();
            versions.add(keyId, kv);
        });
        checkValues(iteration.get(), versions, kvt);
    }

    /**
     * Tests the ability to perform single-key updates and removals (conditional and unconditional). These methods are exercised:
     * - {@link KeyValueTable#update} with {@link Put} instances and with {@link Remove} instances.
     * - {@link KeyValueTable#get} and {@link KeyValueTable#getAll}
     */
    @Test
    public void testSingleKeyUnconditionalRemovals() {
        val versions = new Versions();
        @Cleanup
        val kvt = createKeyValueTable();

        // Put (unconditional update).
        val iteration = new AtomicInteger(0);
        forEveryKey((pk, sk) -> {
            val value = getValue(pk, sk, iteration.get());

            Version kv = kvt.update(new Put(new TableKey(pk, sk), value)).join();
            versions.add(getUniqueKeyId(pk, sk), kv);
        });

        // Remove (both conditional and unconditional)
        iteration.incrementAndGet();
        forEveryKey((pk, sk) -> {
            val keyId = getUniqueKeyId(pk, sk);
            val existingVersion = versions.get(keyId);

            // Verify that conditions are checked both for segment names and their versions.
            val pkValue = Math.abs(PK_SERIALIZER.deserialize(pk));
            boolean conditional = pkValue % 2 == 0;
            if (conditional) {
                // First check that a bad version will be checked.
                Version badVersion = alterVersion(existingVersion, pkValue % 4 == 0, pkValue % 4 != 0);
                AssertExtensions.assertSuppliedFutureThrows(
                        "update(Remove) did not throw for bad version.",
                        () -> kvt.update(new Remove(new TableKey(pk, sk), badVersion)),
                        ex -> ex instanceof BadKeyVersionException);

                // Remove it.
                kvt.update(new Remove(new TableKey(pk, sk), existingVersion)).join();
            } else {
                kvt.update(new Remove(new TableKey(pk, sk))).join();

            }
            versions.remove(keyId);
        });
        Assert.assertTrue("Expected all keys to have been removed.", versions.isEmpty());
        checkValues(iteration.get(), versions, kvt);

        // Re-insert (conditionally).
        iteration.incrementAndGet();
        forEveryKey((pk, sk) -> {
            val value = getValue(pk, sk, iteration.get());

            // First one should work.
            Version kv = kvt.update(new Insert(new TableKey(pk, sk), value)).join();
            versions.add(getUniqueKeyId(pk, sk), kv);
        });
        checkValues(iteration.get(), versions, kvt);
    }

    /**
     * Tests the ability to perform multi-key updates, replacements and removals. These methods should be exercised:
     * - {@link KeyValueTable#update(Iterable)} with {@link Insert} and {@link Put} instances.
     * - {@link KeyValueTable#update(Iterable)} with {@link Remove}.
     * - {@link KeyValueTable#getAll}
     */
    @Test
    public void testMultiKeyOperations() {
        val versions = new Versions();
        @Cleanup
        val kvt = createKeyValueTable();

        // Conditional Insert.
        val iteration = new AtomicInteger(0);
        forEveryPrimaryKey((pk, secondaryKeys) -> {
            List<TableModification> inserts = secondaryKeys.stream()
                    .map(sk -> new Insert(new TableKey(pk, sk), getValue(pk, sk, iteration.get())))
                    .collect(Collectors.toList());
            val keyVersions = kvt.update(inserts).join();

            val hint = getUniqueKeyId(pk);
            Assert.assertEquals("Unexpected result size" + hint, inserts.size(), keyVersions.size());
            for (int i = 0; i < inserts.size(); i++) {
                versions.add(getUniqueKeyId(pk, inserts.get(i).getKey().getSecondaryKey()), keyVersions.get(i));
            }
        });
        checkValues(iteration.get(), versions, kvt);

        // Unconditional update.
        iteration.incrementAndGet();
        forEveryPrimaryKey((pk, secondaryKeys) -> {
            List<TableModification> puts = secondaryKeys.stream()
                    .map(sk -> new Put(new TableKey(pk, sk), getValue(pk, sk, iteration.get())))
                    .collect(Collectors.toList());
            val keyVersions = kvt.update(puts).join();

            val hint = getUniqueKeyId(pk);
            Assert.assertEquals("Unexpected result size" + hint, puts.size(), keyVersions.size());
            for (int i = 0; i < puts.size(); i++) {
                versions.add(getUniqueKeyId(pk, puts.get(i).getKey().getSecondaryKey()), keyVersions.get(i));
            }
        });
        checkValues(iteration.get(), versions, kvt);

        // Conditional replace.
        iteration.incrementAndGet();
        forEveryPrimaryKey((pk, secondaryKeys) -> {
            // Failed update (bad version).
            val pkValue = Math.abs(PK_SERIALIZER.deserialize(pk));
            List<TableModification> badPuts = secondaryKeys.stream()
                    .map(sk -> new Put(new TableKey(pk, sk), getValue(pk, sk, iteration.get()),
                            alterVersion(versions.get(getUniqueKeyId(pk, sk)), pkValue % 2 == 0, pkValue % 2 == 1)))
                    .collect(Collectors.toList());
            AssertExtensions.assertSuppliedFutureThrows(
                    "update(Put) did not throw for bad version.",
                    () -> kvt.update(badPuts),
                    ex -> ex instanceof BadKeyVersionException);

            // Correct update.
            List<TableModification> puts = secondaryKeys.stream()
                    .map(sk -> new Put(new TableKey(pk, sk), getValue(pk, sk, iteration.get()), versions.get(getUniqueKeyId(pk, sk))))
                    .collect(Collectors.toList());
            val keyVersions = kvt.update(puts).join();
            val hint = getUniqueKeyId(pk);
            Assert.assertEquals("Unexpected result size" + hint, puts.size(), keyVersions.size());
            for (int i = 0; i < puts.size(); i++) {
                versions.add(getUniqueKeyId(pk, puts.get(i).getKey().getSecondaryKey()), keyVersions.get(i));
            }
        });
        checkValues(iteration.get(), versions, kvt);

        // Conditional removal.
        iteration.incrementAndGet();
        forEveryPrimaryKey((pk, secondaryKeys) -> {
            val hint = getUniqueKeyId(pk);

            // Failed update (bad version).
            val pkValue = Math.abs(PK_SERIALIZER.deserialize(pk));
            List<TableModification> badRemovals = secondaryKeys.stream()
                    .map(sk -> new Remove(new TableKey(pk, sk), alterVersion(versions.get(getUniqueKeyId(pk, sk)), pkValue % 2 == 0, pkValue % 2 == 1)))
                    .collect(Collectors.toList());
            AssertExtensions.assertSuppliedFutureThrows(
                    "update(Remove) did not throw for bad version." + hint,
                    () -> kvt.update(badRemovals),
                    ex -> ex instanceof BadKeyVersionException);

            // Correct update.
            List<TableModification> removals = secondaryKeys.stream()
                    .map(sk -> new Remove(new TableKey(pk, sk), versions.get(getUniqueKeyId(pk, sk))))
                    .collect(Collectors.toList());
            kvt.update(removals).join();
            for (val sk : secondaryKeys) {
                versions.remove(getUniqueKeyId(pk, sk));
            }
        });
        Assert.assertTrue("Expected all keys to have been removed.", versions.isEmpty());
        checkValues(iteration.get(), versions, kvt);

        // Reinsert (conditionally)
        iteration.incrementAndGet();
        forEveryPrimaryKey((pk, secondaryKeys) -> {
            val hint = getUniqueKeyId(pk);
            List<TableModification> entries = secondaryKeys.stream()
                    .map(sk -> new Insert(new TableKey(pk, sk), getValue(pk, sk, iteration.get())))
                    .collect(Collectors.toList());
            val keyVersions = kvt.update(entries).join();

            Assert.assertEquals("Unexpected result size" + hint, entries.size(), keyVersions.size());
            for (int i = 0; i < entries.size(); i++) {
                versions.add(getUniqueKeyId(pk, entries.get(i).getKey().getSecondaryKey()), keyVersions.get(i));
            }
        });
        checkValues(iteration.get(), versions, kvt);
    }

    /**
     * Verifies that {@link TableEntry} or {@link TableKey} instances that have invalid key/value lengths are rejected.
     */
    @Test
    public void testIllegalEntrySizes() {
        val rnd = new Random(0);
        val limitPK = ByteBuffer.wrap(new byte[getPrimaryKeyLength()]);
        val shortPK = ByteBuffer.allocate(getPrimaryKeyLength() - 1);
        val longPK = ByteBuffer.allocate(getPrimaryKeyLength() + 1);
        val limitSK = ByteBuffer.wrap(new byte[getSecondaryKeyLength()]);
        val longSK = ByteBuffer.allocate(getSecondaryKeyLength() + 1);
        val limitValue = ByteBuffer.wrap(new byte[KeyValueTable.MAXIMUM_VALUE_LENGTH]);
        rnd.nextBytes(limitPK.array());
        rnd.nextBytes(limitSK.array());
        rnd.nextBytes(limitValue.array());
        @Cleanup
        val kvt = createKeyValueTable();
        kvt.update(new Insert(new TableKey(limitPK.duplicate(), limitSK.duplicate()), limitValue.duplicate())).join();
        val resultValue = kvt.get(new TableKey(limitPK.duplicate(), limitSK.duplicate())).join().getValue();
        Assert.assertEquals("Unexpected value returned.", limitValue.duplicate(), resultValue.duplicate());

        // Wrong Key Length
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a PK that is too long.",
                () -> kvt.update(new Put(new TableKey(longPK.duplicate(), limitSK.duplicate()), limitValue.duplicate())),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a PK that is too long.",
                () -> kvt.update(new Remove(new TableKey(longPK.duplicate(), limitSK.duplicate()))),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a PK that is too short.",
                () -> kvt.update(new Put(new TableKey(shortPK.duplicate(), limitSK.duplicate()), limitValue.duplicate())),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a PK that is too short.",
                () -> kvt.update(new Remove(new TableKey(shortPK.duplicate(), limitSK.duplicate()))),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a SK that is too long.",
                () -> kvt.update(new Put(new TableKey(limitPK.duplicate(), longSK.duplicate()), limitValue.duplicate())),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a SK that is too long.",
                () -> kvt.update(new Remove(new TableKey(limitPK.duplicate(), longSK.duplicate()))),
                ex -> ex instanceof IllegalArgumentException);

        if (getSecondaryKeyLength() > 0) {
            val shortSK = ByteBuffer.allocate(getSecondaryKeyLength() - 1);
            AssertExtensions.assertSuppliedFutureThrows(
                    "Expected a rejection of a SK that is too short.",
                    () -> kvt.update(new Put(new TableKey(limitPK.duplicate(), shortSK.duplicate()), limitValue.duplicate())),
                    ex -> ex instanceof IllegalArgumentException);
            AssertExtensions.assertSuppliedFutureThrows(
                    "Expected a rejection of a SK that is too short.",
                    () -> kvt.update(new Remove(new TableKey(limitPK.duplicate(), shortSK.duplicate()))),
                    ex -> ex instanceof IllegalArgumentException);
        }

        // Max Value Length exceeded.
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a value that is too long.",
                () -> kvt.update(new Put(new TableKey(limitPK.duplicate(), limitSK.duplicate()), ByteBuffer.allocate(KeyValueTable.MAXIMUM_VALUE_LENGTH + 1))),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Verifies that overflowing a single {@link TableSegment} limits are rejected. No batch updates, removals or retrievals
     * may exceed the {@link TableSegment#MAXIMUM_BATCH_KEY_COUNT} or {@link TableSegment#MAXIMUM_BATCH_LENGTH} limits.
     */
    @Test
    public void testLargeBatchUpdates() {
        val rnd = new Random(0);

        @Cleanup
        val kvt = createKeyValueTable();

        // Exceeding by batch count.
        val keys = IntStream.range(0, TableSegment.MAXIMUM_BATCH_KEY_COUNT + 1)
                .mapToObj(i -> new TableKey(ByteBuffer.allocate(getPrimaryKeyLength()).putLong(i), ByteBuffer.allocate(getSecondaryKeyLength()).putInt(i)))
                .collect(Collectors.toList());

        val getBatchCountExceeded = keys.stream()
                .map(k -> new TableKey(k.getPrimaryKey().duplicate(), k.getSecondaryKey().duplicate()))
                .collect(Collectors.toList());
        AssertExtensions.assertSuppliedFutureThrows(
                "Get batch exceeded max count.",
                () -> kvt.getAll(getBatchCountExceeded),
                ex -> ex instanceof IllegalArgumentException);

        List<TableModification> putBatchCountExceeded = keys.stream()
                .map(a -> new Put(new TableKey(a.getPrimaryKey().duplicate(), a.getSecondaryKey().duplicate()), a.getSecondaryKey().duplicate()))
                .collect(Collectors.toList());
        AssertExtensions.assertSuppliedFutureThrows(
                "Update batch exceeded max count.",
                () -> kvt.update(putBatchCountExceeded),
                ex -> ex instanceof IllegalArgumentException);

        List<TableModification> removeBatchCountExceeded = keys.stream()
                .map(k -> new Remove(new TableKey(k.getPrimaryKey().duplicate(), k.getSecondaryKey().duplicate())))
                .collect(Collectors.toList());
        AssertExtensions.assertSuppliedFutureThrows(
                "Remove batch exceeded max count.",
                () -> kvt.update(removeBatchCountExceeded),
                ex -> ex instanceof IllegalArgumentException);

        // Exceed by serialization size.
        // It is impossible to exceed the serialization size for retrievals or removals (due to the max key constraint),
        // so the only request we can verify is the update one.
        val limitValue = new byte[KeyValueTable.MAXIMUM_VALUE_LENGTH];
        rnd.nextBytes(limitValue);
        val putBatchSizeExceeded = new ArrayList<TableModification>();
        int estimatedSize = 0;
        while (estimatedSize < TableSegment.MAXIMUM_BATCH_LENGTH) {
            val pk = new byte[getPrimaryKeyLength()];
            val sk = new byte[getSecondaryKeyLength()];
            rnd.nextBytes(pk);
            rnd.nextBytes(sk);
            val e = new Put(new TableKey(ByteBuffer.wrap(pk), ByteBuffer.wrap(sk)), ByteBuffer.wrap(limitValue));
            putBatchSizeExceeded.add(e);
            estimatedSize += pk.length + sk.length + limitValue.length;
        }

        AssertExtensions.assertSuppliedFutureThrows(
                "Put batch exceeded max size.",
                () -> kvt.update(putBatchSizeExceeded),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Verify that multi-get retrieval from a single segment of keys totalling more than the limit(s) works correctly.
     */
    @Test
    public void testLargeEntryBatchRetrieval() {
        val keyCount = TableSegment.MAXIMUM_BATCH_KEY_COUNT;

        Function<Integer, byte[]> getValue = keyId -> {
            val result = new byte[Long.BYTES];
            BitConverter.writeInt(result, 0, keyId + 1);
            return result;
        };

        @Cleanup
        val kvt = createKeyValueTable();

        // Update the entries one-by-one to make sure we do not exceed the max lengths at this step.
        val allKeys = new ArrayList<TableKey>();
        for (int keyId = 0; keyId < keyCount; keyId++) {
            val pk = new byte[getPrimaryKeyLength()];
            val sk = new byte[getSecondaryKeyLength()];
            BitConverter.writeLong(pk, 0, keyId);
            BitConverter.writeInt(sk, 0, keyId);
            val value = getValue.apply(keyId);
            kvt.update(new Put(new TableKey(ByteBuffer.wrap(pk), ByteBuffer.wrap(sk)), ByteBuffer.wrap(value))).join();
            allKeys.add(new TableKey(ByteBuffer.wrap(pk), ByteBuffer.wrap(sk)));
        }

        // Bulk-get all the keys. This should work regardless of the size of the data returned; the KeyValueTable and
        // TableSegment internally should break down the requests and handle this properly.
        val getResult = kvt.getAll(allKeys).join();
        Assert.assertEquals("Unexpected number of keys returned.", allKeys.size(), getResult.size());
        for (int keyId = 0; keyId < allKeys.size(); keyId++) {
            val r = getResult.get(keyId);
            val expectedKey = allKeys.get(keyId);

            Assert.assertTrue("Unexpected key at index " + keyId, areEqual(expectedKey, r.getKey()));
            val expectedValue = getValue.apply(keyId);
            Assert.assertEquals("Unexpected value at index " + keyId, ByteBuffer.wrap(expectedValue), r.getValue());
        }
    }

    /**
     * Tests functionality without secondary keys. Only single-key operations are tested because we cannot do multi-key
     * operations using different primary keys.
     * @throws Exception If an error occurred.
     */
    @Test
    public void testNoSecondaryKeys() throws Exception {
        val entryCount = getPrimaryKeyCount();
        val config = KeyValueTableConfiguration.builder()
                .partitionCount(DEFAULT_SEGMENT_COUNT)
                .primaryKeyLength(16)
                .secondaryKeyLength(0) // No Secondary Keys.
                .build();
        val rnd = new Random(0);

        @Cleanup
        val kvt = createKeyValueTable(new KeyValueTableInfo("Scope", "KVTNoSecondaryKeys"), config);

        List<ByteBuffer> keys = IntStream.range(0, entryCount)
                .mapToObj(i -> {
                    val keyData = new byte[config.getPrimaryKeyLength()];
                    rnd.nextBytes(keyData);
                    return ByteBuffer.wrap(keyData).asReadOnlyBuffer();
                })
                .collect(Collectors.toList());

        // Insertions.
        val iteration = new AtomicInteger(0);
        val versions = new HashMap<ByteBuffer, Version>();
        for (ByteBuffer k : keys) {
            Version v = kvt.update(new Insert(new TableKey(k), getValue(k, iteration.get()))).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            versions.put(k, v);
            AssertExtensions.assertSuppliedFutureThrows(
                    "Conditional insert did not fail for existing key.",
                    () -> kvt.update(new Insert(new TableKey(k), getValue(k, iteration.get()))),
                    ex -> ex instanceof BadKeyVersionException);
        }
        checkValues(iteration.get(), versions, kvt);

        // Updates.
        iteration.incrementAndGet();
        for (ByteBuffer k : keys) {
            Version v = versions.get(k);
            long pkValue = Math.abs(PK_SERIALIZER.deserialize(k));
            Put p = pkValue % 2 == 0
                    ? new Put(new TableKey(k), getValue(k, iteration.get()), v)
                    : new Put(new TableKey(k), getValue(k, iteration.get()));
            v = kvt.update(p).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            versions.put(k, v);
        }
        checkValues(iteration.get(), versions, kvt);

        // Removals.
        iteration.incrementAndGet();
        for (ByteBuffer k : keys) {
            Version v = versions.get(k);
            long pkValue = Math.abs(PK_SERIALIZER.deserialize(k));
            Remove r = pkValue % 2 == 0 ? new Remove(new TableKey(k), v) : new Remove(new TableKey(k));
            kvt.update(r).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            versions.put(k, null);
        }
        checkValues(iteration.get(), versions, kvt);
    }

    @Test
    public void testIterators() {
        @Cleanup
        val kvt = createKeyValueTable();
        val iteration = new AtomicInteger(0);

        // Populate everything.
        val versions = new Versions();
        forEveryPrimaryKey((pk, secondaryKeys) -> {
            List<TableModification> inserts = secondaryKeys.stream()
                    .map(sk -> new Insert(new TableKey(pk, sk), getValue(pk, sk, iteration.get())))
                    .collect(Collectors.toList());
            val keyVersions = kvt.update(inserts).join();
            for (int i = 0; i < inserts.size(); i++) {
                versions.add(getUniqueKeyId(pk, inserts.get(i).getKey().getSecondaryKey()), keyVersions.get(i));
            }
        });

        // Check the Primary Key iterator.
        checkPrimaryKeyIterator(kvt, versions, iteration.get());

        // Check the Global iterators.
        checkGlobalIterator(kvt, versions, iteration.get());
    }

    private void checkPrimaryKeyIterator(KeyValueTable keyValueTable, Versions versions, int iteration) {
        val itemsAtOnce = getSecondaryKeyCount() / 5;

        forEveryPrimaryKey((pk, secondaryKeys) -> {
            // Get all Table Keys and sort them.
            val allKeys = secondaryKeys.stream()
                    .map(sk -> new TableKey(pk, sk))
                    .sorted(KEY_COMPARATOR)
                    .collect(Collectors.toList());
            val allEntries = allKeys.stream()
                    .map(k -> new TableEntry(k, versions.get(getUniqueKeyId(k.getPrimaryKey(), k.getSecondaryKey())), getValue(k.getPrimaryKey(), k.getSecondaryKey(), iteration)))
                    .collect(Collectors.toList());

            // Issue various iterators (range) and verify against expected values.
            for (int startIndex = 0; startIndex < allKeys.size() / 2; startIndex++) {
                val endIndex = allKeys.size() - startIndex;
                val expectedKeys = allKeys.subList(startIndex, endIndex);
                val iterator = keyValueTable.iterator()
                        .maxIterationSize(itemsAtOnce)
                        .forPrimaryKey(pk, expectedKeys.get(0).getSecondaryKey(), expectedKeys.get(expectedKeys.size() - 1).getSecondaryKey());

                val iteratorKeys = new ArrayList<TableKey>();
                iterator.keys().collectRemaining(ii -> iteratorKeys.addAll(ii.getItems())).join();
                AssertExtensions.assertListEquals("Unexpected keys returned from iterator.keys()",
                        expectedKeys, iteratorKeys, this::areEqual);

                val expectedEntries = allEntries.subList(startIndex, endIndex);
                val iteratorEntries = new ArrayList<TableEntry>();
                iterator.entries().collectRemaining(ii -> iteratorEntries.addAll(ii.getItems())).join();
                AssertExtensions.assertListEquals("Unexpected entries returned from iterator.entries()",
                        expectedEntries, iteratorEntries, (e1, e2) -> areEqual(e1, e2) && e1.getVersion().equals(e2.getVersion()));
            }
        });
    }

    private void checkGlobalIterator(KeyValueTable keyValueTable, Versions versions, int iteration) {
        val itemsAtOnce = getSecondaryKeyCount() / 5;

        // All expected Entries, indexed by Keys.
        val allEntries = new HashMap<TableKey, TableEntry>();
        forEveryKey((pk, sk) -> {
            val k = new TableKey(pk, sk);
            val e = new TableEntry(k, versions.get(getUniqueKeyId(k.getPrimaryKey(), k.getSecondaryKey())), getValue(pk, sk, iteration));
            allEntries.put(k, e);
        });

        // All Keys, sorted.
        val allKeys = allEntries.keySet().stream()
                .sorted(KEY_COMPARATOR)
                .collect(Collectors.toList());

        // All unique Primary Keys.
        val allPrimaryKeys = allKeys.stream().map(TableKey::getPrimaryKey).distinct().sorted(KEY_COMPARATOR::compare).collect(Collectors.toList());

        // Issue various iterators (range) and verify against expected values.
        for (int startIndex = 0; startIndex < allPrimaryKeys.size() / 2; startIndex++) {
            val endIndex = allPrimaryKeys.size() - startIndex - 1;
            val firstPK = allPrimaryKeys.get(startIndex);
            val lastPK = allPrimaryKeys.get(endIndex);
            val expectedKeys = allKeys.stream()
                    .filter(k -> KEY_COMPARATOR.compare(k.getPrimaryKey(), firstPK) >= 0 && KEY_COMPARATOR.compare(k.getPrimaryKey(), lastPK) <= 0)
                    .collect(Collectors.toList());

            val iterator = keyValueTable.iterator()
                    .maxIterationSize(itemsAtOnce)
                    .forRange(firstPK, lastPK);

            val iteratorKeys = new ArrayList<TableKey>();
            iterator.keys().collectRemaining(ii -> iteratorKeys.addAll(ii.getItems())).join();
            AssertExtensions.assertListEquals("Unexpected keys returned from iterator.keys()",
                    expectedKeys, iteratorKeys, this::areEqual);

            val expectedEntries = expectedKeys.stream().map(allEntries::get).collect(Collectors.toList());
            val iteratorEntries = new ArrayList<TableEntry>();
            iterator.entries().collectRemaining(ii -> iteratorEntries.addAll(ii.getItems())).join();
            AssertExtensions.assertListEquals("Unexpected entries returned from iterator.entries()",
                    expectedEntries, iteratorEntries, (e1, e2) -> areEqual(e1, e2) && e1.getVersion().equals(e2.getVersion()));
        }

        // Check all() iterator.
        val allIterator = keyValueTable.iterator()
                .maxIterationSize(itemsAtOnce)
                .all();
        val expectedAllEntries = allKeys.stream().map(allEntries::get).collect(Collectors.toList());
        val allIteratorEntries = new ArrayList<TableEntry>();
        allIterator.entries().collectRemaining(ii -> allIteratorEntries.addAll(ii.getItems())).join();
        AssertExtensions.assertListEquals("Unexpected entries returned from allIterator.entries()",
                expectedAllEntries, allIteratorEntries, (e1, e2) -> areEqual(e1, e2) && e1.getVersion().equals(e2.getVersion()));
    }

    //endregion

    //region Helpers

    private void forEveryKey(BiConsumer<ByteBuffer, ByteBuffer> handler) {
        int secondaryCount = getSecondaryKeyCount();
        for (val pk : getPrimaryKeys()) {
            for (int skId = 0; skId < secondaryCount; skId++) {
                handler.accept(pk.duplicate(), SK_SERIALIZER.serialize(skId));
            }
        }
    }

    private void forEveryPrimaryKey(BiConsumer<ByteBuffer, List<ByteBuffer>> handler) {
        int secondaryCount = getSecondaryKeyCount();
        for (val pk : getPrimaryKeys()) {
            val sks = IntStream.range(0, secondaryCount).mapToObj(SK_SERIALIZER::serialize).collect(Collectors.toList());
            handler.accept(pk, sks);
        }
    }

    private Version alterVersion(Version original, boolean changeSegmentId, boolean changeVersion) {
        VersionImpl impl = original.asImpl();
        long newSegmentId = changeSegmentId ? impl.getSegmentId() + 1 : impl.getSegmentId();
        long newVersion = changeVersion ? impl.getSegmentVersion() + 1 : impl.getSegmentVersion();
        return new VersionImpl(newSegmentId, newVersion);
    }

    private void checkValues(int iteration, Versions versions, KeyValueTable keyValueTable) {
        // Check individually.
        forEveryKey((pk, sk) -> {
            val keyId = getUniqueKeyId(pk, sk);
            val expectedValue = getValue(pk, sk, iteration);
            val expectedVersion = versions.get(keyId);

            val requestKey = new TableKey(pk, sk);
            val actualEntry = keyValueTable.get(requestKey).join();
            checkValue(requestKey, expectedValue, expectedVersion, actualEntry, keyId);
        });

        // Check using getAll.
        forEveryPrimaryKey((pk, secondaryKeys) -> {
            val expectedVersions = secondaryKeys.stream()
                    .map(sk -> versions.get(getUniqueKeyId(pk, sk)))
                    .collect(Collectors.toList());
            val expectedValues = secondaryKeys.stream()
                    .map(sk -> getValue(pk, sk, iteration))
                    .collect(Collectors.toList());
            val requestKeys = secondaryKeys.stream().map(sk -> new TableKey(pk, sk)).collect(Collectors.toList());
            val result = keyValueTable.getAll(requestKeys).join();

            val hint = getUniqueKeyId(pk);
            Assert.assertEquals("Unexpected result size" + hint, requestKeys.size(), result.size());
            for (int i = 0; i < requestKeys.size(); i++) {
                checkValue(requestKeys.get(i), expectedValues.get(i), expectedVersions.get(i), result.get(i), hint);
            }
        });
    }

    private void checkValues(int iteration, Map<ByteBuffer, Version> keyVersions, KeyValueTable kvt) {
        // Using single get.
        for (val k : keyVersions.entrySet()) {
            val expectedValue = k.getValue() == null ? null : new TableEntry(new TableKey(k.getKey()), k.getValue(), getValue(k.getKey(), iteration));
            val actualEntry = kvt.get(new TableKey(k.getKey())).join();
            Assert.assertTrue(areEqual(expectedValue, actualEntry));
            boolean exists = kvt.exists(new TableKey(k.getKey())).join();
            Assert.assertEquals(expectedValue != null, exists);
        }

        // Using multi-get.
        val requestKeys = new ArrayList<>(keyVersions.entrySet());
        val entries = kvt.getAll(requestKeys.stream().map(k -> new TableKey(k.getKey())).collect(Collectors.toList())).join();
        Assert.assertEquals(keyVersions.size(), entries.size());
        for (int i = 0; i < requestKeys.size(); i++) {
            val k = requestKeys.get(i);
            val expectedValue = k.getValue() == null ? null : new TableEntry(new TableKey(k.getKey()), k.getValue(), getValue(k.getKey(), iteration));
            val actualEntry = entries.get(i);
            Assert.assertTrue(areEqual(expectedValue, actualEntry));
        }
    }

    private void checkValue(TableKey key, ByteBuffer expectedValue, Version expectedVersion, TableEntry actualEntry, Object hint) {
        if (expectedVersion == null) {
            // Key was removed or never inserted.
            Assert.assertNull("Not expecting a value for removed key" + hint, actualEntry);
        } else {
            // Key exists.
            Assert.assertEquals("Unexpected Primary Key" + hint, key.getPrimaryKey(), actualEntry.getKey().getPrimaryKey());
            Assert.assertEquals("Unexpected Secondary Key" + hint, key.getSecondaryKey(), actualEntry.getKey().getSecondaryKey());
            Assert.assertEquals("Unexpected version" + hint, expectedVersion, actualEntry.getVersion());
            Assert.assertEquals("Unexpected value" + hint, expectedValue, actualEntry.getValue());
        }
    }

    private boolean areEqual(@Nullable TableKey k1, @Nullable TableKey k2) {
        if (k1 == null) {
            Assert.assertNull(k2);
        }
        if (!k1.getPrimaryKey().equals(k2.getPrimaryKey())) {
            return false;
        }
        if (k1.getSecondaryKey() == null || k1.getSecondaryKey().remaining() == 0) {
            return k2.getSecondaryKey() == null || k2.getSecondaryKey().remaining() == 0;
        }

        return k1.getSecondaryKey().equals(k2.getSecondaryKey());
    }

    private boolean areEqual(@Nullable TableEntry e1, @Nullable TableEntry e2) {
        return (e1 == null && e2 == null)
                || (e1 != null && e2 != null && areEqual(e1.getKey(), e2.getKey()) && e1.getValue().equals(e2.getValue()));
    }

    private String getUniqueKeyId(ByteBuffer pk, ByteBuffer sk) {
        return String.format("(PK=%s, SK=%s)", PK_SERIALIZER.deserialize(pk),
                sk == null || sk.remaining() == 0 ? "[null]" : SK_SERIALIZER.deserialize(sk));
    }

    private String getUniqueKeyId(ByteBuffer pk) {
        return String.format("(PK=%s)", PK_SERIALIZER.deserialize(pk));
    }

    private ByteBuffer getValue(ByteBuffer pk, int iteration) {
        return getValue(pk, ByteBuffer.wrap(new byte[0]), iteration);
    }

    private ByteBuffer getValue(ByteBuffer pk, ByteBuffer sk, int iteration) {
        val result = ByteBuffer.allocate(pk.remaining() + sk.remaining() + Integer.BYTES);
        result.put(pk.duplicate());
        result.put(sk.duplicate());
        result.putInt(iteration);
        return result.asReadOnlyBuffer();
    }

    //endregion

    //region Helper classes

    private static class Versions {
        @Getter(AccessLevel.PACKAGE)
        private final Map<String, VersionImpl> versions = new ConcurrentHashMap<>();

        void add(String keyId, Version kv) {
            this.versions.put(keyId, kv.asImpl());
        }

        void remove(String keyId) {
            this.versions.remove(keyId);
        }

        VersionImpl get(String keyId) {
            return this.versions.getOrDefault(keyId, null);
        }

        boolean isEmpty() {
            return this.versions.isEmpty();
        }
    }

    private static class IntegerSerializer implements Serializer<Integer> {
        @Override
        public ByteBuffer serialize(Integer value) {
            return ByteBuffer.allocate(Integer.BYTES).putInt(0, value);
        }

        @Override
        public Integer deserialize(ByteBuffer serializedValue) {
            return serializedValue.duplicate().getInt();
        }
    }

    private static class LongSerializer implements Serializer<Long> {
        @Override
        public ByteBuffer serialize(Long value) {
            return ByteBuffer.allocate(Long.BYTES).putLong(0, value);
        }

        @Override
        public Long deserialize(ByteBuffer serializedValue) {
            return serializedValue.duplicate().getLong();
        }
    }

    //endregion
}
