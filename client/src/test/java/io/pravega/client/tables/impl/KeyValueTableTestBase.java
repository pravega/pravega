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

import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.IteratorState;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.Version;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BitConverter;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Base test suite for anything testing {@link KeyValueTable}s. This covers core functionality for {@link KeyValueTable}s
 * and currently applies both to {@link KeyValueTableImplTests} (using mocked Controller and Segment Store) and
 * `io.pravega.test.integration.KeyValueTableImplTests` (using real Segment Store and Wire Protocol).
 */
public abstract class KeyValueTableTestBase extends KeyValueTableTestSetup {
    
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Getter
    @Rule
    public final Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    /**
     * Tests the ability to perform single-key conditional insertions. These methods are exercised:
     * - {@link KeyValueTable#put} with {@link TableEntry} instances created using {@link TableEntry#notExists}.
     * - {@link KeyValueTable#get} and {@link KeyValueTable#getAll}
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
            val entry = TableEntry.notExists(pk, sk, value);
            Version kv = kvt.put(entry).join();
            versions.add(getUniqueKeyId(pk, sk), kv);

            // Second one should throw.
            AssertExtensions.assertSuppliedFutureThrows(
                    "put(If-Not-Exists) did not throw for already existing key.",
                    () -> kvt.put(entry),
                    ex -> ex instanceof BadKeyVersionException);
        });
        checkValues(iteration.get(), versions, kvt);
    }

    /**
     * Tests the ability to perform single-key updates and replacements. These methods are exercised:
     * - {@link KeyValueTable#put}
     * - {@link KeyValueTable#get} and {@link KeyValueTable#getAll}
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

            val entry = TableEntry.unversioned(pk, sk, value);
            Version kv = kvt.put(entry).join();
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
                    "replace did not throw for bad version.",
                    () -> kvt.put(TableEntry.versioned(pk, sk, badVersion, value)),
                    ex -> ex instanceof BadKeyVersionException);

            Version kv = kvt.put(TableEntry.versioned(pk, sk, existingVersion, value)).join();
            versions.add(keyId, kv);
        });
        checkValues(iteration.get(), versions, kvt);
    }

    /**
     * Tests the ability to perform single-key updates and removals (conditional and unconditional). These methods are exercised:
     * - {@link KeyValueTable#put}
     * - {@link KeyValueTable#remove}
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

            Version kv = kvt.put(TableEntry.unversioned(pk, sk, value)).join();
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
                        "remove did not throw for bad version.",
                        () -> kvt.remove(TableKey.versioned(pk, sk, badVersion)),
                        ex -> ex instanceof BadKeyVersionException);

                // Remove it.
                kvt.remove(TableKey.versioned(pk, sk, existingVersion)).join();
            } else {
                kvt.remove(TableKey.unversioned(pk, sk)).join();

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
            Version kv = kvt.put(TableEntry.notExists(pk, sk, value)).join();
            versions.add(getUniqueKeyId(pk, sk), kv);
        });
        checkValues(iteration.get(), versions, kvt);
    }

    /**
     * Tests the ability to perform multi-key updates, replacements and removals. These methods should be exercised:
     * - {@link KeyValueTable#putAll}
     * - {@link KeyValueTable#removeAll}
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
            val entries = secondaryKeys.stream()
                    .map(sk -> TableEntry.notExists(pk, sk, getValue(pk, sk, iteration.get())))
                    .collect(Collectors.toList());
            val keyVersions = kvt.putAll(entries).join();

            val hint = getUniqueKeyId(pk);
            Assert.assertEquals("Unexpected result size" + hint, entries.size(), keyVersions.size());
            for (int i = 0; i < entries.size(); i++) {
                versions.add(getUniqueKeyId(pk, entries.get(i).getKey().getSecondaryKey()), keyVersions.get(i));
            }
        });
        checkValues(iteration.get(), versions, kvt);

        // Unconditional update.
        iteration.incrementAndGet();
        forEveryPrimaryKey((pk, secondaryKeys) -> {
            val entries = secondaryKeys.stream()
                    .map(sk -> TableEntry.unversioned(pk, sk, getValue(pk, sk, iteration.get())))
                    .collect(Collectors.toList());
            val keyVersions = kvt.putAll(entries).join();

            val hint = getUniqueKeyId(pk);
            Assert.assertEquals("Unexpected result size" + hint, entries.size(), keyVersions.size());
            for (int i = 0; i < entries.size(); i++) {
                versions.add(getUniqueKeyId(pk, entries.get(i).getKey().getSecondaryKey()), keyVersions.get(i));
            }
        });
        checkValues(iteration.get(), versions, kvt);

        // Conditional replace.
        iteration.incrementAndGet();
        forEveryPrimaryKey((pk, secondaryKeys) -> {
            // Failed update (bad version).
            val pkValue = Math.abs(PK_SERIALIZER.deserialize(pk));
            val badEntries = secondaryKeys.stream()
                    .map(sk -> TableEntry.versioned(pk, sk,
                            alterVersion(versions.get(getUniqueKeyId(pk, sk)), pkValue % 2 == 0, pkValue % 2 == 1),
                            getValue(pk, sk, iteration.get())))
                    .collect(Collectors.toList());
            System.out.println();
            AssertExtensions.assertSuppliedFutureThrows(
                    "putAll did not throw for bad version.",
                    () -> kvt.putAll(badEntries),
                    ex -> ex instanceof BadKeyVersionException);

            // Correct update.
            val entries = secondaryKeys.stream()
                    .map(sk -> TableEntry.versioned(pk, sk, versions.get(getUniqueKeyId(pk, sk)), getValue(pk, sk, iteration.get())))
                    .collect(Collectors.toList());
            val keyVersions = kvt.putAll(entries).join();
            val hint = getUniqueKeyId(pk);
            Assert.assertEquals("Unexpected result size" + hint, entries.size(), keyVersions.size());
            for (int i = 0; i < entries.size(); i++) {
                versions.add(getUniqueKeyId(pk, entries.get(i).getKey().getSecondaryKey()), keyVersions.get(i));
            }
        });
        checkValues(iteration.get(), versions, kvt);

        // Conditional removal.
        iteration.incrementAndGet();
        forEveryPrimaryKey((pk, secondaryKeys) -> {
            val hint = getUniqueKeyId(pk);

            // Failed update (bad version).
            val pkValue = Math.abs(PK_SERIALIZER.deserialize(pk));
            val badKeys = secondaryKeys.stream()
                    .map(sk -> TableKey.versioned(pk, sk, alterVersion(versions.get(getUniqueKeyId(pk, sk)), pkValue % 2 == 0, pkValue % 2 == 1)))
                    .collect(Collectors.toList());
            AssertExtensions.assertSuppliedFutureThrows(
                    "removeAll did not throw for bad version." + hint,
                    () -> kvt.removeAll(badKeys),
                    ex -> ex instanceof BadKeyVersionException);

            // Correct update.
            val keysToRemove = secondaryKeys.stream()
                    .map(sk -> TableKey.versioned(pk, sk, versions.get(getUniqueKeyId(pk, sk))))
                    .collect(Collectors.toList());
            kvt.removeAll(keysToRemove).join();
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
            val entries = secondaryKeys.stream()
                    .map(sk -> TableEntry.notExists(pk, sk, getValue(pk, sk, iteration.get())))
                    .collect(Collectors.toList());
            val keyVersions = kvt.putAll(entries).join();

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
        val limitValue = ByteBuffer.wrap(new byte[KeyValueTable.MAXIMUM_SERIALIZED_VALUE_LENGTH]);
        rnd.nextBytes(limitPK.array());
        rnd.nextBytes(limitSK.array());
        rnd.nextBytes(limitValue.array());
        @Cleanup
        val kvt = createKeyValueTable();
        kvt.put(TableEntry.unversioned(limitPK.duplicate(), limitSK.duplicate(), limitValue.duplicate())).join();
        val resultValue = kvt.get(TableKey.unversioned(limitPK.duplicate(), limitSK.duplicate())).join().getValue();
        Assert.assertEquals("Unexpected value returned.", limitValue.duplicate(), resultValue.duplicate());

        // Wrong Key Length
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a PK that is too long.",
                () -> kvt.put(TableEntry.unversioned(longPK.duplicate(), limitSK.duplicate(), limitValue.duplicate())),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a PK that is too long.",
                () -> kvt.remove(TableKey.unversioned(longPK.duplicate(), limitSK.duplicate())),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a PK that is too short.",
                () -> kvt.put(TableEntry.unversioned(shortPK.duplicate(), limitSK.duplicate(), limitValue.duplicate())),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a PK that is too short.",
                () -> kvt.remove(TableKey.unversioned(shortPK.duplicate(), limitSK.duplicate())),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a SK that is too long.",
                () -> kvt.put(TableEntry.unversioned(limitPK.duplicate(), longSK.duplicate(), limitValue.duplicate())),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a SK that is too long.",
                () -> kvt.remove(TableKey.unversioned(limitPK.duplicate(), longSK.duplicate())),
                ex -> ex instanceof IllegalArgumentException);

        if (getSecondaryKeyLength() > 0) {
            val shortSK = ByteBuffer.allocate(getSecondaryKeyLength() - 1);
            AssertExtensions.assertSuppliedFutureThrows(
                    "Expected a rejection of a SK that is too short.",
                    () -> kvt.put(TableEntry.unversioned(limitPK.duplicate(), shortSK.duplicate(), limitValue.duplicate())),
                    ex -> ex instanceof IllegalArgumentException);
            AssertExtensions.assertSuppliedFutureThrows(
                    "Expected a rejection of a SK that is too short.",
                    () -> kvt.remove(TableKey.unversioned(limitPK.duplicate(), shortSK.duplicate())),
                    ex -> ex instanceof IllegalArgumentException);
        }

        // Max Value Length exceeded.
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a rejection of a value that is too long.",
                () -> kvt.put(TableEntry.unversioned(limitPK.duplicate(), limitSK.duplicate(), ByteBuffer.allocate(KeyValueTable.MAXIMUM_SERIALIZED_VALUE_LENGTH + 1))),
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
                .mapToObj(i -> TableKey.unversioned(ByteBuffer.allocate(getPrimaryKeyLength()).putLong(i), ByteBuffer.allocate(getSecondaryKeyLength()).putInt(i)))
                .collect(Collectors.toList());

        val getBatchCountExceeded = keys.stream()
                .map(k -> TableKey.unversioned(k.getPrimaryKey().duplicate(), k.getSecondaryKey().duplicate()))
                .collect(Collectors.toList());
        AssertExtensions.assertSuppliedFutureThrows(
                "Get batch exceeded max count.",
                () -> kvt.getAll(getBatchCountExceeded),
                ex -> ex instanceof IllegalArgumentException);

        val putBatchCountExceeded = keys.stream()
                .map(a -> TableEntry.unversioned(a.getPrimaryKey().duplicate(), a.getSecondaryKey().duplicate(), a.getSecondaryKey().duplicate()))
                .collect(Collectors.toList());
        AssertExtensions.assertSuppliedFutureThrows(
                "Put batch exceeded max count.",
                () -> kvt.putAll(putBatchCountExceeded),
                ex -> ex instanceof IllegalArgumentException);

        val removeBatchCountExceeded = keys.stream()
                .map(k -> TableKey.unversioned(k.getPrimaryKey().duplicate(), k.getSecondaryKey().duplicate()))
                .collect(Collectors.toList());
        AssertExtensions.assertSuppliedFutureThrows(
                "Remove batch exceeded max count.",
                () -> kvt.removeAll(removeBatchCountExceeded),
                ex -> ex instanceof IllegalArgumentException);

        // Exceed by serialization size.
        // It is impossible to exceed the serialization size for retrievals or removals (due to the max key constraint),
        // so the only request we can verify is the update one.
        val limitValue = new byte[KeyValueTable.MAXIMUM_SERIALIZED_VALUE_LENGTH];
        rnd.nextBytes(limitValue);
        val putBatchSizeExceeded = new ArrayList<TableEntry>();
        int estimatedSize = 0;
        while (estimatedSize < TableSegment.MAXIMUM_BATCH_LENGTH) {
            val pk = new byte[getPrimaryKeyLength()];
            val sk = new byte[getSecondaryKeyLength()];
            rnd.nextBytes(pk);
            rnd.nextBytes(sk);
            val e = TableEntry.unversioned(ByteBuffer.wrap(pk), ByteBuffer.wrap(sk), ByteBuffer.wrap(limitValue));
            putBatchSizeExceeded.add(e);
            estimatedSize += pk.length + sk.length + limitValue.length;
        }

        AssertExtensions.assertSuppliedFutureThrows(
                "Put batch exceeded max size.",
                () -> kvt.putAll(putBatchSizeExceeded),
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
            kvt.put(TableEntry.unversioned(ByteBuffer.wrap(pk), ByteBuffer.wrap(sk), ByteBuffer.wrap(value))).join();
            allKeys.add(TableKey.unversioned(ByteBuffer.wrap(pk), ByteBuffer.wrap(sk)));
        }

        // Bulk-get all the keys. This should work regardless of the size of the data returned; the KeyValueTable and
        // TableSegment internally should break down the requests and handle this properly.
        val getResult = kvt.getAll(allKeys).join();
        Assert.assertEquals("Unexpected number of keys returned.", allKeys.size(), getResult.size());
        for (int keyId = 0; keyId < allKeys.size(); keyId++) {
            val r = getResult.get(keyId);
            val expectedKey = allKeys.get(keyId);

            Assert.assertTrue("Unexpected key at index " + keyId, areEqualExcludingVersion(expectedKey, r.getKey()));
            val expectedValue = getValue.apply(keyId);
            Assert.assertEquals("Unexpected value at index " + keyId, ByteBuffer.wrap(expectedValue), r.getValue());
        }
    }

    @Test
    @Ignore("https://github.com/pravega/pravega/issues/5941") // TODO
    public void testIterators() {
        /*
        @Cleanup
        val kvt = createKeyValueTable();
        val iteration = new AtomicInteger(0);
cl
        // Populate everything.
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());
            kvt.putIfAbsent(keyFamily, key, value).join();
        });

        // Check the key iterator. Keys are returned without versions.
        checkIterator(kvt, KeyValueTable::keyIterator, k -> k, TableEntry::getKey, this::areEqualExcludingVersion);

        // Check the entry iterator. Entries are returned with versions.
        checkIterator(kvt, KeyValueTable::entryIterator, TableEntry::getKey, e -> e, this::areEqual);
         */
    }

    /*
    private <ItemT> void checkIterator(KeyValueTable<Integer, String> keyValueTable, InvokeIterator<ItemT> invokeIterator,
                                       Function<ItemT, TableKey<Integer>> getKeyFromItem,
                                       Function<TableEntry<Integer, String>, ItemT> getItemFromEntry,
                                       BiPredicate<ItemT, ItemT> areEqual) {
        val itemsAtOnce = getSecondaryKeyCount() / 5;

        BiPredicate<IteratorItem<ItemT>, IteratorItem<ItemT>> iteratorItemEquals = (e, a) ->
                AssertExtensions.listEquals(e.getItems(), a.getItems(), areEqual)
                        && e.getState().toBytes().equals(a.getState().toBytes());

        forEveryPrimaryKey(false, (keyFamily, keyIds) -> {
            val hint = String.format("(KF=%s)", keyFamily);

            // Collect all the items from the beginning.
            val iteratorResults = new ArrayList<IteratorItem<ItemT>>();
            invokeIterator.apply(keyValueTable, keyFamily, itemsAtOnce, null)
                    .forEachRemaining(iteratorResults::add, executorService()).join();

            // Order them by Key.
            val keys = keyIds.stream().map(this::getKey).sorted().collect(Collectors.toList());
            val actualKeys = iteratorResults.stream()
                    .flatMap(ii -> ii.getItems().stream())
                    .sorted(Comparator.comparingInt(e -> getKeyFromItem.apply(e).getKey()))
                    .collect(Collectors.toList());
            Assert.assertEquals("Unexpected item count" + hint, keys.size(), actualKeys.size());
            for (int i = 0; i < keys.size(); i++) {
                val tableEntry = keyValueTable.get(keyFamily, keys.get(i)).join();
                val actualItem = getItemFromEntry.apply(tableEntry);
                Assert.assertTrue("Unexpected entry at position " + i + " " + hint, areEqual.test(actualItem, actualKeys.get(i)));
            }

            // Now issue "resumed" iterators. We want to verify that we are recording the correct IteratorState and that
            // when we issue a new iterator with that state, we are able to resume the iteration.
            while (!iteratorResults.isEmpty()) {
                IteratorState requestState = iteratorResults.remove(0).getState();
                val resumedResults = new ArrayList<IteratorItem<ItemT>>();
                invokeIterator.apply(keyValueTable, keyFamily, itemsAtOnce, requestState)
                        .forEachRemaining(resumedResults::add, executorService()).join();
                AssertExtensions.assertListEquals("Resumed iterators not consistent" + hint, iteratorResults, resumedResults, iteratorItemEquals);
            }
        });
    }
    */
    //endregion

    //region Helpers

    private Version alterVersion(Version original, boolean changeSegmentId, boolean changeVersion) {
        VersionImpl impl = original.asImpl();
        long newSegmentId = changeSegmentId ? impl.getSegmentId() + 1 : impl.getSegmentId();
        long newVersion = changeVersion ? impl.getSegmentVersion() + 1 : impl.getSegmentVersion();
        return new VersionImpl(newSegmentId, newVersion);
    }

    private boolean areEqualExcludingVersion(TableKey k1, TableKey k2) {
        return k1.getPrimaryKey().equals(k2.getPrimaryKey())
                && k1.getSecondaryKey().equals(k2.getSecondaryKey());
    }

    private boolean areEqual(TableKey k1, TableKey k2) {
        return areEqualExcludingVersion(k1, k2) && k1.getVersion().equals(k2.getVersion());
    }

    private boolean areEqual(TableEntry e1, TableEntry e2) {
        return areEqual(e1.getKey(), e2.getKey()) && e1.getValue().equals(e2.getValue());
    }

    //endregion

    //region Helper classes

    @FunctionalInterface
    private interface InvokeIterator<T> {
        AsyncIterator<IteratorItem<T>> apply(KeyValueTable kvt, String keyFamily, int itemsAtOnce, IteratorState state);
    }

    //endregion
}
