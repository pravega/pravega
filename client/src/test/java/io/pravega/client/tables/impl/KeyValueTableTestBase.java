/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base test suite for anything testing {@link KeyValueTable}s. This covers core functionality for {@link KeyValueTable}s
 * and currently applies both to {@link KeyValueTableImplTests} (using mocked Controller and Segment Store) and
 * `io.pravega.test.integration.KeyValueTableImplTests` (using real Segment Store and Wire Protocol).
 */
public abstract class KeyValueTableTestBase extends KeyValueTableTestSetup {
    //region Tests

    /**
     * Tests the ability to perform single-key conditional insertions. These methods are exercised:
     * - {@link KeyValueTable#putIfAbsent}
     * - {@link KeyValueTable#get} and {@link KeyValueTable#getAll}
     */
    @Test
    public void testSingleKeyConditionalInserts() {
        val versions = new Versions();
        @Cleanup
        val kvt = createKeyValueTable();

        // PutIfAbsent (conditional insert)
        val iteration = new AtomicInteger(0);
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());

            // First one should work.
            Version kv = kvt.putIfAbsent(keyFamily, key, value).join();
            versions.add(keyFamily, keyId, kv);

            // Second one should throw.
            AssertExtensions.assertSuppliedFutureThrows(
                    "putIfAbsent did not throw for already existing key.",
                    () -> kvt.putIfAbsent(keyFamily, key, value),
                    ex -> ex instanceof BadKeyVersionException);
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, kvt);
    }

    /**
     * Tests the ability to perform single-key updates and replacements. These methods are exercised:
     * - {@link KeyValueTable#put}
     * - {@link KeyValueTable#replace}
     * - {@link KeyValueTable#get} and {@link KeyValueTable#getAll}
     */
    @Test
    public void testSingleKeyUpdates() {
        val versions = new Versions();
        @Cleanup
        val kvt = createKeyValueTable();

        // Put (unconditional update).
        val iteration = new AtomicInteger(0);
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());

            Version kv = kvt.put(keyFamily, key, value).join();
            versions.add(keyFamily, keyId, kv);
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, kvt);

        // Replace (conditional update (not insertion)).
        iteration.incrementAndGet();
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());
            val existingVersion = versions.get(keyFamily, keyId);

            // Verify that conditions are checked both for segment names and their versions.
            Version badVersion = alterVersion(existingVersion, keyId % 2 == 0, keyId % 2 == 1);
            AssertExtensions.assertSuppliedFutureThrows(
                    "replace did not throw for bad version.",
                    () -> kvt.replace(keyFamily, key, value, badVersion),
                    ex -> ex instanceof BadKeyVersionException);

            Version kv = kvt.replace(keyFamily, key, value, existingVersion).join();
            versions.add(keyFamily, keyId, kv);
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, kvt);
    }

    /**
     * Tests the ability to perform single-key updates and removals (conditional and unconditional). These methods are exercised:
     * - {@link KeyValueTable#put}
     * - {@link KeyValueTable#remove(String, Object)}
     * - {@link KeyValueTable#remove(String, Object, Version)}
     * - {@link KeyValueTable#get} and {@link KeyValueTable#getAll}
     */
    @Test
    public void testSingleKeyUnconditionalRemovals() {
        val versions = new Versions();
        @Cleanup
        val kvt = createKeyValueTable();

        // Put (unconditional update).
        val iteration = new AtomicInteger(0);
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());

            Version kv = kvt.put(keyFamily, key, value).join();
            versions.add(keyFamily, keyId, kv);
        });

        // Remove (both conditional and unconditional)
        iteration.incrementAndGet();
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val existingVersion = versions.get(keyFamily, keyId);

            // Verify that conditions are checked both for segment names and their versions.
            boolean conditional = keyId % 2 == 0;
            if (conditional) {
                // First check that a bad version will be checked.
                Version badVersion = alterVersion(existingVersion, keyId % 4 == 0, keyId % 4 != 0);
                AssertExtensions.assertSuppliedFutureThrows(
                        "remove did not throw for bad version.",
                        () -> kvt.remove(keyFamily, key, badVersion),
                        ex -> ex instanceof BadKeyVersionException);

                // Remove it.
                kvt.remove(keyFamily, key, existingVersion).join();
            } else {
                kvt.remove(keyFamily, key).join();

            }
            versions.remove(keyFamily, keyId);
        });
        Assert.assertTrue("Expected all keys to have been removed.", versions.isEmpty());
        checkValues(iteration.get(), versions, kvt);

        // Re-insert (conditionally).
        iteration.incrementAndGet();
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());

            // First one should work.
            Version kv = kvt.putIfAbsent(keyFamily, key, value).join();
            versions.add(keyFamily, keyId, kv);
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, kvt);
    }

    /**
     * Tests the ability to perform multi-key updates, replacements and removals. These methods should be exercised:
     * - {@link KeyValueTable#putAll}
     * - {@link KeyValueTable#replaceAll}
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
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val hint = String.format("(KF=%s)", keyFamily);
            val keys = keyIds.stream().map(this::getKey).collect(Collectors.toList());
            val entries = keyIds.stream().map(keyId -> TableEntry.notExists(getKey(keyId), getValue(keyId, iteration.get()))).collect(Collectors.toList());
            val keyVersions = kvt.replaceAll(keyFamily, entries).join();

            Assert.assertEquals("Unexpected result size" + hint, keys.size(), keyVersions.size());
            for (int i = 0; i < keys.size(); i++) {
                versions.add(keyFamily, keys.get(i), keyVersions.get(i));
            }
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, kvt);

        // Unconditional update.
        iteration.incrementAndGet();
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val hint = String.format("(KF=%s)", keyFamily);
            val keys = keyIds.stream().map(this::getKey).collect(Collectors.toList());
            val entries = keyIds.stream().collect(Collectors.toMap(this::getKey, keyId -> getValue(keyId, iteration.get())));
            val keyVersions = kvt.putAll(keyFamily, entries.entrySet()).join();

            Assert.assertEquals("Unexpected result size" + hint, keys.size(), keyVersions.size());
            for (int i = 0; i < keys.size(); i++) {
                versions.add(keyFamily, keys.get(i), keyVersions.get(i));
            }
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, kvt);

        // Conditional replace.
        iteration.incrementAndGet();
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val hint = String.format("(KF=%s)", keyFamily);
            val keys = keyIds.stream().map(this::getKey).collect(Collectors.toList());

            // Failed update (bad version).
            val badEntries = keyIds.stream()
                    .map(keyId -> TableEntry.versioned(
                            getKey(keyId),
                            alterVersion(versions.get(keyFamily, keyId), keyId % 3 < 1, keyId % 3 < 2),
                            getValue(keyId, iteration.get())))
                    .collect(Collectors.toList());
            AssertExtensions.assertSuppliedFutureThrows(
                    "replaceAll did not throw for bad version.",
                    () -> kvt.replaceAll(keyFamily, badEntries),
                    ex -> ex instanceof BadKeyVersionException);

            // Correct update.
            val entries = keyIds.stream()
                    .map(keyId -> TableEntry.versioned(getKey(keyId), versions.get(keyFamily, keyId), getValue(keyId, iteration.get())))
                    .collect(Collectors.toList());
            val keyVersions = kvt.replaceAll(keyFamily, entries).join();
            Assert.assertEquals("Unexpected result size" + hint, keys.size(), keyVersions.size());
            for (int i = 0; i < keys.size(); i++) {
                versions.add(keyFamily, keys.get(i), keyVersions.get(i));
            }
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, kvt);

        // Conditional removal.
        iteration.incrementAndGet();
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val hint = String.format("(KF=%s)", keyFamily);
            val keys = keyIds.stream().map(this::getKey).collect(Collectors.toList());

            // Failed update (bad version).
            val badKeys = keyIds.stream()
                    .map(keyId -> TableKey.versioned(getKey(keyId), alterVersion(versions.get(keyFamily, keyId), keyId % 3 < 1, keyId % 3 < 2)))
                    .collect(Collectors.toList());
            AssertExtensions.assertSuppliedFutureThrows(
                    "removeAll did not throw for bad version." + hint,
                    () -> kvt.removeAll(keyFamily, badKeys),
                    ex -> ex instanceof BadKeyVersionException);

            // Correct update.
            val keysToRemove = keyIds.stream()
                    .map(keyId -> TableKey.versioned(getKey(keyId), versions.get(keyFamily, keyId)))
                    .collect(Collectors.toList());
            kvt.removeAll(keyFamily, keysToRemove).join();
            for (val key : keys) {
                versions.remove(keyFamily, key);
            }
        });
        Assert.assertTrue("Expected all keys to have been removed.", versions.isEmpty());
        checkValues(iteration.get(), versions, kvt);

        // Reinsert (conditionally)
        iteration.incrementAndGet();
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val hint = String.format("(KF=%s)", keyFamily);
            val keys = keyIds.stream().map(this::getKey).collect(Collectors.toList());
            val entries = keyIds.stream().map(keyId -> TableEntry.notExists(getKey(keyId), getValue(keyId, iteration.get()))).collect(Collectors.toList());
            val keyVersions = kvt.replaceAll(keyFamily, entries).join();

            Assert.assertEquals("Unexpected result size" + hint, keys.size(), keyVersions.size());
            for (int i = 0; i < keys.size(); i++) {
                versions.add(keyFamily, keys.get(i), keyVersions.get(i));
            }
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, kvt);
    }

    @Test
    public void testIterators() {
        @Cleanup
        val kvt = createKeyValueTable();
        val iteration = new AtomicInteger(0);

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
    }

    private <ItemT> void checkIterator(KeyValueTable<Integer, String> keyValueTable, InvokeIterator<ItemT> invokeIterator,
                                       Function<ItemT, TableKey<Integer>> getKeyFromItem,
                                       Function<TableEntry<Integer, String>, ItemT> getItemFromEntry,
                                       BiPredicate<ItemT, ItemT> areEqual) {
        val itemsAtOnce = getKeysPerKeyFamily() / 5;

        BiPredicate<IteratorItem<ItemT>, IteratorItem<ItemT>> iteratorItemEquals = (e, a) ->
                AssertExtensions.listEquals(e.getItems(), a.getItems(), areEqual)
                        && e.getState().toBytes().equals(a.getState().toBytes());

        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
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

    //endregion

    //region Helpers

    private void checkSegmentDistributions(Versions v) {
        v.getVersions().forEach((keyFamily, versions) -> {
            val segments = versions.values().stream().map(VersionImpl::getSegmentId).distinct().collect(Collectors.toList());
            if (keyFamily.equals(NULL_KEY_FAMILY)) {
                AssertExtensions.assertGreaterThan("Keys without families were not distributed to multiple segments.",
                        1, segments.size());
            } else {
                // Verify that all KeyVersions go to the same segment.
                Assert.assertEquals("Keys for Key Family " + keyFamily + " were distributed to multiple segments: " + segments,
                        1, segments.size());
            }
        });
    }

    private Version alterVersion(Version original, boolean changeSegmentId, boolean changeVersion) {
        VersionImpl impl = original.asImpl();
        long newSegmentId = changeSegmentId ? impl.getSegmentId() + 1 : impl.getSegmentId();
        long newVersion = changeVersion ? impl.getSegmentVersion() + 1 : impl.getSegmentVersion();
        return new VersionImpl(newSegmentId, newVersion);
    }

    private boolean areEqualExcludingVersion(TableKey<Integer> k1, TableKey<Integer> k2) {
        return k1.getKey().equals(k2.getKey());
    }

    private boolean areEqual(TableKey<Integer> k1, TableKey<Integer> k2) {
        return areEqualExcludingVersion(k1, k2) && k1.getVersion().equals(k2.getVersion());
    }

    private boolean areEqual(TableEntry<Integer, String> e1, TableEntry<Integer, String> e2) {
        return areEqual(e1.getKey(), e2.getKey()) && e1.getValue().equals(e2.getValue());
    }

    //endregion

    //region Helper classes

    @FunctionalInterface
    private interface InvokeIterator<T> {
        AsyncIterator<IteratorItem<T>> apply(KeyValueTable<Integer, String> kvt, String keyFamily, int itemsAtOnce, IteratorState state);
    }

    //endregion
}
