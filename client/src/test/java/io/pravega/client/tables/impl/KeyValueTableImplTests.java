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

import io.netty.buffer.ByteBuf;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.KeyVersion;
import io.pravega.client.tables.NoSuchKeyException;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.common.Exceptions;
import io.pravega.common.util.AsyncIterator;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link KeyValueTableImpl} class.
 */
public class KeyValueTableImplTests extends ThreadPooledTestSuite {
    private static final KeyValueTableInfo KVT = new KeyValueTableInfo("Scope", "KVT");
    private static final Serializer<Integer> KEY_SERIALIZER = new IntegerSerializer();
    private static final Serializer<String> VALUE_SERIALIZER = new UTF8StringSerializer();
    private static final int SEGMENT_COUNT = 4;
    private static final int KEY_FAMILY_COUNT = 100;
    private static final int KEYS_PER_KEY_FAMILY = 10;
    private static final int KEYS_WITHOUT_KEY_FAMILY = KEY_FAMILY_COUNT * KEYS_PER_KEY_FAMILY;
    private static final String NULL_KEY_FAMILY = "[NULL]"; // Used for HashMap keys.

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    /**
     * Tests the ability to perform single-key conditional insertions. These methods are exercised:
     * - {@link KeyValueTable#putIfAbsent}
     * - {@link KeyValueTable#get} and {@link KeyValueTable#getAll}
     */
    @Test
    public void testSingleKeyConditionalInserts() {
        @Cleanup
        val context = new TestContext();
        val versions = new Versions();
        val kvt = context.keyValueTable;

        // PutIfAbsent (conditional insert)
        val iteration = new AtomicInteger(0);
        forEveryKey(context, (keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());

            // First one should work.
            KeyVersion kv = kvt.putIfAbsent(keyFamily, key, value).join();
            versions.add(keyFamily, keyId, kv);

            // Second one should throw.
            AssertExtensions.assertSuppliedFutureThrows(
                    "putIfAbsent did not throw for already existing key.",
                    () -> kvt.putIfAbsent(keyFamily, key, value),
                    ex -> ex instanceof BadKeyVersionException);
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, context);
    }

    /**
     * Tests the ability to perform single-key updates and replacements. These methods are exercised:
     * - {@link KeyValueTable#put}
     * - {@link KeyValueTable#replace}
     * - {@link KeyValueTable#get} and {@link KeyValueTable#getAll}
     */
    @Test
    public void testSingleKeyUpdates() {
        @Cleanup
        val context = new TestContext();
        val versions = new Versions();
        val kvt = context.keyValueTable;

        // Put (unconditional update).
        val iteration = new AtomicInteger(0);
        forEveryKey(context, (keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());

            KeyVersion kv = kvt.put(keyFamily, key, value).join();
            versions.add(keyFamily, keyId, kv);
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, context);

        // Replace (conditional update (not insertion)).
        iteration.incrementAndGet();
        forEveryKey(context, (keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());
            val existingVersion = versions.get(keyFamily, keyId);

            // Verify that conditions are checked both for segment names and their versions.
            KeyVersion badVersion = alterVersion(existingVersion, keyId % 2 == 0, keyId % 2 == 1);
            AssertExtensions.assertSuppliedFutureThrows(
                    "replace did not throw for bad version.",
                    () -> kvt.replace(keyFamily, key, value, badVersion),
                    ex -> ex instanceof BadKeyVersionException);

            KeyVersion kv = kvt.replace(keyFamily, key, value, existingVersion).join();
            versions.add(keyFamily, keyId, kv);
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, context);
    }

    /**
     * Tests the ability to perform single-key updates and removals (conditional and unconditional). These methods are exercised:
     * - {@link KeyValueTable#put}
     * - {@link KeyValueTable#remove(String, Object)}
     * - {@link KeyValueTable#remove(String, Object, KeyVersion)}
     * - {@link KeyValueTable#get} and {@link KeyValueTable#getAll}
     */
    @Test
    public void testSingleKeyUnconditionalRemovals() {
        @Cleanup
        val context = new TestContext();
        val versions = new Versions();
        val kvt = context.keyValueTable;

        // Put (unconditional update).
        val iteration = new AtomicInteger(0);
        forEveryKey(context, (keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());

            KeyVersion kv = kvt.put(keyFamily, key, value).join();
            versions.add(keyFamily, keyId, kv);
        });

        // Remove (both conditional and unconditional)
        iteration.incrementAndGet();
        forEveryKey(context, (keyFamily, keyId) -> {
            val key = getKey(keyId);
            val existingVersion = versions.get(keyFamily, keyId);

            // Verify that conditions are checked both for segment names and their versions.
            boolean conditional = keyId % 2 == 0;
            if (conditional) {
                // First check that a bad version will be checked.
                KeyVersion badVersion = alterVersion(existingVersion, keyId % 4 == 0, keyId % 4 != 0);
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
        checkValues(iteration.get(), versions, context);

        // Re-insert (conditionally).
        iteration.incrementAndGet();
        forEveryKey(context, (keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());

            // First one should work.
            KeyVersion kv = kvt.putIfAbsent(keyFamily, key, value).join();
            versions.add(keyFamily, keyId, kv);
        });
        checkSegmentDistributions(versions);
        checkValues(iteration.get(), versions, context);
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
        @Cleanup
        val context = new TestContext();
        val versions = new Versions();
        val kvt = context.keyValueTable;

        // Conditional Insert.
        val iteration = new AtomicInteger(0);
        forEveryKeyFamily(false, context, (keyFamily, keyIds) -> {
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
        checkValues(iteration.get(), versions, context);

        // Unconditional update.
        iteration.incrementAndGet();
        forEveryKeyFamily(false, context, (keyFamily, keyIds) -> {
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
        checkValues(iteration.get(), versions, context);

        // Conditional replace.
        iteration.incrementAndGet();
        forEveryKeyFamily(false, context, (keyFamily, keyIds) -> {
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
        checkValues(iteration.get(), versions, context);

        // Conditional removal.
        iteration.incrementAndGet();
        forEveryKeyFamily(false, context, (keyFamily, keyIds) -> {
            val hint = String.format("(KF=%s)", keyFamily);
            val keys = keyIds.stream().map(this::getKey).collect(Collectors.toList());

            // Failed update (bad version).
            val badKeys = keyIds.stream()
                    .map(keyId -> TableKey.versioned(getKey(keyId), alterVersion(versions.get(keyFamily, keyId), keyId % 3 < 1, keyId % 3 < 2)))
                    .collect(Collectors.toList());
            AssertExtensions.assertSuppliedFutureThrows(
                    "removeAll did not throw for bad version.",
                    () -> kvt.removeAll(keyFamily, badKeys),
                    ex -> ex instanceof BadKeyVersionException);

            // Correct update.
            val keysToRemove = keyIds.stream()
                    .map(keyId -> TableKey.versioned(getKey(keyId), versions.get(keyFamily, keyId)))
                    .collect(Collectors.toList());
            kvt.removeAll(keyFamily, keysToRemove).join();
            for (int i = 0; i < keys.size(); i++) {
                versions.remove(keyFamily, keys.get(i));
            }
        });
        Assert.assertTrue("Expected all keys to have been removed.", versions.isEmpty());
        checkValues(iteration.get(), versions, context);

        // Reinsert (conditionally)
        iteration.incrementAndGet();
        forEveryKeyFamily(false, context, (keyFamily, keyIds) -> {
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
        checkValues(iteration.get(), versions, context);
    }

    @Test
    public void testKeyIterator() {
        // TODO
    }

    @Test
    public void testEntryIterator() {
        // TODO
    }

    /**
     * Tests the {@link KeyValueTable#close()} method.
     */
    @Test
    public void testClose() {

    }

    private void checkValues(int iteration, Versions versions, TestContext context) {
        // Check individually.
        forEveryKey(context, (keyFamily, keyId) -> {
            val hint = String.format("(KF=%s, Key=%s)", keyFamily, keyId);
            val key = getKey(keyId);
            val expectedValue = getValue(keyId, iteration);
            val expectedVersion = versions.get(keyFamily, keyId);

            val actualEntry = context.keyValueTable.get(keyFamily, key).join();
            checkValue(key, expectedValue, expectedVersion, actualEntry, hint);
        });

        // Check using getAll.
        forEveryKeyFamily(context, (keyFamily, keyIds) -> {
            val hint = String.format("(KF=%s)", keyFamily);
            val keys = keyIds.stream().map(this::getKey).collect(Collectors.toList());
            val expectedVersions = keyIds.stream().map(keyId -> versions.get(keyFamily, keyId)).collect(Collectors.toList());
            val expectedValues = keyIds.stream().map(keyId -> getValue(keyId, iteration)).collect(Collectors.toList());
            val result = context.keyValueTable.getAll(keyFamily, keys).join();

            Assert.assertEquals("Unexpected result size" + hint, keys.size(), result.size());
            for (int i = 0; i < keys.size(); i++) {
                checkValue(keys.get(i), expectedValues.get(i), expectedVersions.get(i), result.get(i), hint);
            }
        });
    }

    private void checkValue(Integer key, String expectedValue, KeyVersion expectedVersion, TableEntry<Integer, String> actualEntry, String hint) {
        Assert.assertEquals("Unexpected key" + hint, key, actualEntry.getKey().getKey());
        if (expectedVersion == null) {
            // Key was removed or never inserted.
            Assert.assertNull("Not expecting a value for removed key" + hint, actualEntry.getValue());
            Assert.assertEquals("", KeyVersion.NOT_EXISTS.asImpl().getSegmentVersion(), actualEntry.getKey().getVersion().asImpl().getSegmentVersion());
        } else {
            // Key exists.
            Assert.assertEquals("Unexpected version" + hint, expectedVersion, actualEntry.getKey().getVersion());
            Assert.assertEquals("Unexpected value" + hint, expectedValue, actualEntry.getValue());
        }
    }

    private void checkSegmentDistributions(Versions v) {
        v.versions.forEach((keyFamily, versions) -> {
            val segments = versions.values().stream().map(KeyVersionImpl::getSegmentName).distinct().collect(Collectors.toList());
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

    private void forEveryKey(TestContext context, BiConsumer<String, Integer> handler) {
        for (val keyFamily : context.keyFamilies) {
            int keyCount = keyFamily == null ? KEYS_WITHOUT_KEY_FAMILY : KEYS_PER_KEY_FAMILY;
            for (int keyId = 0; keyId < keyCount; keyId++) {
                handler.accept(keyFamily, keyId);
            }
        }
    }

    private void forEveryKeyFamily(TestContext context, BiConsumer<String, List<Integer>> handler) {
        forEveryKeyFamily(true, context, handler);
    }

    private void forEveryKeyFamily(boolean includeNullKeyFamily, TestContext context, BiConsumer<String, List<Integer>> handler) {
        for (val keyFamily : context.keyFamilies) {
            if (keyFamily == null && !includeNullKeyFamily) {
                continue;
            }
            int keyCount = keyFamily == null ? KEYS_WITHOUT_KEY_FAMILY : KEYS_PER_KEY_FAMILY;
            val keyIds = new ArrayList<Integer>();
            for (int keyId = 0; keyId < keyCount; keyId++) {
                keyIds.add(keyId);
            }
            handler.accept(keyFamily, keyIds);
        }
    }

    private KeyVersion alterVersion(KeyVersion original, boolean changeSegmentName, boolean changeVersion) {
        KeyVersionImpl impl = original.asImpl();
        String newSegmentName = changeSegmentName ? impl.getSegmentName() + "a" : impl.getSegmentName();
        long newVersion = changeVersion ? impl.getSegmentVersion() + 1 : impl.getSegmentVersion();
        return new KeyVersionImpl(newSegmentName, newVersion);
    }

    private int getKey(int keyId) {
        return keyId;
    }

    private String getValue(int keyId, int iteration) {
        return String.format("%s_%s", keyId, iteration);
    }

    private static class Versions {
        private final HashMap<String, HashMap<Integer, KeyVersionImpl>> versions = new HashMap<>();

        void add(String keyFamily, int keyId, KeyVersion kv) {
            keyFamily = adjustKeyFamily(keyFamily);
            val familyVersions = this.versions.computeIfAbsent(keyFamily, kf -> new HashMap<>());
            familyVersions.put(keyId, kv.asImpl());
        }

        void remove(String keyFamily, int keyId) {
            keyFamily = adjustKeyFamily(keyFamily);
            val familyVersions = this.versions.getOrDefault(keyFamily, null);
            if (familyVersions != null) {
                familyVersions.remove(keyId);
                if (familyVersions.isEmpty()) {
                    this.versions.remove(keyFamily);
                }
            }
        }

        KeyVersionImpl get(String keyFamily, int keyId) {
            keyFamily = adjustKeyFamily(keyFamily);
            val familyVersions = this.versions.getOrDefault(keyFamily, null);
            if (familyVersions != null) {
                return familyVersions.getOrDefault(keyId, null);
            }
            return null;
        }

        boolean isEmpty() {
            return this.versions.isEmpty();
        }

        private String adjustKeyFamily(String keyFamily) {
            return keyFamily == null ? NULL_KEY_FAMILY : keyFamily;
        }
    }

    private class TestContext implements AutoCloseable {
        final MockConnectionFactoryImpl connectionFactory;
        final MockTableSegmentFactory segmentFactory;
        final MockController controller;
        final KeyValueTable<Integer, String> keyValueTable;
        final List<String> keyFamilies;

        TestContext() {
            this.connectionFactory = new MockConnectionFactoryImpl();
            this.controller = new MockController("localhost", 0, this.connectionFactory, false);
            this.controller.createScope(KVT.getScope());
            this.controller.createKeyValueTable(KVT.getScope(), KVT.getKeyValueTableName(),
                    KeyValueTableConfiguration.builder().partitionCount(SEGMENT_COUNT).build());
            this.segmentFactory = new MockTableSegmentFactory(SEGMENT_COUNT);
            this.keyValueTable = new KeyValueTableImpl<>(KVT, this.segmentFactory, this.controller, KEY_SERIALIZER, VALUE_SERIALIZER);
            this.keyFamilies = Collections.unmodifiableList(getKeyFamilies());
        }

        private List<String> getKeyFamilies() {
            val result = new ArrayList<String>();
            result.add(null); // No key family.
            for (int i = 0; i < KEY_FAMILY_COUNT; i++) {
                result.add(String.format("KF[%d]", i));
            }
            return result;
        }

        @Override
        public void close() {
            this.keyValueTable.close();
            this.controller.close();
            this.connectionFactory.close();
        }
    }

    @RequiredArgsConstructor
    private class MockTableSegmentFactory implements TableSegmentFactory {
        private final int segmentCount;

        @Override
        public TableSegment forSegment(@NonNull Segment segment) {
            AssertExtensions.assertLessThan("Too many segments requested.", this.segmentCount, segment.getSegmentId());
            return new MockTableSegment(segment);
        }
    }

    @RequiredArgsConstructor
    private class MockTableSegment implements TableSegment {
        private final Segment segment;
        private final AtomicLong nextVersion = new AtomicLong();
        @GuardedBy("data")
        private final Map<ByteBuf, EntryValue> data = new HashMap<>();
        @GuardedBy("data")
        private boolean closed = false;

        @Override
        public CompletableFuture<List<TableSegmentKeyVersion>> put(Iterator<TableSegmentEntry> entries) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.data) {
                    Exceptions.checkNotClosed(this.closed, this);
                    val result = new ArrayList<TableSegmentKeyVersion>();
                    val toUpdate = new HashMap<ByteBuf, EntryValue>();
                    entries.forEachRemaining(e -> {
                        checkVersion(e.getKey());
                        long version = this.nextVersion.getAndIncrement();
                        toUpdate.put(e.getKey().getKey().copy(), new EntryValue(e.getValue().copy(), version));
                        result.add(TableSegmentKeyVersion.from(version));
                    });
                    this.data.putAll(toUpdate);
                    return result;
                }
            }, executorService());
        }

        @Override
        public CompletableFuture<Void> remove(Iterator<TableSegmentKey> keys) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.data) {
                    Exceptions.checkNotClosed(this.closed, this);
                    val toRemove = new ArrayList<ByteBuf>();
                    keys.forEachRemaining(k -> {
                        checkVersion(k);
                        toRemove.add(k.getKey());
                    });
                    toRemove.forEach(this.data::remove);
                }
            }, executorService());
        }

        @Override
        public CompletableFuture<List<TableSegmentEntry>> get(Iterator<ByteBuf> keys) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.data) {
                    Exceptions.checkNotClosed(this.closed, this);
                    val result = new ArrayList<TableSegmentEntry>();
                    keys.forEachRemaining(k -> {
                        EntryValue ev = this.data.getOrDefault(k, null);
                        TableSegmentEntry e = ev == null
                                ? TableSegmentEntry.notFound(k.copy())
                                : TableSegmentEntry.versioned(k.copy(), ev.value.copy(), ev.version);
                        result.add(e);
                    });
                    return result;
                }
            }, executorService());
        }

        @Override
        public AsyncIterator<IteratorItem<TableSegmentKey>> keyIterator(IteratorArgs args) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AsyncIterator<IteratorItem<TableSegmentEntry>> entryIterator(IteratorArgs args) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getSegmentName() {
            return this.segment.getScopedName();
        }

        @Override
        public void close() {
            synchronized (this.data) {
                if (!this.closed) {
                    this.data.forEach((k, e) -> {
                        k.release();
                        e.value.release();
                    });
                    this.data.clear();
                    this.closed = true;
                }
            }
        }

        @GuardedBy("data")
        @SneakyThrows(ConditionalTableUpdateException.class)
        private void checkVersion(TableSegmentKey key) {
            TableSegmentKeyVersion v = key.getVersion();
            if (v != null && !v.equals(TableSegmentKeyVersion.NO_VERSION)) {
                EntryValue existing = this.data.get(key.getKey());
                if (v.equals(TableSegmentKeyVersion.NOT_EXISTS)) {
                    if (existing != null) {
                        throw new BadKeyVersionException(this.segment.getScopedName());
                    }
                } else {
                    if (existing == null) {
                        throw new NoSuchKeyException(this.segment.getScopedName());
                    } else if (existing.version != key.getVersion().getSegmentVersion()) {
                        throw new BadKeyVersionException(this.segment.getScopedName());
                    }
                }
            }
        }

        @RequiredArgsConstructor
        private class EntryValue {
            final ByteBuf value;
            final long version;
        }
    }

    private static class IntegerSerializer implements Serializer<Integer> {
        @Override
        public ByteBuffer serialize(Integer value) {
            return ByteBuffer.allocate(Integer.BYTES).putInt(0, value);
        }

        @Override
        public Integer deserialize(ByteBuffer serializedValue) {
            return serializedValue.getInt();
        }
    }
}
