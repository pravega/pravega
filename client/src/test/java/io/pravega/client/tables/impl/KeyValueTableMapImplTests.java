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

import com.google.common.util.concurrent.Runnables;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.TableEntry;
import io.pravega.test.common.AssertExtensions;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link KeyValueTableMapImpl}.
 */
public class KeyValueTableMapImplTests extends KeyValueTableTestSetup {
    private static final KeyValueTableInfo KVT = new KeyValueTableInfo("Scope", "KVT");
    private MockConnectionFactoryImpl connectionFactory;
    private MockTableSegmentFactory segmentFactory;
    private MockController controller;
    private KeyValueTable<Integer, String> keyValueTable;

    //region Setup and Teardown

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Override
    protected KeyValueTable<Integer, String> createKeyValueTable() {
        return this.keyValueTable;
    }

    @Override
    protected <K, V> KeyValueTable<K, V> createKeyValueTable(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return new KeyValueTableImpl<>(KVT, this.segmentFactory, this.controller, keySerializer, valueSerializer);
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        this.connectionFactory = new MockConnectionFactoryImpl();
        this.controller = new MockController("localhost", 0, this.connectionFactory, false);
        this.controller.createScope(KVT.getScope());
        this.controller.createKeyValueTable(KVT.getScope(), KVT.getKeyValueTableName(),
                KeyValueTableConfiguration.builder().partitionCount(getSegmentCount()).build());
        this.segmentFactory = new MockTableSegmentFactory(getSegmentCount(), executorService());
        this.keyValueTable = createKeyValueTable(KEY_SERIALIZER, VALUE_SERIALIZER);
    }

    @After
    public void tearDown() {
        this.keyValueTable.close();
        this.controller.close();
        this.connectionFactory.close();
    }

    //endregion

    /**
     * Tests {@link KeyValueTableMapImpl#put}, {@link KeyValueTableMapImpl#putDirect}, {@link KeyValueTableMapImpl#putIfAbsent},
     * {@link KeyValueTableMapImpl#replace} and {@link KeyValueTableMapImpl#remove}.
     */
    @Test
    public void testSingleKeyOperations() {
        @Cleanup
        val kvt = createKeyValueTable();
        val expectedValues = new ExpectedValues();

        // PutIfAbsent.
        val iteration = new AtomicInteger(0);
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());
            val map = kvt.getMapFor(keyFamily);

            if (keyId % 2 == 0) {
                // putIfAbsent should work when the key doesn't exist.
                val result = map.putIfAbsent(key, value);
                Assert.assertEquals(value, result);
            } else {
                // put should return null when the key doesn't exist.
                val result = map.put(key, value);
                Assert.assertNull(result);
            }
            expectedValues.put(keyFamily, key, value);

            // ... and should have no effect when it already does.
            Assert.assertEquals(value, map.putIfAbsent(key, value + "bad"));

            Assert.assertTrue(map.containsKey(key));
            if (keyFamily != null) {
                Assert.assertTrue(map.containsValue(value));
            }
        });
        check(kvt, expectedValues);

        // Put/PutDirect.
        iteration.incrementAndGet();
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());
            val map = kvt.getMapFor(keyFamily);

            // PutDirect should just override the value.
            val randomValue = value + "random";
            map.putDirect(key, randomValue);

            // Put should return the old value.
            val result = map.put(key, value);
            Assert.assertEquals(randomValue, result);
            expectedValues.put(keyFamily, key, value);
        });
        check(kvt, expectedValues);

        // Replace.
        iteration.incrementAndGet();
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val oldValue = getValue(keyId, iteration.get() - 1);
            val value = getValue(keyId, iteration.get());
            val map = kvt.getMapFor(keyFamily);

            // Replace
            if (keyId % 2 == 0) {
                val result = map.replace(key, value);
                Assert.assertEquals(oldValue, result);
            } else {
                boolean wasReplaced;
                wasReplaced = map.replace(key, oldValue + "foo", value);
                Assert.assertFalse(wasReplaced);
                wasReplaced = map.replace(key, oldValue, value);
                Assert.assertTrue(wasReplaced);
            }
            expectedValues.put(keyFamily, key, value);
        });
        check(kvt, expectedValues);

        // Remove.
        iteration.incrementAndGet();
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val oldValue = getValue(keyId, iteration.get() - 1);
            val map = kvt.getMapFor(keyFamily);

            // Remove.
            Assert.assertFalse(map.remove(key, oldValue + "foo"));
            val choice = keyId % 3;
            if (choice == 0) {
                val result = map.remove(key);
                Assert.assertEquals(oldValue, result);
            } else if (choice == 1) {
                map.removeDirect(key);
                Assert.assertNull(map.remove(key));
            } else {
                val wasRemoved = map.remove(key, oldValue);
                Assert.assertTrue(wasRemoved);
            }

            expectedValues.remove(keyFamily, key);

            Assert.assertNull(map.replace(key, "foo"));
        });
        check(kvt, expectedValues);
    }

    /**
     * Tests {@link KeyValueTableMapImpl#compute}, {@link KeyValueTableMapImpl#computeIfAbsent}, {@link KeyValueTableMapImpl#computeIfPresent}.
     */
    @Test
    public void testCompute() {
        @Cleanup
        val kvt = createKeyValueTable();
        val expectedValues = new ExpectedValues();

        val iteration = new AtomicInteger(0);

        // Insert
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());
            val map = kvt.getMapFor(keyFamily);

            // computeIfPresent() should not invoke or return anything.
            val cp = map.computeIfPresent(key, (k, existingValue) -> AssertExtensions.fail("computeIfPresent executed when no key present."));
            Assert.assertNull(cp);

            if (key % 2 == 0) {
                // Insert via computeIfAbsent().
                val ca = map.computeIfAbsent(key, k -> {
                    Assert.assertEquals(key, (int) k);
                    return value;
                });
                Assert.assertEquals(value, ca);
                val ca2 = map.computeIfAbsent(key, k -> AssertExtensions.fail("computeIfAbsent executed when key already present."));
                Assert.assertEquals(value, ca2); // computeIfAbsent returns existing value, if any.
            } else {
                // Insert via compute().
                val c = map.compute(key, (k, existingValue) -> {
                    Assert.assertEquals(key, (int) k);
                    Assert.assertNull(existingValue);
                    return value;
                });
                Assert.assertEquals(value, c);
            }
            expectedValues.put(keyFamily, key, value);
        });
        check(kvt, expectedValues);

        // Remove
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration.get());
            val map = kvt.getMapFor(keyFamily);

            if (key % 2 == 0) {
                // Remove via computeIfPresent.
                val cp = map.computeIfPresent(key, (k, existingValue) -> {
                    Assert.assertEquals(key, (int) k);
                    Assert.assertEquals(value, existingValue);
                    return null;
                });
                Assert.assertNull(cp);
            } else {
                // Remove via compute().
                val c = map.compute(key, (k, existingValue) -> {
                    Assert.assertEquals(key, (int) k);
                    Assert.assertEquals(value, existingValue);
                    return null;
                });
                Assert.assertNull(c);
            }

            val cp2 = map.computeIfPresent(key, (k, existingValue) -> AssertExtensions.fail("computeIfPresent executed when no key present."));
            Assert.assertNull(cp2);
            val ca = map.computeIfAbsent(key, k -> {
                Assert.assertEquals(key, (int) k);
                return null;
            });
            Assert.assertNull(ca);

            val c2 = map.compute(key, (k, existingValue) -> {
                Assert.assertEquals(key, (int) k);
                Assert.assertNull(existingValue);
                return null;
            });
            Assert.assertNull(c2);
            expectedValues.remove(keyFamily, key);
        });
        check(kvt, expectedValues);
    }

    /**
     * Tests {@link KeyValueTableMapImpl#putAll}, {@link KeyValueTableMapImpl#replaceAll}, {@link KeyValueTableMapImpl#clear()},
     * {@link KeyValueTableMapImpl#size()}, {@link KeyValueTableMapImpl#isEmpty()}.
     */
    @Test
    public void testMultiKeyOperations() {
        @Cleanup
        val kvt = createKeyValueTable();
        val expectedValues = new ExpectedValues();

        // PutAll.
        val iteration = new AtomicInteger(0);
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val map = kvt.getMapFor(keyFamily);
            val toPut = keyIds.stream().collect(Collectors.toMap(this::getKey, keyId -> getValue(keyId, iteration.get())));
            map.putAll(toPut);
            expectedValues.putAll(keyFamily, toPut);
        });
        check(kvt, expectedValues);

        // ReplaceAll.
        iteration.incrementAndGet();
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val map = kvt.getMapFor(keyFamily);
            map.replaceAll((keyId, existingValue) -> {
                Assert.assertEquals(expectedValues.get(keyFamily).get(keyId), existingValue);
                val newValue = getValue(keyId, iteration.get());
                expectedValues.put(keyFamily, keyId, newValue);
                return newValue;
            });
        });
        check(kvt, expectedValues);

        // Clear.
        iteration.incrementAndGet();
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val map = kvt.getMapFor(keyFamily);
            map.clear();
            Assert.assertTrue(map.isEmpty());
            Assert.assertEquals(0, map.size());
            expectedValues.removeAll(keyFamily);
        });
        check(kvt, expectedValues);
    }

    /**
     * Tests {@link KeyValueTableMapImpl#keySet()} and all operations on it.
     */
    @Test
    public void testKeySet() {
        @Cleanup
        val kvt = createKeyValueTable();
        val expectedEntries = new ExpectedValues();
        populate(kvt, expectedEntries, 0);
        val keysByKeyFamily = new HashMap<String, List<Integer>>();

        // Contains, ContainsAll, size, isEmpty, iterator, stream, toArray.
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val map = kvt.getMapFor(keyFamily);
            val keySet = map.keySet();
            val expected = expectedEntries.get(keyFamily);
            Assert.assertEquals(expected.size(), keySet.size());
            Assert.assertEquals(expected.isEmpty(), keySet.isEmpty());
            Assert.assertTrue(keySet.containsAll(keyIds));
            for (val keyId : keyIds) {
                Assert.assertTrue(keySet.contains(keyId));
            }

            val expectedKeys = expected.keySet().stream().sorted().collect(Collectors.toList());
            val iteratorItems = StreamSupport.stream(Spliterators.spliteratorUnknownSize(keySet.iterator(), 0), false).sorted().collect(Collectors.toList());
            val streamItems = keySet.stream().sorted().collect(Collectors.toList());
            val toArrayItems = Stream.of(keySet.toArray()).mapToInt(o -> (int) o).sorted().boxed().collect(Collectors.toList());
            AssertExtensions.assertListEquals("iterator", expectedKeys, iteratorItems, Integer::equals);
            AssertExtensions.assertListEquals("stream", expectedKeys, streamItems, Integer::equals);
            AssertExtensions.assertListEquals("toArray", expectedKeys, toArrayItems, Integer::equals);
            keysByKeyFamily.put(keyFamily, expectedKeys);
        });

        // RemoveAll, ContainsAll.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove first 3 elements using removeAll.
            val map = kvt.getMapFor(keyFamily);
            val first3Keys = keysByKeyFamily.get(keyFamily).subList(0, 3);
            Assert.assertTrue(map.keySet().removeAll(first3Keys));
            Assert.assertFalse(map.keySet().containsAll(first3Keys));
            expectedEntries.remove(keyFamily, first3Keys);
        });
        check(kvt, expectedEntries);

        // Remove.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove 4th element using remove.
            val map = kvt.getMapFor(keyFamily);
            val fourthKey = keysByKeyFamily.get(keyFamily).get(3);
            Assert.assertTrue(map.keySet().remove(fourthKey));
            Assert.assertFalse(map.keySet().remove(fourthKey)); // Already removed.
            expectedEntries.remove(keyFamily, fourthKey);
        });
        check(kvt, expectedEntries);

        // RetainAll.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Preserve only indices 4-8.
            val map = kvt.getMapFor(keyFamily);
            val toPreserve = keysByKeyFamily.get(keyFamily).subList(4, 8);
            Assert.assertTrue(map.keySet().retainAll(toPreserve));
            Assert.assertFalse(map.keySet().retainAll(toPreserve)); // Already removed.
            expectedEntries.remove(keyFamily, keysByKeyFamily.get(keyFamily).stream().filter(e -> !toPreserve.contains(e)).collect(Collectors.toList()));
        });
        check(kvt, expectedEntries);

        // RemoveIf.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove all even indices
            val map = kvt.getMapFor(keyFamily);
            Assert.assertTrue(map.keySet().removeIf(key -> key % 2 == 0));
            Assert.assertFalse(map.keySet().removeIf(key -> key % 2 == 0)); // Already removed.
            val toRemove = expectedEntries.get(keyFamily).keySet().stream().filter(key -> key % 2 == 0).collect(Collectors.toList());
            expectedEntries.remove(keyFamily, toRemove);
        });
        check(kvt, expectedEntries);

        // Clear.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            kvt.getMapFor(keyFamily).keySet().clear();
            expectedEntries.removeAll(keyFamily);
        });
        check(kvt, expectedEntries);
    }

    /**
     * Tests {@link KeyValueTableMapImpl#entrySet()} and all operations on it.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testEntrySet() {
        @Cleanup
        val kvt = createKeyValueTable();
        val expectedEntries = new ExpectedValues();
        val entriesByKeyFamily = new HashMap<String, List<Map.Entry<Integer, String>>>();

        // Add, AddAll
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val map = kvt.getMapFor(keyFamily);
            val entrySet = map.entrySet();
            if (expectedEntries.getKeyFamilies().size() % 2 == 0) {
                // Add.
                for (val keyId : keyIds) {
                    val key = getKey(keyId);
                    val value = getValue(keyId, 0);
                    entrySet.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
                    expectedEntries.put(keyFamily, key, value);
                }
            } else {
                // AddAll.
                val toAdd = getExpectedValues(keyIds, 0);
                map.entrySet().addAll(new ArrayList<>(toAdd.entrySet()));
                expectedEntries.putAll(keyFamily, toAdd);
            }
            entriesByKeyFamily.put(keyFamily, new ArrayList<>(expectedEntries.get(keyFamily).entrySet()));
        });
        check(kvt, expectedEntries);

        // Contains, ContainsAll, size, isEmpty, iterator, stream, toArray.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            val map = kvt.getMapFor(keyFamily);
            val entrySet = map.entrySet();
            val expected = expectedEntries.get(keyFamily);
            Assert.assertEquals(expected.size(), entrySet.size());
            Assert.assertEquals(expected.isEmpty(), entrySet.isEmpty());
            Assert.assertTrue(entrySet.containsAll(expectedEntries.get(keyFamily).entrySet()));
            for (val e : expectedEntries.get(keyFamily).entrySet()) {
                Assert.assertTrue(entrySet.contains(e));
            }

            val expectedEntrySet = expected.entrySet().stream()
                    .sorted(Comparator.comparingInt(Map.Entry::getKey)).collect(Collectors.toList());
            val iteratorItems = StreamSupport.stream(Spliterators.spliteratorUnknownSize(entrySet.iterator(), 0), false)
                    .sorted(Comparator.comparingInt(Map.Entry::getKey)).collect(Collectors.toList());
            val streamItems = entrySet.stream().sorted(Comparator.comparingInt(Map.Entry::getKey)).collect(Collectors.toList());
            val toArrayItems = Stream.of(entrySet.toArray()).map(o -> (Map.Entry<Integer, String>) o)
                    .sorted(Comparator.comparingInt(Map.Entry::getKey)).collect(Collectors.toList());
            AssertExtensions.assertListEquals("iterator", expectedEntrySet, iteratorItems, Object::equals);
            AssertExtensions.assertListEquals("stream", expectedEntrySet, streamItems, Object::equals);
            AssertExtensions.assertListEquals("toArray", expectedEntrySet, toArrayItems, Object::equals);
        });

        // RemoveAll, ContainsAll.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove first 3 elements using removeAll.
            val first3Entries = entriesByKeyFamily.get(keyFamily).subList(0, 3);

            // First try with mismatched values.
            val first3EntriesBad = first3Entries.stream().map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue() + "foo"))
                    .collect(Collectors.toList());
            val map = kvt.getMapFor(keyFamily);
            Assert.assertFalse(map.entrySet().containsAll(first3EntriesBad));
            Assert.assertFalse(map.entrySet().removeAll(first3EntriesBad));

            // Then with correct values.
            Assert.assertTrue(map.entrySet().removeAll(first3Entries));
            Assert.assertFalse(map.entrySet().containsAll(first3Entries));
            Assert.assertFalse(map.entrySet().removeAll(first3Entries));
            expectedEntries.remove(keyFamily, first3Entries.stream().map(Map.Entry::getKey).collect(Collectors.toList()));
        });
        check(kvt, expectedEntries);

        // Remove.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove 4th element using remove.
            val fourthEntry = entriesByKeyFamily.get(keyFamily).get(3);

            // First try with mismatched value.
            val fourthEntryBad = new AbstractMap.SimpleImmutableEntry<>(fourthEntry.getKey(), fourthEntry.getValue() + "foo");
            val map = kvt.getMapFor(keyFamily);
            Assert.assertFalse(map.entrySet().contains(fourthEntryBad));
            Assert.assertFalse(map.entrySet().remove(fourthEntryBad));

            // Then with correct value.
            Assert.assertTrue(map.entrySet().remove(fourthEntry));
            Assert.assertFalse(map.entrySet().remove(fourthEntry)); // Already removed.
            expectedEntries.remove(keyFamily, fourthEntry.getKey());
        });
        check(kvt, expectedEntries);

        // RetainAll.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Preserve only indices 4-8. We do not try with "mismatched" values as that will clear out the entire map.
            val toPreserve = entriesByKeyFamily.get(keyFamily).subList(4, 8);
            val map = kvt.getMapFor(keyFamily);
            Assert.assertTrue(map.entrySet().retainAll(toPreserve));
            Assert.assertFalse(map.entrySet().retainAll(toPreserve)); // Already removed.
            expectedEntries.remove(keyFamily,
                    entriesByKeyFamily.get(keyFamily).stream()
                            .filter(e -> !toPreserve.contains(e))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList()));
        });
        check(kvt, expectedEntries);

        // RemoveIf.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove all even indices.
            val map = kvt.getMapFor(keyFamily);
            Assert.assertTrue(map.entrySet().removeIf(e -> e.getKey() % 2 == 0));
            Assert.assertFalse(map.entrySet().removeIf(e -> e.getKey() % 2 == 0)); // Already removed.
            val toRemove = expectedEntries.get(keyFamily).keySet().stream().filter(key -> key % 2 == 0).collect(Collectors.toList());
            expectedEntries.remove(keyFamily, toRemove);
        });
        check(kvt, expectedEntries);

        // Clear.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            kvt.getMapFor(keyFamily).entrySet().clear();
            expectedEntries.removeAll(keyFamily);
        });
        check(kvt, expectedEntries);
    }

    /**
     * Tests {@link KeyValueTableMapImpl#values()} and all operations on it.
     */
    @Test
    public void testValues() {
        @Cleanup
        val kvt = createKeyValueTable();
        val expectedEntries = new ExpectedValues();

        // Populate twice, with different keys but same values. This way we have 2 keys per unique value.
        populate(kvt, expectedEntries, 0);
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId + DEFAULT_KEYS_PER_KEY_FAMILY);
            val value = getValue(keyId, 0);
            val map = kvt.getMapFor(keyFamily);
            map.putDirect(key, value);
            expectedEntries.put(keyFamily, key, value);
        });
        val entriesByKeyFamily = new HashMap<String, List<Map.Entry<Integer, String>>>();

        // Contains, ContainsAll, size, isEmpty, iterator, stream, toArray.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            val map = kvt.getMapFor(keyFamily);
            val values = map.values();
            val expected = expectedEntries.get(keyFamily);
            Assert.assertEquals(expected.size(), values.size());
            Assert.assertEquals(expected.isEmpty(), values.isEmpty());
            Assert.assertTrue(values.containsAll(expectedEntries.get(keyFamily).values()));
            for (val value : expected.values()) {
                Assert.assertTrue(values.contains(value));
            }

            val expectedValues = expected.values().stream().sorted().collect(Collectors.toList());
            val iteratorItems = StreamSupport.stream(Spliterators.spliteratorUnknownSize(values.iterator(), 0), false).sorted().collect(Collectors.toList());
            val streamItems = values.stream().sorted().collect(Collectors.toList());
            val toArrayItems = Stream.of(values.toArray()).map(o -> (String) o).sorted().collect(Collectors.toList());
            AssertExtensions.assertListEquals("iterator", expectedValues, iteratorItems, String::equals);
            AssertExtensions.assertListEquals("stream", expectedValues, streamItems, String::equals);
            AssertExtensions.assertListEquals("toArray", expectedValues, toArrayItems, String::equals);
            entriesByKeyFamily.put(keyFamily, new ArrayList<>(expected.entrySet()));
        });

        // RemoveAll, ContainsAll.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove first 3 values using removeAll.
            val map = kvt.getMapFor(keyFamily);
            val first3Entries = entriesByKeyFamily.get(keyFamily).subList(0, 3);
            val first3Values = first3Entries.stream().map(Map.Entry::getValue).collect(Collectors.toList());
            Assert.assertTrue(map.values().removeAll(first3Values));
            Assert.assertFalse(map.values().containsAll(first3Values));
            expectedEntries.removeValues(keyFamily, first3Values);
        });
        check(kvt, expectedEntries);

        // Remove.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove 4th element using remove.
            val map = kvt.getMapFor(keyFamily);
            val fourthValue = entriesByKeyFamily.get(keyFamily).get(3).getValue();
            Assert.assertTrue(map.values().remove(fourthValue));
            Assert.assertFalse(map.values().remove(fourthValue)); // Already removed.
            expectedEntries.removeValues(keyFamily, Collections.singletonList(fourthValue));
        });
        check(kvt, expectedEntries);

        // RetainAll.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Preserve only indices 4-8.
            val map = kvt.getMapFor(keyFamily);
            val toPreserveEntries = entriesByKeyFamily.get(keyFamily).subList(4, 8);
            val toPreserveValues = toPreserveEntries.stream().map(Map.Entry::getValue).collect(Collectors.toList());
            Assert.assertTrue(map.values().retainAll(toPreserveValues));
            Assert.assertFalse(map.values().retainAll(toPreserveValues)); // Already removed.
            expectedEntries.removeValues(keyFamily,
                    entriesByKeyFamily.get(keyFamily).stream().map(Map.Entry::getValue)
                            .filter(e -> !toPreserveValues.contains(e)).collect(Collectors.toList()));
        });
        check(kvt, expectedEntries);

        // RemoveIf.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove all even indices
            val map = kvt.getMapFor(keyFamily);
            Assert.assertTrue(map.values().removeIf(v -> getKeyFromValue(v) % 2 == 0));
            Assert.assertFalse(map.values().removeIf(v -> getKeyFromValue(v) % 2 == 0)); // Already removed.
            val toRemove = expectedEntries.get(keyFamily).keySet().stream().filter(key -> key % 2 == 0).collect(Collectors.toList());
            expectedEntries.remove(keyFamily, toRemove);
        });
        check(kvt, expectedEntries);

        // Clear.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            kvt.getMapFor(keyFamily).values().clear();
            expectedEntries.removeAll(keyFamily);
        });
        check(kvt, expectedEntries);
    }

    /**
     * Tests the ability to remove entries via the {@link KeyValueTableMapImpl#keySet()}, {@link KeyValueTableMapImpl#entrySet()} or
     * {@link KeyValueTableMapImpl#values()} iterators.
     */
    @Test
    public void testIteratorRemove() {
        @Cleanup
        val kvt = createKeyValueTable();
        val expectedEntries = new ExpectedValues();
        populate(kvt, expectedEntries, 0);
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId + DEFAULT_KEYS_PER_KEY_FAMILY);
            val value = getValue(keyId, 0);
            kvt.getMapFor(keyFamily).putDirect(key, value);
            expectedEntries.put(keyFamily, key, value);
        });

        // KeySet.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove all even indices
            val map = kvt.getMapFor(keyFamily);
            val iterator = map.keySet().iterator();
            AssertExtensions.assertThrows("", iterator::remove, ex -> ex instanceof IllegalStateException);
            while (iterator.hasNext()) {
                val k = iterator.next();
                if (k % 4 == 0) {
                    iterator.remove();
                    AssertExtensions.assertThrows("", iterator::remove, ex -> ex instanceof IllegalStateException);
                    expectedEntries.remove(keyFamily, k);
                }
            }
        });
        check(kvt, expectedEntries);

        // EntrySet.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove all even indices
            val map = kvt.getMapFor(keyFamily);
            val iterator = map.entrySet().iterator();
            AssertExtensions.assertThrows("", iterator::remove, ex -> ex instanceof IllegalStateException);
            while (iterator.hasNext()) {
                val e = iterator.next();
                if (e.getKey() % 4 == 1) {
                    iterator.remove();
                    AssertExtensions.assertThrows("", iterator::remove, ex -> ex instanceof IllegalStateException);
                    expectedEntries.remove(keyFamily, e.getKey());
                }
            }
        });
        check(kvt, expectedEntries);

        // Values.
        forEveryKeyFamily(false, (keyFamily, ignored) -> {
            // Remove all even indices
            val map = kvt.getMapFor(keyFamily);
            val iterator = map.values().iterator();
            AssertExtensions.assertThrows("", iterator::remove, ex -> ex instanceof IllegalStateException);
            while (iterator.hasNext()) {
                val key = getKeyFromValue(iterator.next());
                if (key % 4 == 3) {
                    iterator.remove();
                    AssertExtensions.assertThrows("", iterator::remove, ex -> ex instanceof IllegalStateException);
                    expectedEntries.remove(keyFamily, key);
                }
            }
        });
        check(kvt, expectedEntries);
    }

    /**
     * Tests {@link KeyValueTableMapImpl} without a Key Family (limited functionality).
     */
    @Test
    public void testNoKeyFamily() {
        @Cleanup
        val kvt = createKeyValueTable();
        val map = kvt.getMapFor(null);
        val toTest = new AssertExtensions.RunnableWithException[]{
                map::keySet, map::entrySet, map::values, map::clear, map::size, map::isEmpty, () -> map.replaceAll((a, b) -> b),
                () -> map.putAll(Collections.emptyMap()), () -> map.forEach((k, v) -> Runnables.doNothing()), () -> map.containsValue("")};
        for (val method : toTest) {
            AssertExtensions.assertThrows("Unsupported operation was executed without a key family.",
                    method, ex -> ex instanceof UnsupportedOperationException);
        }
    }

    private Map<Integer, String> getExpectedValues(List<Integer> keyIds, int iteration) {
        return keyIds.stream().collect(Collectors.toMap(this::getKey, keyId -> getValue(keyId, iteration)));
    }

    private void populate(KeyValueTable<Integer, String> kvt, ExpectedValues expectedValues, int iteration) {
        forEveryKey((keyFamily, keyId) -> {
            val key = getKey(keyId);
            val value = getValue(keyId, iteration);
            val map = kvt.getMapFor(keyFamily);
            map.putDirect(key, value);
            expectedValues.put(keyFamily, key, value);
        });
    }

    private void check(KeyValueTable<Integer, String> kvt, ExpectedValues values) {
        values.getKeyFamilies().forEach(keyFamily -> {
            val expectedMap = values.get(keyFamily);
            val actualMap = kvt.getMapFor(keyFamily);
            if (keyFamily == null) {
                // There are no iterators available, so we need to check each entry.
                for (val e : expectedMap.entrySet()) {
                    val actualValue = actualMap.getOrDefault(e.getKey(), null);
                    Assert.assertEquals(e.getValue(), actualValue);
                }
            } else {
                // Test using entrySet iterator; it's a more complete check it will also return any "extra" entries that
                // we may not know about.
                val actualEntries = actualMap.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry::getKey)).collect(Collectors.toList());
                val expectedEntries = expectedMap.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry::getKey)).collect(Collectors.toList());
                AssertExtensions.assertListEquals("", expectedEntries, actualEntries, Objects::equals);
            }

            // Now check the actual Key-Value Table.
            val kvtValues = kvt.getAll(keyFamily, expectedMap.keySet()).join().stream()
                    .collect(Collectors.toMap(e -> e.getKey().getKey(), TableEntry::getValue));
            Assert.assertEquals(expectedMap, kvtValues);
        });
    }

    private static class ExpectedValues {
        private final HashMap<String, HashMap<Integer, String>> valuesByKeyFamily = new HashMap<>();

        void put(String keyFamily, int keyId, String value) {
            this.valuesByKeyFamily.computeIfAbsent(keyFamily, kf -> new HashMap<>()).put(keyId, value);
        }

        void putAll(String keyFamily, Map<Integer, String> values) {
            this.valuesByKeyFamily.computeIfAbsent(keyFamily, kf -> new HashMap<>()).putAll(values);
        }

        void remove(String keyFamily, int keyId) {
            this.valuesByKeyFamily.computeIfAbsent(keyFamily, kf -> new HashMap<>()).remove(keyId);
        }

        void remove(String keyFamily, List<Integer> keyIds) {
            this.valuesByKeyFamily.computeIfAbsent(keyFamily, kf -> new HashMap<>()).keySet().removeAll(keyIds);
        }

        void removeValues(String keyFamily, List<String> values) {
            this.valuesByKeyFamily.computeIfAbsent(keyFamily, kf -> new HashMap<>()).values().removeAll(values);
        }

        void removeAll(String keyFamily) {
            this.valuesByKeyFamily.computeIfAbsent(keyFamily, kf -> new HashMap<>()).clear();
        }

        Map<Integer, String> get(String keyFamily) {
            return Collections.unmodifiableMap(this.valuesByKeyFamily.getOrDefault(keyFamily, new HashMap<>()));
        }

        Collection<String> getKeyFamilies() {
            return Collections.unmodifiableCollection(this.valuesByKeyFamily.keySet());
        }
    }
}
