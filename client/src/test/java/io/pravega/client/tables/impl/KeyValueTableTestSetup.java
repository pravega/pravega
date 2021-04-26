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

import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.Version;
import io.pravega.test.common.LeakDetectorTestSuite;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;

/**
 * Base class that sets up any test suite that tests {@link KeyValueTable}. There are no tests defined in this class.
 */
abstract class KeyValueTableTestSetup extends LeakDetectorTestSuite {
    protected static final String NULL_KEY_FAMILY = "[NULL]"; // Used for HashMap keys.
    protected static final Serializer<Integer> KEY_SERIALIZER = new IntegerSerializer();
    protected static final Serializer<String> VALUE_SERIALIZER = new UTF8StringSerializer();
    protected static final int DEFAULT_SEGMENT_COUNT = 4;
    protected static final int DEFAULT_KEY_FAMILY_COUNT = 100;
    protected static final int DEFAULT_KEYS_PER_KEY_FAMILY = 10;
    @Getter(AccessLevel.PROTECTED)
    private List<String> keyFamilies;

    @Before
    public void setup() throws Exception {
        int count = getKeyFamilyCount();
        this.keyFamilies = new ArrayList<>();
        this.keyFamilies.add(null); // No key family.
        for (int i = 0; i < count; i++) {
            this.keyFamilies.add(String.format("KF[%d]", i));
        }

        this.keyFamilies = Collections.unmodifiableList(this.keyFamilies);
    }

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    protected abstract KeyValueTable<Integer, String> createKeyValueTable();

    protected abstract <K, V> KeyValueTable<K, V> createKeyValueTable(Serializer<K> keySerializer, Serializer<V> valueSerializer);

    protected int getKeyFamilyCount() {
        return DEFAULT_KEY_FAMILY_COUNT;
    }

    protected int getSegmentCount() {
        return DEFAULT_SEGMENT_COUNT;
    }

    protected int getKeysPerKeyFamily() {
        return DEFAULT_KEYS_PER_KEY_FAMILY;
    }

    private int getKeysWithoutKeyFamily() {
        return Math.min(TableSegment.MAXIMUM_BATCH_KEY_COUNT, getKeyFamilyCount() * getKeysPerKeyFamily());
    }

    protected void forEveryKey(BiConsumer<String, Integer> handler) {
        for (val keyFamily : getKeyFamilies()) {
            int keyCount = keyFamily == null ? getKeysWithoutKeyFamily() : getKeysPerKeyFamily();
            for (int keyId = 0; keyId < keyCount; keyId++) {
                handler.accept(keyFamily, keyId);
            }
        }
    }

    protected void forEveryKeyFamily(BiConsumer<String, List<Integer>> handler) {
        forEveryKeyFamily(true, handler);
    }

    protected void forEveryKeyFamily(boolean includeNullKeyFamily, BiConsumer<String, List<Integer>> handler) {
        for (val keyFamily : getKeyFamilies()) {
            if (keyFamily == null && !includeNullKeyFamily) {
                continue;
            }
            int keyCount = keyFamily == null ? getKeysWithoutKeyFamily() : getKeysPerKeyFamily();
            val keyIds = new ArrayList<Integer>();
            for (int keyId = 0; keyId < keyCount; keyId++) {
                keyIds.add(keyId);
            }
            handler.accept(keyFamily, keyIds);
        }
    }

    protected void checkValues(int iteration, Versions versions, KeyValueTable<Integer, String> keyValueTable) {
        // Check individually.
        forEveryKey((keyFamily, keyId) -> {
            val hint = String.format("(KF=%s, Key=%s)", keyFamily, keyId);
            val key = getKey(keyId);
            val expectedValue = getValue(keyId, iteration);
            val expectedVersion = versions.get(keyFamily, keyId);

            val actualEntry = keyValueTable.get(keyFamily, key).join();
            checkValue(key, expectedValue, expectedVersion, actualEntry, hint);
        });

        // Check using getAll.
        forEveryKeyFamily((keyFamily, keyIds) -> {
            val hint = String.format("(KF=%s)", keyFamily);
            val keys = keyIds.stream().map(this::getKey).collect(Collectors.toList());
            val expectedVersions = keyIds.stream().map(keyId -> versions.get(keyFamily, keyId)).collect(Collectors.toList());
            val expectedValues = keyIds.stream().map(keyId -> getValue(keyId, iteration)).collect(Collectors.toList());
            val result = keyValueTable.getAll(keyFamily, keys).join();

            Assert.assertEquals("Unexpected result size" + hint, keys.size(), result.size());
            for (int i = 0; i < keys.size(); i++) {
                checkValue(keys.get(i), expectedValues.get(i), expectedVersions.get(i), result.get(i), hint);
            }
        });
    }

    protected void checkValue(Integer key, String expectedValue, Version expectedVersion, TableEntry<Integer, String> actualEntry, String hint) {
        if (expectedVersion == null) {
            // Key was removed or never inserted.
            Assert.assertNull("Not expecting a value for removed key" + hint, actualEntry);
        } else {
            // Key exists.
            Assert.assertEquals("Unexpected key" + hint, key, actualEntry.getKey().getKey());
            Assert.assertEquals("Unexpected version" + hint, expectedVersion, actualEntry.getKey().getVersion());
            Assert.assertEquals("Unexpected value" + hint, expectedValue, actualEntry.getValue());
        }
    }

    protected int getKey(int keyId) {
        return keyId;
    }

    protected String getValue(int keyId, int iteration) {
        return String.format("%s_%s", keyId, iteration);
    }

    protected int getKeyFromValue(String value) {
        int pos = value.indexOf("_");
        return Integer.parseInt(value.substring(0, pos));
    }

    protected static class Versions {
        @Getter(AccessLevel.PACKAGE)
        private final HashMap<String, HashMap<Integer, VersionImpl>> versions = new HashMap<>();

        void add(String keyFamily, int keyId, Version kv) {
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

        VersionImpl get(String keyFamily, int keyId) {
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
