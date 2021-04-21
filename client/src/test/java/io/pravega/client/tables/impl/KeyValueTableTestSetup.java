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
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.Version;
import io.pravega.test.common.LeakDetectorTestSuite;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;

/**
 * Base class that sets up any test suite that tests {@link KeyValueTable}. There are no tests defined in this class.
 */
abstract class KeyValueTableTestSetup extends LeakDetectorTestSuite {
    protected static final Serializer<Long> PK_SERIALIZER = new LongSerializer();
    protected static final Serializer<Integer> SK_SERIALIZER = new IntegerSerializer(); // TODO add tests for optional SecondaryKey
    protected static final Serializer<String> VALUE_SERIALIZER = new UTF8StringSerializer();
    protected static final int DEFAULT_SEGMENT_COUNT = 4;
    protected static final int DEFAULT_PRIMARY_KEY_COUNT = 100;
    protected static final int DEFAULT_SECONDARY_KEY_COUNT = 10; // Per Primary Key
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

    protected void forEveryKey(BiConsumer<ByteBuffer, ByteBuffer> handler) {
        int secondaryCount = getSecondaryKeyCount();
        for (val pk : getPrimaryKeys()) {
            for (int skId = 0; skId < secondaryCount; skId++) {
                handler.accept(pk.duplicate(), SK_SERIALIZER.serialize(skId));
            }
        }
    }

    protected void forEveryPrimaryKey(BiConsumer<ByteBuffer, List<ByteBuffer>> handler) {
        int secondaryCount = getSecondaryKeyCount();
        for (val pk : getPrimaryKeys()) {
            val sks = IntStream.range(0, secondaryCount).mapToObj(SK_SERIALIZER::serialize).collect(Collectors.toList());
            handler.accept(pk, sks);
        }
    }

    protected void checkValues(int iteration, Versions versions, KeyValueTable keyValueTable) {
        // Check individually.
        forEveryKey((pk, sk) -> {
            val keyId = getUniqueKeyId(pk, sk);
            val expectedValue = getValue(pk, sk, iteration);
            val expectedVersion = versions.get(keyId);

            val requestKey = TableKey.unversioned(pk, sk);
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
            val requestKeys = secondaryKeys.stream().map(sk -> TableKey.unversioned(pk, sk)).collect(Collectors.toList());
            val result = keyValueTable.getAll(requestKeys).join();

            val hint = getUniqueKeyId(pk);
            Assert.assertEquals("Unexpected result size" + hint, requestKeys.size(), result.size());
            for (int i = 0; i < requestKeys.size(); i++) {
                checkValue(requestKeys.get(i), expectedValues.get(i), expectedVersions.get(i), result.get(i), hint);
            }
        });
    }

    protected void checkValue(TableKey key, ByteBuffer expectedValue, Version expectedVersion, TableEntry actualEntry, Object hint) {
        if (expectedVersion == null) {
            // Key was removed or never inserted.
            Assert.assertNull("Not expecting a value for removed key" + hint, actualEntry);
        } else {
            // Key exists.
            Assert.assertEquals("Unexpected Primary Key" + hint, key.getPrimaryKey(), actualEntry.getKey().getPrimaryKey());
            Assert.assertEquals("Unexpected Secondary Key" + hint, key.getSecondaryKey(), actualEntry.getKey().getSecondaryKey());
            Assert.assertEquals("Unexpected version" + hint, expectedVersion, actualEntry.getKey().getVersion());
            Assert.assertEquals("Unexpected value" + hint, expectedValue, actualEntry.getValue());
        }
    }

    protected String getUniqueKeyId(ByteBuffer pk, ByteBuffer sk) {
        return String.format("(PK=%s, SK=%s)", PK_SERIALIZER.deserialize(pk),
                sk == null || sk.remaining() == 0 ? "[null]" : SK_SERIALIZER.deserialize(sk));
    }

    protected String getUniqueKeyId(ByteBuffer pk) {
        return String.format("(PK=%s)", PK_SERIALIZER.deserialize(pk));
    }

    protected ByteBuffer getValue(ByteBuffer pk, ByteBuffer sk, int iteration) {
        val result = ByteBuffer.allocate(pk.remaining() + sk.remaining() + Integer.BYTES);
        result.put(pk.duplicate());
        result.put(sk.duplicate());
        result.putInt(iteration);
        return result.asReadOnlyBuffer();
    }

    protected static class Versions {
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
            return ByteBuffer.allocate(Integer.BYTES).putInt(0, value).asReadOnlyBuffer();
        }

        @Override
        public Integer deserialize(ByteBuffer serializedValue) {
            return serializedValue.duplicate().getInt();
        }
    }

    private static class LongSerializer implements Serializer<Long> {
        @Override
        public ByteBuffer serialize(Long value) {
            return ByteBuffer.allocate(Long.BYTES).putLong(0, value).asReadOnlyBuffer();
        }

        @Override
        public Long deserialize(ByteBuffer serializedValue) {
            return serializedValue.duplicate().getLong();
        }
    }
}
