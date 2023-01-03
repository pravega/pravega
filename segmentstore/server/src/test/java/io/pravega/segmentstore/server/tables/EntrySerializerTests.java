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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link EntrySerializer} class.
 */
public class EntrySerializerTests {
    private static final int COUNT = 100;
    private static final int MAX_VALUE_SIZE = EntrySerializer.MAX_KEY_LENGTH * 4;

    /**
     * Tests the ability to serialize updates.
     */
    @Test
    public void testUpdate() throws Exception {
        val entries = generateEntries();
        val s = new EntrySerializer();
        val expectedLength = entries.stream().map(s::getUpdateLength).mapToInt(i -> i).sum();
        val serialization = s.serializeUpdate(entries).getCopy();
        Assert.assertEquals(expectedLength, serialization.length);

        int offset = 0;
        for (val e : entries) {
            offset += checkEntry(e, serialization, offset, s, false);
        }

        Assert.assertEquals("Did not read the entire serialization.", serialization.length, offset);
    }

    /**
     * Tests the ability to serialize updates with explicit versions.
     */
    @Test
    public void testUpdateWithExplicitVersion() throws Exception {
        val entries = generateEntries();
        val s = new EntrySerializer();
        val expectedLength = entries.stream().map(s::getUpdateLength).mapToInt(i -> i).sum();
        val serialization = s.serializeUpdateWithExplicitVersion(entries).getCopy();
        Assert.assertEquals(expectedLength, serialization.length);

        int offset = 0;
        for (val e : entries) {
            offset += checkEntry(e, serialization, offset, s, true);
        }

        Assert.assertEquals("Did not read the entire serialization.", serialization.length, offset);
    }

    @Test
    public void testRemoval() throws Exception {
        val keys = generateKeys();
        val s = new EntrySerializer();
        val expectedLength = keys.stream().map(s::getRemovalLength).mapToInt(i -> i).sum();
        val serialization = s.serializeRemoval(keys).getCopy();
        Assert.assertEquals(expectedLength, serialization.length);

        int offset = 0;
        for (val key : keys) {
            val header = s.readHeader(new ByteArraySegment(serialization, offset, serialization.length - offset).getBufferViewReader());
            Assert.assertEquals("Unexpected key length.", key.getKey().getLength(), header.getKeyLength());
            Assert.assertTrue("Unexpected value from isDeletion().", header.isDeletion());

            Assert.assertEquals("Unexpected serialized key.",
                    new ByteArraySegment(serialization, offset + header.getKeyOffset(), header.getKeyLength()),
                    key.getKey());

            AssertExtensions.assertThrows(
                    "Able to retrieve value for deletion header.",
                    header::getValueOffset,
                    ex -> ex instanceof IllegalStateException);
            offset += header.getTotalLength();
        }

        Assert.assertEquals("Did not read the entire serialization.", serialization.length, offset);
    }

    /**
     * Utility method for tests that need to append serialized TableEntries outside the tables package.
     *
     * @param entries Table Entries to serialize.
     * @return Serialized Table Entries.
     */
    @VisibleForTesting
    public static BufferView generateUpdateWithExplicitVersion(List<TableEntry> entries) {
        val s = new EntrySerializer();
        return s.serializeUpdateWithExplicitVersion(entries);
    }

    private List<TableKey> generateKeys() {
        return generateKeys(new Random(0));
    }

    private List<TableKey> generateKeys(Random rnd) {
        val result = new ArrayList<TableKey>();
        for (int i = 0; i < COUNT; i++) {
            val key = new byte[1 + rnd.nextInt(EntrySerializer.MAX_KEY_LENGTH)];
            rnd.nextBytes(key);
            result.add(TableKey.unversioned(new ByteArraySegment(key)));
        }

        return result;
    }

    private List<TableEntry> generateEntries() {
        val rnd = new Random(0);
        AtomicBoolean generatedEmpty = new AtomicBoolean(false);
        return generateKeys(rnd)
                .stream()
                .map(key -> {
                    byte[] value = new byte[generatedEmpty.get() ? rnd.nextInt(MAX_VALUE_SIZE) : 0];
                    generatedEmpty.set(true);
                    rnd.nextBytes(value);
                    return TableEntry.versioned(key.getKey(), new ByteArraySegment(value), Math.max(0, rnd.nextLong()));
                })
                .collect(Collectors.toList());
    }

    private int checkEntry(TableEntry e, byte[] serialization, int offset, EntrySerializer s, boolean explicitVersion) throws Exception {
        val header = s.readHeader(new ByteArraySegment(serialization, offset, serialization.length - offset).getBufferViewReader());
        if (explicitVersion) {
            Assert.assertEquals("Unexpected explicit version serialized.", e.getKey().getVersion(), header.getEntryVersion());
        } else {
            Assert.assertEquals("Not expecting an explicit version to be serialized.", TableKey.NO_VERSION, header.getEntryVersion());
        }
        Assert.assertEquals("Unexpected key length.", e.getKey().getKey().getLength(), header.getKeyLength());

        Assert.assertEquals("Unexpected value length.", e.getValue().getLength(), header.getValueLength());
        Assert.assertFalse("Unexpected value from isDeletion().", header.isDeletion());

        Assert.assertEquals("Unexpected serialized key.",
                new ByteArraySegment(serialization, offset + header.getKeyOffset(), header.getKeyLength()),
                e.getKey().getKey());

        Assert.assertEquals("Unexpected serialized value.",
                new ByteArraySegment(serialization, offset + header.getValueOffset(), header.getValueLength()),
                e.getValue());

        return header.getTotalLength();
    }
}
