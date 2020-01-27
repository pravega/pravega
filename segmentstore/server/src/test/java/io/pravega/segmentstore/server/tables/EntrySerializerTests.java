/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
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
        val length = entries.stream().map(s::getUpdateLength).mapToInt(i -> i).sum();
        byte[] serialization = new byte[length];
        s.serializeUpdate(entries, serialization);

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
        val length = entries.stream().map(s::getUpdateLength).mapToInt(i -> i).sum();
        byte[] serialization = new byte[length];
        s.serializeUpdateWithExplicitVersion(entries, serialization);

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
        val length = keys.stream().map(s::getRemovalLength).mapToInt(i -> i).sum();
        byte[] serialization = new byte[length];
        s.serializeRemoval(keys, serialization);

        int offset = 0;
        for (val key : keys) {
            val headerStream = s.readHeader(new ByteArrayInputStream(serialization, offset, serialization.length - offset));
            val headerArray = s.readHeader(new ByteArraySegment(serialization, offset, serialization.length - offset));
            Assert.assertEquals("Unexpected key length (stream).", key.getKey().getLength(), headerStream.getKeyLength());
            Assert.assertEquals("Unexpected key length (array).", key.getKey().getLength(), headerArray.getKeyLength());
            Assert.assertTrue("Unexpected value from isDeletion().", headerStream.isDeletion() && headerArray.isDeletion());

            AssertExtensions.assertArrayEquals("Unexpected serialized key.",
                    serialization, offset + headerStream.getKeyOffset(),
                    key.getKey().array(), key.getKey().arrayOffset(), headerStream.getKeyLength());

            AssertExtensions.assertThrows(
                    "Able to retrieve value for deletion header.",
                    headerStream::getValueOffset,
                    ex -> ex instanceof IllegalStateException);
            offset += headerArray.getTotalLength();
        }

        Assert.assertEquals("Did not read the entire serialization.", serialization.length, offset);
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
        val headerStream = s.readHeader(new ByteArrayInputStream(serialization, offset, serialization.length - offset));
        val headerArray = s.readHeader(new ByteArraySegment(serialization, offset, serialization.length - offset));
        Assert.assertEquals("Mismatch getEntryVersion", headerStream.getEntryVersion(), headerArray.getEntryVersion());
        if (explicitVersion) {
            Assert.assertEquals("Unexpected explicit version serialized.", e.getKey().getVersion(), headerStream.getEntryVersion());
        } else {
            Assert.assertEquals("Not expecting an explicit version to be serialized.", TableKey.NO_VERSION, headerStream.getEntryVersion());
        }
        Assert.assertEquals("Unexpected key length (stream).", e.getKey().getKey().getLength(), headerStream.getKeyLength());
        Assert.assertEquals("Unexpected key length (array).", e.getKey().getKey().getLength(), headerArray.getKeyLength());

        Assert.assertEquals("Unexpected value length (stream).", e.getValue().getLength(), headerStream.getValueLength());
        Assert.assertEquals("Unexpected value length (array).", e.getValue().getLength(), headerArray.getValueLength());
        Assert.assertFalse("Unexpected value from isDeletion().", headerStream.isDeletion() || headerArray.isDeletion());

        AssertExtensions.assertArrayEquals("Unexpected serialized key.",
                serialization, offset + headerStream.getKeyOffset(),
                e.getKey().getKey().array(), e.getKey().getKey().arrayOffset(), headerStream.getKeyLength());

        AssertExtensions.assertArrayEquals("Unexpected serialized value.",
                serialization, offset + headerStream.getValueOffset(),
                e.getValue().array(), e.getValue().arrayOffset(), headerStream.getValueLength());

        return headerArray.getTotalLength();
    }
}
