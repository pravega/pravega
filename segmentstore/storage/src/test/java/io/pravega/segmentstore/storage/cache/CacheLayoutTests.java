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
package io.pravega.segmentstore.storage.cache;

import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.BiFunction;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link CacheLayout} class.
 */
public class CacheLayoutTests {
    /**
     * Tests the {@link CacheLayout#calculateAddress}, {@link CacheLayout#getBufferId}
     * and {@link CacheLayout#getBlockId(int)} methods.
     */
    @Test
    public void testAddresses() {
        Assert.assertEquals("getBlockId(NO_ADDRESS)", CacheLayout.NO_BLOCK_ID, layout().getBlockId(CacheLayout.NO_ADDRESS));
        Assert.assertEquals("getBufferId(NO_ADDRESS)", 0, layout().getBufferId(CacheLayout.NO_ADDRESS));
        AssertExtensions.assertThrows("calculateAddress with negative block id",
                () -> layout().calculateAddress(-1, 1),
                ex -> ex instanceof AssertionError);
        AssertExtensions.assertThrows("calculateAddress with negative buffer id",
                () -> layout().calculateAddress(1, -1),
                ex -> ex instanceof AssertionError);

        // Exhaustively test every combination. There should be about 16M of them, but this method is (or should be)
        // very fast to complete.
        for (int bufferId = 0; bufferId < layout().maxBufferCount(); bufferId++) {
            for (int blockId = 0; blockId < layout().blocksPerBuffer(); blockId++) {
                int address = layout().calculateAddress(bufferId, blockId);
                Assert.assertEquals(bufferId, layout().getBufferId(address));
                Assert.assertEquals(blockId, layout().getBlockId(address));
            }
        }
    }

    /**
     * Tests {@link CacheLayout#setPredecessorAddress} and {@link CacheLayout#getPredecessorAddress}.
     */
    @Test
    public void testPredecessorAddress() {
        testMetadataField(CacheLayout::getPredecessorAddress, CacheLayout::setPredecessorAddress, CacheLayout.NO_ADDRESS, getAddressBitCount());
    }

    /**
     * Tests {@link CacheLayout#setLength} and {@link CacheLayout#getLength}.
     */
    @Test
    public void testLength() {
        testMetadataField(CacheLayout::getLength, CacheLayout::setLength, 0, getLengthBitCount());
    }

    /**
     * Tests {@link CacheLayout#setNextFreeBlockId} and {@link CacheLayout#getNextFreeBlockId}.
     */
    @Test
    public void testNextFreeBlockId() {
        testMetadataField(CacheLayout::getNextFreeBlockId, CacheLayout::setNextFreeBlockId, CacheLayout.NO_BLOCK_ID, getBlockIdBitCount());
    }

    /**
     * Tests {@link CacheLayout#newBlockMetadata}, {@link CacheLayout#emptyBlockMetadata()} and others.
     */
    @Test
    public void testNewBlockMetadata() {
        val blockIds = getAllOneBitNumbers(getBlockIdBitCount());
        val lengths = getAllOneBitNumbers(getLengthBitCount());
        val addresses = getAllOneBitNumbers(getAddressBitCount());
        val firsts = Arrays.asList(true, false);
        for (boolean first : firsts) {
            for (long b : blockIds) {
                int expectedBlockId = (int) b;
                for (long l : lengths) {
                    int expectedLength = (int) l;
                    for (long a : addresses) {
                        int expectedAddress = (int) a;
                        long m = layout().newBlockMetadata(expectedBlockId, expectedLength, expectedAddress);
                        Assert.assertTrue(layout().isUsedBlock(m));
                        Assert.assertEquals(expectedBlockId, layout().getNextFreeBlockId(m));
                        Assert.assertEquals(expectedLength, layout().getLength(m));
                        Assert.assertEquals(expectedAddress, layout().getPredecessorAddress(m));
                    }
                }
            }
        }

        // Test the empty metadata.
        Assert.assertFalse(layout().isUsedBlock(layout().emptyBlockMetadata()));
        Assert.assertEquals(CacheLayout.NO_BLOCK_ID, layout().getNextFreeBlockId(layout().emptyBlockMetadata()));
        Assert.assertEquals(0, layout().getLength(layout().emptyBlockMetadata()));
        Assert.assertEquals(CacheLayout.NO_ADDRESS, layout().getPredecessorAddress(layout().emptyBlockMetadata()));
    }

    private void testMetadataField(BiFunction<CacheLayout, Long, Integer> get, TriFunction<CacheLayout, Long, Integer, Long> set, int emptyResult, int fieldBitCount) {
        Assert.assertEquals("empty", CacheLayout.NO_BLOCK_ID, (int) get.apply(layout(), layout().emptyBlockMetadata()));
        val blockMetadata = getAllOneBitNumbers(Long.SIZE);
        val fieldValues = getAllOneBitNumbers(fieldBitCount);
        for (long bm : blockMetadata) {
            for (long v : fieldValues) {
                int expectedValue = (int) v;
                long newMetadata = set.apply(layout(), bm, expectedValue);
                int storedAdddress = get.apply(layout(), newMetadata);
                Assert.assertEquals("Unexpected result for metadata " + bm + ", value " + expectedValue, expectedValue, storedAdddress);
            }
        }
    }

    private ArrayList<Long> getAllOneBitNumbers(int bitCount) {
        val result = new ArrayList<Long>();
        result.add(0L);
        long value = 1;
        for (int i = 0; i < bitCount; i++) {
            result.add(value);
            value = value << 1;
        }

        return result;
    }

    private CacheLayout layout() {
        return new CacheLayout.DefaultLayout();
    }

    private int getAddressBitCount() {
        return CacheLayout.DefaultLayout.ADDRESS_BIT_COUNT;
    }

    private int getBlockIdBitCount() {
        return CacheLayout.DefaultLayout.BLOCK_ID_BIT_COUNT;
    }

    private int getLengthBitCount() {
        return CacheLayout.DefaultLayout.BLOCK_LENGTH_BIT_COUNT;
    }

    @FunctionalInterface
    public interface TriFunction<T1, T2, T3, R> {
        R apply(T1 var1, T2 var2, T3 var3);
    }
}
