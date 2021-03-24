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

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import java.util.Arrays;
import java.util.function.Function;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link KeyTranslator}.
 */
public class KeyTranslatorTests {
    /**
     * Tests {@link KeyTranslator#identity()}.
     */
    @Test
    public void testIdentity() {
        testTranslator(KeyTranslator.identity(), a -> a);
    }

    /**
     * Tests {@link KeyTranslator#partitioned}.
     */
    @Test
    public void testPartitioned() {
        byte partition = (byte) 'p';
        Function<ArrayView, ArrayView> converter = a -> {
            byte[] r = new byte[a.getLength() + 1];
            r[0] = partition;
            System.arraycopy(a.array(), a.arrayOffset(), r, 1, r.length - 1);
            return new ByteArraySegment(r);
        };
        testTranslator(KeyTranslator.partitioned(partition), converter);
    }

    private void testTranslator(KeyTranslator translator, Function<ArrayView, ArrayView> externalToInternal) {
        val tests = Arrays.asList(
                new byte[0],
                new byte[]{Byte.MAX_VALUE},
                new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0});
        for (val t : tests) {
            val copy = Arrays.copyOf(t, t.length); // Make a copy to verify we aren't altering this in any form.

            val keyData = new ByteArraySegment(t);
            val key = TableKey.versioned(keyData, 12345L);
            val entry = TableEntry.versioned(keyData, new ByteArraySegment(new byte[]{10, 11}), 64789L);
            val expectedInboundKeyData = externalToInternal.apply(keyData);

            boolean isIdentity = keyData.equals(expectedInboundKeyData);
            Assert.assertEquals(isIdentity, translator.isInternal(keyData));
            Assert.assertEquals(isIdentity, translator.isInternal(key));

            val inboundKeyData = translator.inbound(keyData);
            Assert.assertEquals(expectedInboundKeyData, inboundKeyData);
            Assert.assertTrue(translator.isInternal(expectedInboundKeyData));

            val expectedInboundKey = TableKey.versioned(expectedInboundKeyData, key.getVersion());
            val inboundKey = translator.inbound(key);
            Assert.assertEquals(expectedInboundKey, inboundKey);
            Assert.assertTrue(translator.isInternal(inboundKey));

            val expectedInboundEntry = TableEntry.versioned(expectedInboundKeyData, entry.getValue(), entry.getKey().getVersion());
            val inboundEntry = translator.inbound(entry);
            Assert.assertEquals(expectedInboundEntry, inboundEntry);

            val outboundKeyData = translator.outbound(inboundKeyData);
            Assert.assertEquals(keyData, outboundKeyData);

            val outboundKey = translator.outbound(inboundKey);
            Assert.assertEquals(key, outboundKey);

            val outboundEntry = translator.outbound(inboundEntry);
            Assert.assertEquals(entry, outboundEntry);

            Assert.assertArrayEquals(copy, t);
        }
    }
}
