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
package io.pravega.segmentstore.contracts;

import io.pravega.common.util.BufferViewComparator;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link AttributeId} class
 */
public class AttributeIdTests {
    /**
     * Tests basic functionality of {@link AttributeId.UUID}.
     */
    @Test
    public void testUUIDBasic() {
        val uuid = (AttributeId.UUID) AttributeId.randomUUID();
        val baseUUID = uuid.toUUID();
        Assert.assertTrue(uuid.isUUID());
        Assert.assertEquals(16, uuid.byteCount());
        Assert.assertEquals(baseUUID.getMostSignificantBits(), uuid.getBitGroup(0));
        Assert.assertEquals(baseUUID.getLeastSignificantBits(), uuid.getBitGroup(1));

        val buf = uuid.toBuffer();
        val uuid2 = AttributeId.fromUUID(new java.util.UUID(buf.getLong(0), buf.getLong(Long.BYTES)));
        Assert.assertEquals(uuid, uuid2);
    }

    /**
     * Tests {@link AttributeId#equals}, {@link AttributeId#hashCode()} and {@link AttributeId#compareTo} for {@link AttributeId.UUID}.
     */
    @Test
    public void testUUIDEqualsHashCompare() {
        val ids = new AttributeId[]{
                AttributeId.uuid(Long.MIN_VALUE, Long.MIN_VALUE),
                AttributeId.uuid(Long.MIN_VALUE, Long.MAX_VALUE),
                AttributeId.uuid(-1, 0),
                AttributeId.uuid(-1, 1),
                AttributeId.uuid(0, 1),
                AttributeId.uuid(0, Long.MAX_VALUE),
                AttributeId.uuid(Long.MAX_VALUE, Long.MAX_VALUE)};

        for (int i = 0; i < ids.length; i++) {
            for (int j = 0; j < ids.length; j++) {
                val left = ids[i];
                val right = ids[j];
                Assert.assertEquals(i == j, left.equals(right));
                if (i == j) {
                    Assert.assertEquals(left.hashCode(), right.hashCode());
                }

                val expectedCmp = Integer.compare(i, j);
                Assert.assertEquals(expectedCmp, left.compareTo(right));
            }
        }
    }

    /**
     * Tests basic functionality of {@link AttributeId.Variable}.
     */
    @Test
    public void testVariableBasic() {
        val lengths = new int[]{1, Long.BYTES, AttributeId.randomUUID().byteCount(), AttributeId.MAX_LENGTH};
        for (val length : lengths) {
            val attribute = AttributeId.random(length);

            Assert.assertFalse(attribute.isUUID());
            Assert.assertEquals(length, attribute.byteCount());
            val buf = attribute.toBuffer();
            if (length > Long.BYTES) {
                int bitGroupCount = length / Long.BYTES;
                for (int i = 0; i < bitGroupCount; i++) {
                    val expected = buf.getLong(i * Long.BYTES);
                    val actual = attribute.getBitGroup(i);
                    Assert.assertEquals(expected, actual);
                }
            }
        }
    }

    /**
     * Tests {@link AttributeId#equals}, {@link AttributeId#hashCode()} and {@link AttributeId#compareTo} for {@link AttributeId.UUID}.
     */
    @Test
    public void testVariableEqualsHashCompare() {
        val ids = new AttributeId[]{
                AttributeId.Variable.minValue(2),
                AttributeId.from(new byte[]{BufferViewComparator.MIN_VALUE, BufferViewComparator.MIN_VALUE + 1}),
                AttributeId.from(new byte[]{BufferViewComparator.MIN_VALUE, BufferViewComparator.MAX_VALUE}),
                AttributeId.from(new byte[]{1, BufferViewComparator.MIN_VALUE}),
                AttributeId.from(new byte[]{1, 1}),
                AttributeId.from(new byte[]{1, BufferViewComparator.MAX_VALUE}),
                AttributeId.from(new byte[]{BufferViewComparator.MAX_VALUE, BufferViewComparator.MAX_VALUE - 1}),
                AttributeId.Variable.maxValue(2)};

        for (int i = 0; i < ids.length; i++) {
            for (int j = 0; j < ids.length; j++) {
                val left = ids[i];
                val right = ids[j];
                Assert.assertEquals(i == j, left.equals(right));
                if (i == j) {
                    Assert.assertEquals(left.hashCode(), right.hashCode());
                }

                val expectedCmp = Integer.compare(i, j);
                Assert.assertEquals(expectedCmp, (int) Math.signum(left.compareTo(right)));
            }
        }
    }
}
