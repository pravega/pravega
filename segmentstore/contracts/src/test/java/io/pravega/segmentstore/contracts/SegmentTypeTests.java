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

import io.pravega.segmentstore.contracts.tables.TableAttributes;
import java.util.Collections;
import java.util.HashMap;
import java.util.function.Predicate;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link SegmentType} class.
 */
public class SegmentTypeTests {

    /**
     * Tests all {@link SegmentType}s with a single value.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testBuilder() {
        checkBuilder(SegmentType.STREAM_SEGMENT, SegmentType.FORMAT_BASIC);
        checkBuilder(SegmentType.builder().tableSegment().build(), SegmentType.FORMAT_TABLE_SEGMENT, SegmentType::isTableSegment);
        checkBuilder(SegmentType.builder().fixedKeyLengthTableSegment().build(), SegmentType.FORMAT_FIXED_KEY_LENGTH_TABLE_SEGMENT,
                SegmentType::isTableSegment, SegmentType::isFixedKeyLengthTableSegment);
        checkBuilder(SegmentType.builder().internal().build(), SegmentType.ROLE_INTERNAL, SegmentType::isInternal);
        checkBuilder(SegmentType.builder().system().build(), SegmentType.ROLE_SYSTEM, SegmentType::isSystem, SegmentType::isInternal);
        checkBuilder(SegmentType.builder().critical().build(), SegmentType.ROLE_CRITICAL, SegmentType::isCritical);
        checkBuilder(SegmentType.builder().transientSegment().build(), SegmentType.ROLE_TRANSIENT, SegmentType::isTransientSegment);
    }

    /**
     * Tests {@link SegmentType#fromAttributes} and {@link SegmentType#intoAttributes}.
     */
    @Test
    public void testToFromAttributes() {
        val empty = SegmentType.fromAttributes(Collections.emptyMap());
        Assert.assertEquals("Empty attributes. ", SegmentType.FORMAT_BASIC, empty.getValue());

        val baseType = SegmentType.builder().critical().internal().system().build();
        val segmentAttributes = new HashMap<AttributeId, Long>();
        Assert.assertTrue(baseType.intoAttributes(segmentAttributes));
        Assert.assertFalse(baseType.intoAttributes(segmentAttributes));

        val nonTableSegment = SegmentType.fromAttributes(segmentAttributes);
        Assert.assertEquals("Non-table segment.", baseType, nonTableSegment);

        segmentAttributes.put(TableAttributes.INDEX_OFFSET, 0L);
        val simpleTableSegment = SegmentType.fromAttributes(segmentAttributes);
        val expectedSimpleSegment = SegmentType.builder(baseType).tableSegment().build();
        Assert.assertEquals("Simple Table Segment.", expectedSimpleSegment, simpleTableSegment);
    }

    @SafeVarargs
    private void checkBuilder(SegmentType type, long expectedValue, Predicate<SegmentType>... predicates) {
        check(type, expectedValue, predicates);
        val rebuilt = SegmentType.builder(type).build();
        check(rebuilt, expectedValue, predicates); // Inherited builder.
        Assert.assertEquals(type, rebuilt);
        Assert.assertEquals(type.hashCode(), rebuilt.hashCode());
    }

    @SafeVarargs
    private void check(SegmentType type, long expectedValue, Predicate<SegmentType>... predicates) {
        Assert.assertEquals("Unexpected getValue() for " + type.toString(), expectedValue, type.getValue());
        for (val p : predicates) {
            Assert.assertTrue("Check failed for " + type.toString(), p.test(type));
        }
    }
}
