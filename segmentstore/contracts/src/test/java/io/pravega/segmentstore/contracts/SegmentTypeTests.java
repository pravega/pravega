/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

import io.pravega.segmentstore.contracts.tables.TableAttributes;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
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
        checkBuilder(SegmentType.builder().sortedTableSegment().build(), SegmentType.FORMAT_SORTED_TABLE_SEGMENT,
                SegmentType::isTableSegment, SegmentType::isSortedTableSegment);
        checkBuilder(SegmentType.builder().internal().build(), SegmentType.ROLE_INTERNAL, SegmentType::isInternal);
        checkBuilder(SegmentType.builder().system().build(), SegmentType.ROLE_SYSTEM, SegmentType::isSystem, SegmentType::isInternal);
        checkBuilder(SegmentType.builder().critical().build(), SegmentType.ROLE_CRITICAL, SegmentType::isCritical);
    }

    /**
     * Tests {@link SegmentType#fromAttributes} and {@link SegmentType#intoAttributes}.
     */
    @Test
    public void testToFromAttributes() {
        val empty = SegmentType.fromAttributes(Collections.emptyMap());
        Assert.assertEquals("Empty attributes. ", SegmentType.FORMAT_BASIC, empty.getValue());

        val baseType = SegmentType.builder().critical().internal().system().build();
        val segmentAttributes = new HashMap<UUID, Long>();
        Assert.assertTrue(baseType.intoAttributes(segmentAttributes));
        Assert.assertFalse(baseType.intoAttributes(segmentAttributes));

        val nonTableSegment = SegmentType.fromAttributes(segmentAttributes);
        Assert.assertEquals("Non-table segment.", baseType, nonTableSegment);

        segmentAttributes.put(TableAttributes.INDEX_OFFSET, 0L);
        val simpleTableSegment = SegmentType.fromAttributes(segmentAttributes);
        val expectedSimpleSegment = SegmentType.builder(baseType).tableSegment().build();
        Assert.assertEquals("Simple Table Segment.", expectedSimpleSegment, simpleTableSegment);

        segmentAttributes.put(TableAttributes.SORTED, Attributes.BOOLEAN_FALSE);
        Assert.assertEquals("Simple Table Segment (Sorted==False).", expectedSimpleSegment, SegmentType.fromAttributes(segmentAttributes));

        segmentAttributes.put(TableAttributes.SORTED, Attributes.BOOLEAN_TRUE);
        val sortedTableSegment = SegmentType.fromAttributes(segmentAttributes);
        val expectedSortedSegment = SegmentType.builder(expectedSimpleSegment).sortedTableSegment().build();
        Assert.assertEquals("Sorted Table Segment.", expectedSortedSegment, sortedTableSegment);
    }

    private void checkBuilder(SegmentType type, long expectedValue, Predicate<SegmentType>... predicates) {
        check(type, expectedValue, predicates);
        val rebuilt = SegmentType.builder(type).build();
        check(rebuilt, expectedValue, predicates); // Inherited builder.
        Assert.assertEquals(type, rebuilt);
        Assert.assertEquals(type.hashCode(), rebuilt.hashCode());
    }

    private void check(SegmentType type, long expectedValue, Predicate<SegmentType>... predicates) {
        Assert.assertEquals("Unexpected getValue() for " + type.toString(), expectedValue, type.getValue());
        for (val p : predicates) {
            Assert.assertTrue("Check failed for " + type.toString(), p.test(type));
        }
    }
}
