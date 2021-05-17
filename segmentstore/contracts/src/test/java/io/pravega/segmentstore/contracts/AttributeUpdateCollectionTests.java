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

import io.pravega.test.common.AssertExtensions;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link AttributeUpdateCollection} class.
 */
public class AttributeUpdateCollectionTests {
    private static final int COUNT = 10;

    @Test
    public void testAdd() {
        val uuids = generate(i -> AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX, i));
        val variables = generate(i -> AttributeId.from(new byte[]{(byte) (int) i}));

        val c = new AttributeUpdateCollection();
        c.addAll(uuids);
        Assert.assertFalse(c.hasVariableAttributeIds());
        c.addAll(variables);
        Assert.assertTrue(c.hasVariableAttributeIds());
        Assert.assertEquals(uuids.size() + variables.size(), c.size());
        AssertExtensions.assertContainsSameElements("", uuids, c.getUUIDAttributeUpdates(), this::compare);
        AssertExtensions.assertContainsSameElements("", variables, c.getVariableAttributeUpdates(), this::compare);

        // Check dynamic ones.
        val expectedDynamicUpdates = c.stream().filter(AttributeUpdate::isDynamic).map(u -> (DynamicAttributeUpdate) u).collect(Collectors.toList());
        AssertExtensions.assertContainsSameElements("", expectedDynamicUpdates, c.getDynamicAttributeUpdates(), this::compare);
    }

    /**
     * Tests {@link AttributeUpdateCollection#getExtendedAttributeIdLength()}.
     */
    @Test
    public void testExtendedAttributeIdLength() {

        // First check with UUID attribute ids.
        val c1 = new AttributeUpdateCollection();
        Assert.assertNull(c1.getExtendedAttributeIdLength());

        // Adding Core Attributes should not change it.
        c1.add(new AttributeUpdate(Attributes.EVENT_COUNT, AttributeUpdateType.None, 1L));
        Assert.assertNull(c1.getExtendedAttributeIdLength());

        // Adding an extended Attribute should set it.
        c1.add(new AttributeUpdate(AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX + 1, 1), AttributeUpdateType.None, 1L));
        c1.add(new AttributeUpdate(AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX + 2, 1), AttributeUpdateType.None, 1L));
        c1.add(new AttributeUpdate(Attributes.CREATION_TIME, AttributeUpdateType.None, 1L));
        Assert.assertEquals(0, (int) c1.getExtendedAttributeIdLength());

        // Adding a variable extended Attribute should be rejected now (we have a UUID one set already).
        AssertExtensions.assertThrows(
                "add() accepted a variable Attribute Id after accepting a UUID attribute id",
                () -> c1.add(new AttributeUpdate(AttributeId.from(new byte[AttributeId.UUID.ATTRIBUTE_ID_LENGTH]), AttributeUpdateType.None, 1L)),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "add() accepted a variable Attribute Id after accepting a UUID attribute id",
                () -> c1.add(new AttributeUpdate(AttributeId.from(new byte[1]), AttributeUpdateType.None, 1L)),
                ex -> ex instanceof IllegalArgumentException);

        Assert.assertEquals(4, c1.size());

        // Now check with variable attribute ids.
        val c2 = new AttributeUpdateCollection();

        // Start with a core attribute (should be ignored).
        c2.add(new AttributeUpdate(Attributes.EVENT_COUNT, AttributeUpdateType.None, 1L));

        // Then add a few variable ones with the same length.
        c2.add(new AttributeUpdate(AttributeId.from(new byte[]{0, 1}), AttributeUpdateType.None, 1L));
        c2.add(new AttributeUpdate(AttributeId.from(new byte[]{1, 2}), AttributeUpdateType.None, 1L));
        c2.add(new AttributeUpdate(AttributeId.from(new byte[]{3, 3}), AttributeUpdateType.None, 1L));
        Assert.assertEquals(2, (int) c2.getExtendedAttributeIdLength());

        // Now add another core one - this one should work just fine.
        c2.add(new AttributeUpdate(Attributes.CREATION_TIME, AttributeUpdateType.None, 1L));
        Assert.assertEquals(2, (int) c2.getExtendedAttributeIdLength());

        // These should be rejected.
        AssertExtensions.assertThrows(
                "add() accepted an Attribute Id with incompatible length",
                () -> c2.add(new AttributeUpdate(AttributeId.from(new byte[3]), AttributeUpdateType.None, 1L)),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "add() accepted a UUID Attribute Id after a variable one",
                () -> c2.add(new AttributeUpdate(AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX + 1, 1), AttributeUpdateType.None, 1L)),
                ex -> ex instanceof IllegalArgumentException);
        Assert.assertEquals(5, c2.size());
    }

    /**
     * These methods are used only for various testing purposes; there is no good reason to compare two
     * {@link AttributeUpdateCollection} instances for equality in production code.
     */
    @Test
    public void testEqualsHashCode() {
        val uuids = generate(i -> AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX, i));
        val variables = generate(i -> AttributeId.from(new byte[]{(byte) (int) i}));

        val c1 = AttributeUpdateCollection.from(uuids);
        val c2 = AttributeUpdateCollection.from(variables.toArray(AttributeUpdate[]::new));
        val c3 = new AttributeUpdateCollection();
        c3.addAll(uuids);
        c3.addAll(variables);
        val c4 = AttributeUpdateCollection.from(c3);

        Assert.assertNotEquals(c1, c2);
        Assert.assertNotEquals(c1, c3);
        Assert.assertNotEquals(c2, c4);
        Assert.assertNotEquals(c2, new Object());
        Assert.assertEquals(c3, c3); // Same object
        Assert.assertEquals(c3, c4); // Same contents
        Assert.assertEquals(c4, c3); // Same contents
        Assert.assertEquals(c3.hashCode(), c4.hashCode());
    }

    private List<AttributeUpdate> generate(Function<Integer, AttributeId> createAttributeId) {
        return IntStream.range(0, COUNT)
                .mapToObj(i -> i % 2 == 0
                        ? new AttributeUpdate(createAttributeId.apply(i), AttributeUpdateType.values()[i % AttributeUpdateType.values().length], i, i)
                        : new DynamicAttributeUpdate(createAttributeId.apply(i), AttributeUpdateType.values()[i % AttributeUpdateType.values().length], DynamicAttributeValue.segmentLength(i), i))
                .collect(Collectors.toList());
    }

    private int compare(AttributeUpdate a1, AttributeUpdate a2) {
        return a1.equals(a2) ? 0 : 1;
    }
}
