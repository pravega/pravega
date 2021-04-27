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
        val uuids = generate(i -> AttributeId.uuid(i, i));
        val variables = generate(i -> AttributeId.from(new byte[]{(byte) (int) i}));

        val c = new AttributeUpdateCollection();
        c.addAll(uuids);
        Assert.assertFalse(c.hasVariableAttributeIds());
        c.addAll(variables);
        Assert.assertTrue(c.hasVariableAttributeIds());
        Assert.assertEquals(uuids.size() + variables.size(), c.size());
        AssertExtensions.assertContainsSameElements("", uuids, c.getUUIDAttributeUpdates(), this::compare);
        AssertExtensions.assertContainsSameElements("", variables, c.getVariableAttributeUpdates(), this::compare);
    }

    /**
     * These methods are used only for various testing purposes; there is no good reason to compare two
     * {@link AttributeUpdateCollection} instances for equality in production code.
     */
    @Test
    public void testEqualsHashCode() {
        val uuids = generate(i -> AttributeId.uuid(i, i));
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
                .mapToObj(i -> new AttributeUpdate(createAttributeId.apply(i), AttributeUpdateType.values()[i % AttributeUpdateType.values().length], i, i))
                .collect(Collectors.toList());
    }

    private int compare(AttributeUpdate a1, AttributeUpdate a2) {
        return a1.equals(a2) ? 0 : 1;
    }
}
