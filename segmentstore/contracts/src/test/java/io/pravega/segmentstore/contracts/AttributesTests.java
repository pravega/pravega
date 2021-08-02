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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link Attributes} class.
 */
public class AttributesTests {
    /**
     * Tests {@link Attributes#isUnmodifiable(AttributeId)} and {@link Attributes#isCoreAttribute(AttributeId)}.
     */
    @Test
    public void testGeneral() {
        boolean atLeastOneUnmodifiable = false;
        for (val attributeId : getAllAttributes()) {
            val expectedUnmodifiable = attributeId.equals(Attributes.ATTRIBUTE_SEGMENT_TYPE)
                    || attributeId.equals(Attributes.ATTRIBUTE_ID_LENGTH);
            val actualUnmodifiable = Attributes.isUnmodifiable(attributeId);

            Assert.assertEquals("Unmodifiable for " + attributeId, expectedUnmodifiable, actualUnmodifiable);
            Assert.assertTrue(Attributes.isCoreAttribute(attributeId));
            atLeastOneUnmodifiable |= expectedUnmodifiable;
        }

        Assert.assertTrue(atLeastOneUnmodifiable);
    }

    private List<AttributeId> getAllAttributes() {
        return Arrays.stream(Attributes.class.getDeclaredFields())
                .filter(f -> f.getType().equals(AttributeId.class))
                .map(f -> {
                    try {
                        return (AttributeId) f.get(null);
                    } catch (IllegalAccessException ex) {
                        return null;
                        // Non-public; skip.
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
