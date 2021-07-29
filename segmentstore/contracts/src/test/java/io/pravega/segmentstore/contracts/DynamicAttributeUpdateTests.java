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
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link DynamicAttributeValue} class.
 */
public class DynamicAttributeUpdateTests {
    @Test
    public void testSegmentLength() {
        val si = StreamSegmentInformation.builder().name("s").length(1000).build();
        val v1 = DynamicAttributeValue.segmentLength(0);
        val au1 = new DynamicAttributeUpdate(AttributeId.randomUUID(), AttributeUpdateType.Replace, v1);
        AssertExtensions.assertThrows("", au1::getValue, ex -> ex instanceof IllegalStateException);
        au1.setValue(v1.evaluate(si));
        Assert.assertEquals(au1.toString(), si.getLength(), au1.getValue());

        val v2 = DynamicAttributeValue.segmentLength(-10);
        val au2 = new DynamicAttributeUpdate(AttributeId.randomUUID(), AttributeUpdateType.ReplaceIfEquals, v1, 123L);
        au2.setValue(v2.evaluate(si));
        Assert.assertEquals(au2.toString(), si.getLength() - 10, au2.getValue());

        val v3 = DynamicAttributeValue.segmentLength(19);
        val au3 = new DynamicAttributeUpdate(AttributeId.randomUUID(), AttributeUpdateType.Accumulate, v1, 123L);
        au3.setValue(v3.evaluate(si));
        Assert.assertEquals(au3.toString(), si.getLength() + 19, au3.getValue());
    }
}
