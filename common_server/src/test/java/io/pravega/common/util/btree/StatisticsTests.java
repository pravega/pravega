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
package io.pravega.common.util.btree;

import io.pravega.test.common.AssertExtensions;
import java.util.concurrent.atomic.AtomicReference;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link Statistics} class.
 */
public class StatisticsTests {

    @Test
    public void testUpdate() {
        val s1 = Statistics.EMPTY;
        Assert.assertEquals(0, s1.getEntryCount());
        Assert.assertEquals(0, s1.getPageCount());

        val s2 = s1.update(12, 19);
        Assert.assertEquals(12, s2.getEntryCount());
        Assert.assertEquals(19, s2.getPageCount());

        val s3 = s2.update(-4, -9);
        Assert.assertEquals(8, s3.getEntryCount());
        Assert.assertEquals(10, s3.getPageCount());

        val r = new AtomicReference<Statistics>();
        AssertExtensions.assertThrows("", () -> r.set(s3.update(-10, 0)), ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("", () -> r.set(s3.update(0, -11)), ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testSerializer() throws Exception {
        val s1 = Statistics.builder().entryCount(10).pageCount(11).build();
        val data = Statistics.SERIALIZER.serialize(s1);
        val s2 = Statistics.SERIALIZER.deserialize(data);
        Assert.assertEquals(s1, s2);
    }
}
