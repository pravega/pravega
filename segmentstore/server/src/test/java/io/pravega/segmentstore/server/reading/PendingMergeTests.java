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
package io.pravega.segmentstore.server.reading;

import io.pravega.test.common.AssertExtensions;
import java.util.Arrays;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the PendingMerge class.
 */
public class PendingMergeTests {
    /**
     * Tests the functionality of the PendingMerge class.
     */
    @Test
    public void testFunctionality() {
        val m = new PendingMerge(123);
        Assert.assertEquals("Unexpected value from getMergeOffset().", 123, m.getMergeOffset());

        val toRegister = Arrays.asList(
                new FutureReadResultEntry(1, 10),
                new FutureReadResultEntry(2, 20),
                new FutureReadResultEntry(3, 30));
        for (val e : toRegister) {
            boolean result = m.register(e);
            Assert.assertTrue("Unexpected return value from register() when not sealed.", result);
        }

        val sealResult = m.seal();
        AssertExtensions.assertListEquals("Unexpected result from seal().", toRegister, sealResult, Object::equals);
        Assert.assertFalse("Not expecting new calls to register() to work after calling seal().",
                m.register(new FutureReadResultEntry(4, 40)));
        val seal2Result = m.seal();
        Assert.assertEquals("Not expecting any items after the second call to seal().", 0, seal2Result.size());
    }
}
