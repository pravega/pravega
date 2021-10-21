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
package io.pravega.segmentstore.storage.cache;

import io.pravega.common.util.ByteArraySegment;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link NoOpCache}.
 */
public class NoOpCacheTests {
    @Test
    public void testFunctionality() {
        @Cleanup
        val c = new NoOpCache();
        Assert.assertEquals(4096, c.getBlockAlignment());
        Assert.assertEquals(CacheLayout.MAX_ENTRY_SIZE, c.getMaxEntryLength());
        val a = c.insert(new ByteArraySegment(new byte[1]));
        val b = c.replace(a, new ByteArraySegment(new byte[2]));
        c.append(a, 1, new ByteArraySegment(new byte[1]));
        Assert.assertEquals(c.getBlockAlignment() - 1, c.getAppendableLength(1));
        c.delete(b);
        Assert.assertNull(c.get(a));
        val s = c.getState();
        Assert.assertEquals(0, s.getStoredBytes() + s.getUsedBytes() + s.getReservedBytes() + s.getAllocatedBytes());
        Assert.assertEquals(CacheLayout.MAX_TOTAL_SIZE, s.getMaxBytes());
    }
}
