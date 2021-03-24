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
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link KeyUpdateCollection} class.
 */
public class KeyUpdateCollectionTests {
    /**
     * Tests the {@link KeyUpdateCollection#add} method.
     */
    @Test
    public void testAdd() {
        final int keyCount = 10;
        final int duplication = 2;
        val rnd = new Random(0);
        long maxOffset = 0;
        long maxHighestOffset = -1;
        val allUpdates = new ArrayList<BucketUpdate.KeyUpdate>();
        val c = new KeyUpdateCollection(10);
        Assert.assertEquals("Length differs.", 10, c.getLength());
        for (int i = 0; i < keyCount; i++) {
            byte[] key = new byte[i + 1];
            rnd.nextBytes(key);
            for (int j = 0; j < duplication; j++) {
                long offset = 1 + rnd.nextInt(10000);
                int length = key.length + rnd.nextInt(200);
                boolean copiedEntry = i % 2 == j % 2;
                long version = offset / 2 + (copiedEntry ? 1 : -1);
                long originalOffset = copiedEntry ? version : -1;
                val update = new BucketUpdate.KeyUpdate(new ByteArraySegment(key), offset, version, false);
                c.add(update, length, originalOffset);
                maxOffset = Math.max(maxOffset, offset + length);
                if (originalOffset >= 0) {
                    maxHighestOffset = Math.max(maxHighestOffset, originalOffset + length);
                }

                allUpdates.add(update);
            }
        }

        Assert.assertEquals("Unexpected TotalUpdateCount.", keyCount * duplication, c.getTotalUpdateCount());
        Assert.assertEquals("Unexpected LastIndexedOffset.", maxOffset, c.getLastIndexedOffset());
        Assert.assertEquals("Unexpected HighestCopiedOffset.", maxHighestOffset, c.getHighestCopiedOffset());

        val allByKey = allUpdates.stream().collect(Collectors.groupingBy(BucketUpdate.KeyInfo::getKey));
        val expected = allByKey.values().stream()
                .map(l -> l.stream().max(Comparator.comparingLong(BucketUpdate.KeyInfo::getVersion)).get())
                .sorted(Comparator.comparingLong(BucketUpdate.KeyInfo::getOffset))
                .collect(Collectors.toList());
        val actual = c.getUpdates().stream()
                .sorted(Comparator.comparingLong(BucketUpdate.KeyInfo::getOffset))
                .collect(Collectors.toList());

        AssertExtensions.assertListEquals("Unexpected result from getUpdates().", expected, actual,
                (u1, u2) -> u1.getKey().equals(u2.getKey()) && u1.getVersion() == u2.getVersion() && u1.getOffset() == u2.getOffset());
    }
}
