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
import java.util.Comparator;
import java.util.HashSet;
import java.util.UUID;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link BucketUpdate} class.
 */
public class BucketUpdateTests {
    /**
     * Tests the Builder in the absence of copied entries.
     */
    @Test
    public void testBuilder() throws Exception {
        int count = 5;
        val bucket = new TableBucket(UUID.randomUUID(), 0L);
        val builder = BucketUpdate.forBucket(bucket);
        val b1 = builder.build();
        Assert.assertEquals("Unexpected bucket.", bucket, b1.getBucket());
        Assert.assertFalse("Not expecting any updates at this time.", b1.hasUpdates());

        for (int i = 0; i < count; i++) {
            builder.withExistingKey(new BucketUpdate.KeyInfo(new ByteArraySegment(new byte[]{(byte) i}), i, i));
            builder.withKeyUpdate(new BucketUpdate.KeyUpdate(new ByteArraySegment(new byte[]{(byte) -i}), i, i, i % 2 == 0));
        }

        val b2 = builder.build();
        Assert.assertTrue("Unexpected result from isKeyUpdated for updated key.",
                b2.isKeyUpdated(new ByteArraySegment(new byte[]{(byte) -1})));
        Assert.assertFalse("Unexpected result from isKeyUpdated for non-updated key.",
                b2.isKeyUpdated(new ByteArraySegment(new byte[]{(byte) -count})));

        Assert.assertEquals("Unexpected existing keys count.", count, b2.getExistingKeys().size());
        Assert.assertEquals("Unexpected updated keys count.", count, b2.getKeyUpdates().size());

        val existingIterator = b2.getExistingKeys().stream().sorted(Comparator.comparingLong(BucketUpdate.KeyInfo::getOffset)).iterator();
        val updatesIterator = b2.getKeyUpdates().stream().sorted(Comparator.comparingLong(BucketUpdate.KeyInfo::getOffset)).iterator();
        for (int i = 0; i < count; i++) {
            val e = existingIterator.next();
            val u = updatesIterator.next();
            Assert.assertEquals("Unexpected key for existing " + i, (byte) i, (byte) e.getKey().getReader().read());
            Assert.assertEquals("Unexpected offset for existing " + i, i, e.getOffset());
            Assert.assertEquals("Unexpected key for update " + i, (byte) -i, (byte) u.getKey().getReader().read());
            Assert.assertEquals("Unexpected offset for update " + i, i, u.getOffset());
            Assert.assertEquals("Unexpected value for isDeleted " + i, i % 2 == 0, u.isDeleted());
        }
    }

    /**
     * Tests the Builder in the presence of copied entries.
     */
    @Test
    public void testBuilderCopiedEntries() {
        int count = 20;
        val bucket = new TableBucket(UUID.randomUUID(), 0L);
        val builder = BucketUpdate.forBucket(bucket);
        val expectedUpdate = new HashSet<Integer>();

        for (int i = 0; i < count; i++) {
            boolean hasExistingKey = i % 3 == 0;
            boolean isCopy = i % 5 == 0;
            boolean copyWithSmallerVersion = i % 10 == 0;
            if (hasExistingKey) {
                builder.withExistingKey(new BucketUpdate.KeyInfo(new ByteArraySegment(new byte[]{(byte) i}), i + 1, i + 1));
            }

            long updateVersion = isCopy
                    ? (copyWithSmallerVersion ? i : i + 1)
                    : (i + 1) * 10; // No copy
            builder.withKeyUpdate(new BucketUpdate.KeyUpdate(new ByteArraySegment(new byte[]{(byte) i}), (i + 1) * 10, updateVersion, false));
            if (hasExistingKey && !copyWithSmallerVersion || (!hasExistingKey && !isCopy)) {
                expectedUpdate.add(i);
            }
        }

        val b = builder.build();
        for (int i = 0; i < count; i++) {
            boolean expectedIsUpdated = expectedUpdate.contains(i);
            boolean isUpdated = b.isKeyUpdated(new ByteArraySegment(new byte[]{(byte) i}));
            Assert.assertEquals("Unexpected update status for key " + i, expectedIsUpdated, isUpdated);
        }
    }

    /**
     * Tests the {@link BucketUpdate.KeyInfo#supersedes} method and {@link BucketUpdate.Builder#build} with superseded
     * Key updates.
     */
    @Test
    public void testSuperseding() {
        val bucket = new TableBucket(UUID.randomUUID(), 0L);

        val existingKey = new BucketUpdate.KeyInfo(new ByteArraySegment(new byte[]{(byte) 1}), 10, 9);

        // This one has the same version but higher offset, so it should supersede it.
        val sameVersionHigherOffsetUpdate = new BucketUpdate.KeyUpdate(new ByteArraySegment(new byte[]{(byte) 1}), 11, 9, false);
        Assert.assertTrue(sameVersionHigherOffsetUpdate.supersedes(existingKey));
        Assert.assertFalse(existingKey.supersedes(sameVersionHigherOffsetUpdate));
        val bu1 = BucketUpdate.forBucket(bucket)
                .withExistingKey(existingKey)
                .withKeyUpdate(sameVersionHigherOffsetUpdate)
                .build();
        Assert.assertTrue("Expected superseded key to be reported as updated", bu1.isKeyUpdated(existingKey.getKey()));
        Assert.assertEquals("Unexpected updated key that supersedes existing key.", sameVersionHigherOffsetUpdate, bu1.getKeyUpdates().stream().findFirst().orElse(null));
        Assert.assertEquals("Unexpected Bucket Offset for superseding key.", sameVersionHigherOffsetUpdate.getOffset(), bu1.getBucketOffset());

        // This one has a lower version but higher offset.
        val lowerVersionUpdate = new BucketUpdate.KeyUpdate(new ByteArraySegment(new byte[]{(byte) 1}), 12, 8, false);
        Assert.assertTrue(existingKey.supersedes(lowerVersionUpdate));
        Assert.assertFalse(lowerVersionUpdate.supersedes(existingKey));
        val bu2 = BucketUpdate.forBucket(bucket)
                .withExistingKey(existingKey)
                .withKeyUpdate(lowerVersionUpdate)
                .build();
        Assert.assertFalse("Expected non-superseded key to not be reported as updated", bu2.isKeyUpdated(existingKey.getKey()));
        Assert.assertTrue("Not expecting any updates for non-superseded key.", bu2.getKeyUpdates().isEmpty());
        Assert.assertEquals("Unexpected Bucket Offset for non-superseding key.", existingKey.getOffset(), bu2.getBucketOffset());
    }
}
