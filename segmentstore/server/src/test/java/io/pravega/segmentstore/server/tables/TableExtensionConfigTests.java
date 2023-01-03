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

import io.pravega.common.util.ConfigurationException;
import io.pravega.test.common.AssertExtensions;
import java.time.Duration;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link TableExtensionConfig} class.
 */
public class TableExtensionConfigTests {
    @Test
    public void testDefaultValues() {
        val b = TableExtensionConfig.builder();
        val defaultConfig = b.build();
        Assert.assertEquals(EntrySerializer.MAX_BATCH_SIZE * 4, defaultConfig.getMaxTailCachePreIndexLength());
        Assert.assertEquals(EntrySerializer.MAX_BATCH_SIZE * 4, defaultConfig.getMaxTailCachePreIndexBatchLength());
        Assert.assertEquals(Duration.ofSeconds(60), defaultConfig.getRecoveryTimeout());
        Assert.assertEquals(EntrySerializer.MAX_BATCH_SIZE * 4, defaultConfig.getMaxUnindexedLength());
        Assert.assertEquals(EntrySerializer.MAX_BATCH_SIZE * 8, defaultConfig.getSystemCriticalMaxUnindexedLength());
        Assert.assertEquals(EntrySerializer.MAX_SERIALIZATION_LENGTH * 4, defaultConfig.getMaxCompactionSize());
        Assert.assertEquals(Duration.ofSeconds(30), defaultConfig.getCompactionFrequency());
        Assert.assertEquals(75, defaultConfig.getDefaultMinUtilization());
        Assert.assertEquals(EntrySerializer.MAX_SERIALIZATION_LENGTH * 4 * 4, defaultConfig.getDefaultRolloverSize());
        Assert.assertEquals(EntrySerializer.MAX_BATCH_SIZE, defaultConfig.getMaxBatchSize());
    }

    @Test
    public void testBuilder() {
        val b = TableExtensionConfig.builder();
        b.with(TableExtensionConfig.DEFAULT_MIN_UTILIZATION, 101);
        AssertExtensions.assertThrows(ConfigurationException.class, b::build); // 101 is out of the range [0, 100]

        b.with(TableExtensionConfig.DEFAULT_MIN_UTILIZATION, 10);
        b.with(TableExtensionConfig.MAX_TAIL_CACHE_PREINDEX_LENGTH, 11L);
        b.with(TableExtensionConfig.MAX_TAIL_CACHE_PREINDEX_BATCH_SIZE, 111);
        b.with(TableExtensionConfig.RECOVERY_TIMEOUT, 12);
        b.with(TableExtensionConfig.MAX_UNINDEXED_LENGTH, 13);
        b.with(TableExtensionConfig.MAX_COMPACTION_SIZE, 14);
        b.with(TableExtensionConfig.COMPACTION_FREQUENCY, 15);
        b.with(TableExtensionConfig.DEFAULT_ROLLOVER_SIZE, 16L);
        b.with(TableExtensionConfig.MAX_BATCH_SIZE, 17);
        b.with(TableExtensionConfig.SYSTEM_CRITICAL_MAX_UNINDEXED_LENGTH, 18);

        val c = b.build();
        Assert.assertEquals(10, c.getDefaultMinUtilization());
        Assert.assertEquals(11L, c.getMaxTailCachePreIndexLength());
        Assert.assertEquals(111, c.getMaxTailCachePreIndexBatchLength());
        Assert.assertEquals(Duration.ofMillis(12), c.getRecoveryTimeout());
        Assert.assertEquals(13, c.getMaxUnindexedLength());
        Assert.assertEquals(14, c.getMaxCompactionSize());
        Assert.assertEquals(Duration.ofMillis(15), c.getCompactionFrequency());
        Assert.assertEquals(16, c.getDefaultRolloverSize());
        Assert.assertEquals(17, c.getMaxBatchSize());
        Assert.assertEquals(18, c.getSystemCriticalMaxUnindexedLength());
    }
}
