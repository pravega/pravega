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
package io.pravega.segmentstore.storage;

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link QueueStats} class.
 */
public class QueueStatsTests {
    private static final double RATIO_ERROR = 0.01; // How much we're willing to tolerate for calculation errors.

    /**
     * Tests the {@link QueueStats#getAverageItemFillRatio()}
     */
    @Test
    public void testGetAverageItemFillRatio() {
        final int fixedSize = 100;
        final int fixedMaxWriteLength = 1024 * 1024;
        final int fixedTotalLength = fixedMaxWriteLength * fixedSize;

        // Check extremes.
        checkFillRatio(0.0, 0, 1000, 1000);
        checkFillRatio(1, 1, 1000, 1000);

        // Keep Size constant and vary total length.
        for (double ratio = 0; ratio < 1.0; ratio += RATIO_ERROR) {
            int totalLength = (int) Math.round(fixedMaxWriteLength * fixedSize * ratio);
            checkFillRatio(ratio, fixedSize, totalLength, fixedMaxWriteLength);
        }

        // Keep Total Length constant and vary size.
        for (double ratio = RATIO_ERROR; ratio < 1.0; ratio += RATIO_ERROR) {
            int size = (int) Math.round(fixedTotalLength / (fixedMaxWriteLength * ratio));
            checkFillRatio(ratio, size, fixedTotalLength, fixedMaxWriteLength);
        }
    }

    private void checkFillRatio(double expected, int size, long totalLength, int maxWriteLength) {
        val qs = new QueueStats(size, totalLength, maxWriteLength, Integer.MAX_VALUE);
        Assert.assertEquals(
                String.format("Unexpected FillRatio for TotalLength=%s, MaxWriteLength=%d, QS=%s",
                        qs.getTotalLength(), qs.getMaxWriteLength(), qs),
                expected, qs.getAverageItemFillRatio(), RATIO_ERROR);
    }
}
