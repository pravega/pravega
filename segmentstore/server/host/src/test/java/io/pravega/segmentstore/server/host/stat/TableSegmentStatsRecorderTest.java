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
package io.pravega.segmentstore.server.host.stat;

import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.OpStatsLogger;
import java.time.Duration;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the {@link TableSegmentStatsRecorderImpl} class.
 */
public class TableSegmentStatsRecorderTest {
    private static final String SEGMENT_NAME = "scope/stream/TableSegment";
    private static final Duration ELAPSED = Duration.ofMillis(123456);

    @Test
    public void testMetrics() {
        @Cleanup
        val r = new TestRecorder();

        // Create Segment.
        r.createTableSegment(SEGMENT_NAME, ELAPSED);
        verify(r.getCreateSegment()).reportSuccessEvent(ELAPSED);

        // Delete Segment.
        r.deleteTableSegment(SEGMENT_NAME, ELAPSED);
        verify(r.getDeleteSegment()).reportSuccessEvent(ELAPSED);

        // Unconditional update.
        r.updateEntries(SEGMENT_NAME, 2, false, ELAPSED);
        verify(r.getUpdateUnconditionalLatency()).reportSuccessEvent(ELAPSED);
        verify(r.getUpdateUnconditional()).add(2);

        // Conditional update.
        r.updateEntries(SEGMENT_NAME, 3, true, ELAPSED);
        verify(r.getUpdateConditionalLatency()).reportSuccessEvent(ELAPSED);
        verify(r.getUpdateConditional()).add(3);

        // Unconditional removal.
        r.removeKeys(SEGMENT_NAME, 4, false, ELAPSED);
        verify(r.getRemoveUnconditionalLatency()).reportSuccessEvent(ELAPSED);
        verify(r.getRemoveUnconditional()).add(4);

        // Conditional removal.
        r.removeKeys(SEGMENT_NAME, 5, true, ELAPSED);
        verify(r.getRemoveConditionalLatency()).reportSuccessEvent(ELAPSED);
        verify(r.getRemoveConditional()).add(5);

        // Get Keys.
        r.getKeys(SEGMENT_NAME, 6, ELAPSED);
        verify(r.getGetKeysLatency()).reportSuccessEvent(ELAPSED);
        verify(r.getGetKeys()).add(6);

        // Iterate Keys.
        r.iterateKeys(SEGMENT_NAME, 7, ELAPSED);
        verify(r.getIterateKeysLatency()).reportSuccessEvent(ELAPSED);
        verify(r.getIterateKeys()).add(7);

        // Iterate Entries.
        r.iterateEntries(SEGMENT_NAME, 8, ELAPSED);
        verify(r.getIterateEntriesLatency()).reportSuccessEvent(ELAPSED);
        verify(r.getIterateEntries()).add(8);

        // GetInfo
        r.getInfo(SEGMENT_NAME, ELAPSED);
        verify(r.getGetInfoLatency()).reportSuccessEvent(ELAPSED);
        verify(r.getGetInfo()).inc();
    }

    @RequiredArgsConstructor
    private static class TestRecorder extends TableSegmentStatsRecorderImpl {
        @Override
        protected OpStatsLogger createLogger(String name) {
            return mock(OpStatsLogger.class);
        }

        @Override
        protected Counter createCounter(String name) {
            return mock(Counter.class);
        }
    }
}
