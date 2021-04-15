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
package io.pravega.segmentstore.server.logs;

import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.server.logs.operations.OperationType;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link PriorityCalculator} class.
 */
public class PriorityCalculatorTests {
    private static final SegmentType[] SEGMENT_TYPES = new SegmentType[]{
            SegmentType.STREAM_SEGMENT,
            SegmentType.TABLE_SEGMENT_HASH,
            SegmentType.builder().internal().build(),
            SegmentType.builder().system().build(),
            SegmentType.builder().system().tableSegment().build(),
            SegmentType.builder().critical().build(),
            SegmentType.builder().critical().internal().tableSegment().build(),
            SegmentType.builder().system().critical().build()};
    private static final OperationType[] OPERATION_TYPES = OperationType.values();

    /**
     * Tests the {@link PriorityCalculator#getPriority} method.
     */
    @Test
    public void testGetPriority() {
        for (val st : SEGMENT_TYPES) {
            for (val ot : OPERATION_TYPES) {
                OperationPriority expected;
                if (st.isSystem()) {
                    expected = st.isCritical()
                            ? OperationPriority.SystemCritical
                            : (ot == OperationType.Deletion ? OperationPriority.Critical : OperationPriority.High);
                } else {
                    expected = st.isCritical() || (ot == OperationType.Deletion) ? OperationPriority.Critical : OperationPriority.Normal;
                }

                OperationPriority actual = PriorityCalculator.getPriority(st, ot);
                Assert.assertEquals("Unexpected priority for SegmentType = " + st + ", OperationType = " + ot, expected, actual);
            }
        }
    }
}
