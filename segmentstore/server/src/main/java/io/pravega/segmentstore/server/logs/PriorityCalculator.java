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

/**
 * Calculates {@link OperationPriority} based on various factors.
 */
public final class PriorityCalculator {

    /**
     * Determines an {@link OperationPriority} appropriate for the given {@link SegmentType} and {@link OperationType}.
     *
     * <pre>
     *
     * SegmentType       | OperationType | Priority
     * ------------------+---------------+----------------
     * System &amp; Critical | (any)         | {@link OperationPriority#SystemCritical}
     * Critical          | (any)         | {@link OperationPriority#Critical}
     * (any)             | Deletion      | {@link OperationPriority#Critical}
     * System            | Normal        | {@link OperationPriority#High}
     * (all other combinations)          | {@link OperationPriority#Normal}
     *
     * </pre>
     *
     * @param segmentType   {@link SegmentType} that the Operation applies to.
     * @param operationType Operation's {@link OperationType}.
     * @return A {@link OperationPriority}.
     */
    public static OperationPriority getPriority(SegmentType segmentType, OperationType operationType) {
        if (segmentType.isSystem() && segmentType.isCritical()) {
            // Only Segments marked as System and Critical can get the highest possible priority. These are "must-process"
            // Segments and nothing else must get in their way, not even the Operation Processor throttling.
            // Non-Critical System segments will fall into one of the other types of priorities.
            return OperationPriority.SystemCritical;
        } else if (segmentType.isCritical() || operationType == OperationType.Deletion) {
            // Deletion operations can be expedited without issues.
            // Any other Critical segments go in here.
            return OperationPriority.Critical;
        } else if (segmentType.isSystem()) {
            // Segments marked as System (but not Critical) which do not need an elevated priority due to the nature of
            // their operations, will still get priority above all other operations.
            return OperationPriority.High;
        }

        // Everything else.
        return OperationPriority.Normal;
    }
}
