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
package io.pravega.segmentstore.server.logs.operations;

import java.util.Random;

/**
 * Unit tests for the DeleteSegmentOperation class.
 */
public class DeleteSegmentOperationTests extends OperationTestsBase<DeleteSegmentOperation> {
    @Override
    protected DeleteSegmentOperation createOperation(Random random) {
        DeleteSegmentOperation result = new DeleteSegmentOperation(1 + random.nextInt(1000)); // segmentId must not be zero.
        result.setStreamSegmentOffset(random.nextInt(1000));
        return result;
    }

    @Override
    protected OperationType getExpectedOperationType() {
        return OperationType.Deletion;
    }
}
