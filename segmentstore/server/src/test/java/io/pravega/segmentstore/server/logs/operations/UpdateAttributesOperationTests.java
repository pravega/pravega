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
import lombok.val;

/**
 * Unit tests for the UpdateAttributesOperation class.
 */
public class UpdateAttributesOperationTests extends OperationTestsBase<UpdateAttributesOperation> {
    @Override
    protected UpdateAttributesOperation createOperation(Random random) {
        val attributes = StreamSegmentAppendOperationTests.createAttributes();
        return new UpdateAttributesOperation(random.nextLong(), attributes);
    }
}
