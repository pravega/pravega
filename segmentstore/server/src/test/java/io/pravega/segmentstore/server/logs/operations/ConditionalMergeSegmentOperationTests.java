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

import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;

import java.util.Random;

/**
 * Unit tests for MergeSegmentOperation class when AttributeUpdates are used.
 */
public class ConditionalMergeSegmentOperationTests extends MergeSegmentOperationTests {
    @Override
    protected MergeSegmentOperation createOperation(Random random) {
        AttributeUpdateCollection attributeUpdates = AttributeUpdateCollection.from(
                new AttributeUpdate(AttributeId.randomUUID(), AttributeUpdateType.ReplaceIfEquals, 0, Long.MIN_VALUE),
                new AttributeUpdate(AttributeId.randomUUID(), AttributeUpdateType.ReplaceIfEquals, 0, Long.MIN_VALUE));
        return new MergeSegmentOperation(random.nextLong(), random.nextLong(), attributeUpdates);
    }
}

