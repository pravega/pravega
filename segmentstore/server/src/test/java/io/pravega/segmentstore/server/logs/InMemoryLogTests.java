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

import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link InMemoryLog} class.
 */
public class InMemoryLogTests {
    @Test
    public void testSequenceNumbers() {
        @Cleanup
        val log = new InMemoryLog();

        AssertExtensions.assertThrows(
                "An Operation with no Sequence Number was accepted.",
                () -> log.add(new MetadataCheckpointOperation()),
                ex -> ex instanceof InMemoryLog.OutOfOrderOperationException);

        val op1 = new MetadataCheckpointOperation();
        op1.setSequenceNumber(1);
        log.add(op1);

        val op2 = new MetadataCheckpointOperation();
        op2.setSequenceNumber(1);
        AssertExtensions.assertThrows(
                "An Operation with out-of-order sequence number was accepted.",
                () -> log.add(op2),
                ex -> ex instanceof InMemoryLog.OutOfOrderOperationException);

        val items = log.poll(10);
        Assert.assertTrue(items.size() == 1 && items.poll().equals(op1));
    }
}
