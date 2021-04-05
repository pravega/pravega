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

import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.util.Random;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for all classes derived from the CheckpointOperationBase class.
 */
public abstract class CheckpointOperationTests extends OperationTestsBase<CheckpointOperationBase> {

    public static class MetadataCheckpointOperationTests extends CheckpointOperationTests {
        @Override
        protected CheckpointOperationBase createOperation(Random random) {
            return new MetadataCheckpointOperation();
        }
    }

    public static class StorageMetadataCheckpointOperationTests extends CheckpointOperationTests {
        @Override
        protected CheckpointOperationBase createOperation(Random random) {
            return new StorageMetadataCheckpointOperation();
        }
    }

    @Override
    protected boolean isPreSerializationConfigRequired(CheckpointOperationBase operation) {
        return operation.getContents() == null;
    }

    @Override
    protected void configurePreSerialization(CheckpointOperationBase operation, Random random) {
        if (operation.getContents() == null) {
            byte[] data = new byte[10245];
            random.nextBytes(data);
            operation.setContents(new ByteArraySegment(data));
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }

    @Test
    public void testSetClearContents() {
        val rnd = new Random(0);
        val op = createOperation(rnd);
        byte[] data = new byte[10245];
        rnd.nextBytes(data);
        op.setContents(new ByteArraySegment(data));
        AssertExtensions.assertThrows(
                "setContents() allowed double-setting the contents.",
                () -> op.setContents(new ByteArraySegment(data)),
                ex -> ex instanceof IllegalStateException);
        Assert.assertNotNull("setContents() did not set contents.", op.getContents());
        op.clearContents();
        Assert.assertNull("clearContents() did not clear the contents.", op.getContents());
    }
}
