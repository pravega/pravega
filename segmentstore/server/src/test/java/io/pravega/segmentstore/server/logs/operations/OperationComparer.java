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

import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.test.common.AssertExtensions;
import java.util.Collection;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;

/**
 * Helps compare Log Operations between them.
 */
public class OperationComparer {
    /**
     * Most commonly used OperationComparer: not enforcing reference equality and no cache provided.
     */
    public static final OperationComparer DEFAULT = new OperationComparer(false);
    private final boolean enforceReferenceEquality;

    /**
     * Creates a new instance of the OperationComparer class.
     *
     * @param enforceReferenceEquality If true, every call to assertEquals will check the actual object reference; if
     *                                 false, it checks object properties. This parameter does not apply when comparing
     *                                 StreamSegmentAppendOperation with CachedStreamSegmentAppendOperation.
     */
    public OperationComparer(boolean enforceReferenceEquality) {
        this.enforceReferenceEquality = enforceReferenceEquality;
    }

    /**
     * Checks if the given operations are the same.
     *
     * @param expected The expected Operation.
     * @param actual   The actual operation.
     */
    public void assertEquals(Operation expected, Operation actual) {
        assertEquals("", expected, actual);
    }

    /**
     * Checks if the given operations are the same.
     *
     * @param message  The message ot include in the assert message.
     * @param expected The expected Operation.
     * @param actual   The actual operation.
     */
    public void assertEquals(String message, Operation expected, Operation actual) {
        if (this.enforceReferenceEquality) {
            if (expected instanceof StreamSegmentAppendOperation && actual instanceof CachedStreamSegmentAppendOperation) {
                // StreamSegmentAppendOperation and CachedStreamSegmentAppendOperation have a special relationship.
                assertSame(message, (StreamSegmentAppendOperation) expected, (CachedStreamSegmentAppendOperation) actual);
            } else {
                Assert.assertEquals(message + " Unexpected Operation.", expected, actual);
            }
        } else {
            assertSame(message, expected, actual);
        }
    }

    private void assertSame(String message, Operation expected, Operation actual) {
        if (expected instanceof StreamSegmentAppendOperation) {
            // StreamSegmentAppendOperation and CachedStreamSegmentAppendOperation have a special relationship.
            Assert.assertTrue(message + " Unexpected Java class.", actual instanceof StreamSegmentAppendOperation || actual instanceof CachedStreamSegmentAppendOperation);
        } else {
            Assert.assertEquals(message + " Unexpected Java class.", expected.getClass(), actual.getClass());
        }

        Assert.assertEquals(" Unexpected Sequence Number", expected.getSequenceNumber(), actual.getSequenceNumber());

        if (expected instanceof StorageOperation) {
            assertSame(message, (StorageOperation) expected, (StorageOperation) actual);
        } else if (expected instanceof MetadataOperation) {
            assertSame(message, (MetadataOperation) expected, (MetadataOperation) actual);
        } else {
            Assert.fail(message + " No comparison implemented for operation " + expected);
        }
    }

    private void assertSame(String message, StorageOperation expected, StorageOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentId.", expected.getStreamSegmentId(), actual.getStreamSegmentId());
        if (expected instanceof StreamSegmentSealOperation) {
            assertSame(message, (StreamSegmentSealOperation) expected, (StreamSegmentSealOperation) actual);
        } else if (expected instanceof StreamSegmentAppendOperation) {
            if (actual instanceof StreamSegmentAppendOperation) {
                assertSame(message, (StreamSegmentAppendOperation) expected, (StreamSegmentAppendOperation) actual);
            } else if (actual instanceof CachedStreamSegmentAppendOperation) {
                // StreamSegmentAppendOperation and CachedStreamSegmentAppendOperation have a special relationship.
                assertSame(message, (StreamSegmentAppendOperation) expected, (CachedStreamSegmentAppendOperation) actual);
            } else {
                Assert.fail(message + " No comparison implemented for operations " + expected + " and " + actual);
            }
        } else if (expected instanceof CachedStreamSegmentAppendOperation) {
            assertSame(message, (CachedStreamSegmentAppendOperation) expected, (CachedStreamSegmentAppendOperation) actual);
        } else if (expected instanceof MergeSegmentOperation) {
            assertSame(message, (MergeSegmentOperation) expected, (MergeSegmentOperation) actual);
        } else if (expected instanceof StreamSegmentTruncateOperation) {
            assertSame(message, (StreamSegmentTruncateOperation) expected, (StreamSegmentTruncateOperation) actual);
        } else if (expected instanceof DeleteSegmentOperation) {
            assertSame(message, (DeleteSegmentOperation) expected, (DeleteSegmentOperation) actual);
        } else {
            Assert.fail(message + " No comparison implemented for operation " + expected);
        }
    }

    private void assertSame(String message, StreamSegmentSealOperation expected, StreamSegmentSealOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentOffset.", expected.getStreamSegmentOffset(), actual.getStreamSegmentOffset());
    }

    private void assertSame(String message, StreamSegmentAppendOperation expected, StreamSegmentAppendOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentOffset.", expected.getStreamSegmentOffset(), actual.getStreamSegmentOffset());
        Assert.assertArrayEquals(message + " Unexpected Data. ", expected.getData().getCopy(), actual.getData().getCopy());
        assertSame(message + " Unexpected attributes:", expected.getAttributeUpdates(), actual.getAttributeUpdates());
    }

    private void assertSame(String message, StreamSegmentAppendOperation expected, CachedStreamSegmentAppendOperation cachedActual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentOffset.", expected.getStreamSegmentOffset(), cachedActual.getStreamSegmentOffset());
        Assert.assertEquals(message + " Unexpected Length.", expected.getData().getLength(), cachedActual.getLength());
        Assert.assertEquals(message + " Unexpected CacheLength.", expected.getCacheLength(), cachedActual.getCacheLength());
        assertSame(message + " Unexpected attributes:", expected.getAttributeUpdates(), cachedActual.getAttributeUpdates());
    }

    private void assertSame(String message, CachedStreamSegmentAppendOperation expected, CachedStreamSegmentAppendOperation actual) {
        Assert.assertEquals(message + " Unexpected Length.", expected.getLength(), actual.getLength());
        Assert.assertEquals(message + " Unexpected CacheLength.", expected.getCacheLength(), actual.getCacheLength());
        assertSame(message + " Unexpected attributes:", expected.getAttributeUpdates(), actual.getAttributeUpdates());
    }

    private void assertSame(String message, Collection<AttributeUpdate> expected, Collection<AttributeUpdate> actual) {
        if (expected == null || expected.size() == 0) {
            Assert.assertTrue(message + " Not expecting attributes.", actual == null || actual.size() == 0);
            return;
        } else {
            Assert.assertNotNull(message + " Expected attributes, but none found.", actual);
        }

        Assert.assertEquals(message + " Unexpected number of attributes.", expected.size(), actual.size());
        val expectedIndexed = expected.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));
        for (AttributeUpdate au : actual) {

            Assert.assertTrue(message + " Found extra AttributeUpdate: " + au, expectedIndexed.containsKey(au.getAttributeId()));
            long expectedValue = expectedIndexed.get(au.getAttributeId());
            Assert.assertEquals(message + " Unexpected value for AttributeUpdate: " + au, expectedValue, au.getValue());
        }
    }

    private void assertSame(String message, MergeSegmentOperation expected, MergeSegmentOperation actual) {
        Assert.assertEquals(message + " Unexpected TransactionStreamSegmentId.", expected.getSourceSegmentId(), actual.getSourceSegmentId());
        Assert.assertEquals(message + " Unexpected TransactionStreamSegmentLength.", expected.getLength(), actual.getLength());
        Assert.assertEquals(message + " Unexpected TargetStreamSegmentOffset.", expected.getStreamSegmentOffset(), actual.getStreamSegmentOffset());
    }

    private void assertSame(String message, StreamSegmentTruncateOperation expected, StreamSegmentTruncateOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentOffset.", expected.getStreamSegmentOffset(), actual.getStreamSegmentOffset());
    }

    private void assertSame(String message, MetadataOperation expected, MetadataOperation actual) {
        if (expected instanceof CheckpointOperationBase) {
            assertSame(message, (CheckpointOperationBase) expected, (CheckpointOperationBase) actual);
        } else if (expected instanceof StreamSegmentMapOperation) {
            assertSame(message, (StreamSegmentMapOperation) expected, (StreamSegmentMapOperation) actual);
        } else if (expected instanceof UpdateAttributesOperation) {
            assertSame(message, (UpdateAttributesOperation) expected, (UpdateAttributesOperation) actual);
        } else {
            Assert.fail(message + " No comparison implemented for operation " + expected);
        }
    }

    private void assertSame(String message, StreamSegmentMapOperation expected, StreamSegmentMapOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentId.", expected.getStreamSegmentId(), actual.getStreamSegmentId());
        Assert.assertEquals(message + " Unexpected StartOffset.", expected.getStartOffset(), actual.getStartOffset());
        Assert.assertEquals(message + " Unexpected StreamSegmentLength.", expected.getLength(), actual.getLength());
        Assert.assertEquals(message + " Unexpected StreamSegmentName.", expected.getStreamSegmentName(), actual.getStreamSegmentName());
        AssertExtensions.assertMapEquals(message + "Unexpected attributes.", expected.getAttributes(), actual.getAttributes());
        Assert.assertEquals("Unexpected Pinned.", expected.isPinned(), actual.isPinned());
    }

    private void assertSame(String message, UpdateAttributesOperation expected, UpdateAttributesOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentId.", expected.getStreamSegmentId(), actual.getStreamSegmentId());
        assertSame(message + "Unexpected attributes.", expected.getAttributeUpdates(), actual.getAttributeUpdates());
    }

    private void assertSame(String message, DeleteSegmentOperation expected, DeleteSegmentOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentId.", expected.getStreamSegmentId(), actual.getStreamSegmentId());
    }

    private void assertSame(String message, CheckpointOperationBase expected, CheckpointOperationBase actual) {
        Assert.assertEquals(message + " Lengths mismatch.", expected.getContents().getLength(), actual.getContents().getLength());
        for (int j = 0; j < expected.getContents().getLength(); j++) {
            if (expected.getContents().get(j) != actual.getContents().get(j)) {
                Assert.fail(String.format("%s Contents differ at index %d.", message, j));
            }
        }
    }
}
