/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.logs.operations;

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
        } else if (expected instanceof MergeTransactionOperation) {
            assertSame(message, (MergeTransactionOperation) expected, (MergeTransactionOperation) actual);
        } else {
            Assert.fail(message + " No comparison implemented for operation " + expected);
        }
    }

    private void assertSame(String message, StreamSegmentSealOperation expected, StreamSegmentSealOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentLength.", expected.getStreamSegmentOffset(), actual.getStreamSegmentOffset());
    }

    private void assertSame(String message, StreamSegmentAppendOperation expected, StreamSegmentAppendOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentOffset.", expected.getStreamSegmentOffset(), actual.getStreamSegmentOffset());
        Assert.assertEquals(message + " Unexpected AppendContext.ClientId", expected.getAppendContext().getClientId(), actual.getAppendContext().getClientId());
        Assert.assertEquals(message + " Unexpected AppendContext.EventNumber", expected.getAppendContext().getEventNumber(), actual.getAppendContext().getEventNumber());
        Assert.assertArrayEquals(message + " Unexpected Data. ", expected.getData(), actual.getData());
    }

    private void assertSame(String message, StreamSegmentAppendOperation expected, CachedStreamSegmentAppendOperation cachedActual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentOffset.", expected.getStreamSegmentOffset(), cachedActual.getStreamSegmentOffset());
        Assert.assertEquals(message + " Unexpected Length.", expected.getData().length, cachedActual.getLength());
    }

    private void assertSame(String message, CachedStreamSegmentAppendOperation expected, CachedStreamSegmentAppendOperation actual) {
        Assert.assertEquals(message + " Unexpected Length.", expected.getLength(), actual.getLength());
    }

    private void assertSame(String message, MergeTransactionOperation expected, MergeTransactionOperation actual) {
        Assert.assertEquals(message + " Unexpected TransactionStreamSegmentId.", expected.getTransactionSegmentId(), actual.getTransactionSegmentId());
        Assert.assertEquals(message + " Unexpected TransactionStreamSegmentLength.", expected.getLength(), actual.getLength());
        Assert.assertEquals(message + " Unexpected TargetStreamSegmentOffset.", expected.getStreamSegmentOffset(), actual.getStreamSegmentOffset());
    }

    private void assertSame(String message, MetadataOperation expected, MetadataOperation actual) {
        if (expected instanceof MetadataCheckpointOperation) {
            assertSame(message, (MetadataCheckpointOperation) expected, (MetadataCheckpointOperation) actual);
        } else if (expected instanceof StreamSegmentMapOperation) {
            assertSame(message, (StreamSegmentMapOperation) expected, (StreamSegmentMapOperation) actual);
        } else if (expected instanceof TransactionMapOperation) {
            assertSame(message, (TransactionMapOperation) expected, (TransactionMapOperation) actual);
        } else {
            Assert.fail(message + " No comparison implemented for operation " + expected);
        }
    }

    private void assertSame(String message, StreamSegmentMapOperation expected, StreamSegmentMapOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentId.", expected.getStreamSegmentId(), actual.getStreamSegmentId());
        Assert.assertEquals(message + " Unexpected StreamSegmentLength.", expected.getLength(), actual.getLength());
        Assert.assertEquals(message + " Unexpected StreamSegmentName.", expected.getStreamSegmentName(), actual.getStreamSegmentName());
    }

    private void assertSame(String message, TransactionMapOperation expected, TransactionMapOperation actual) {
        Assert.assertEquals(message + " Unexpected TransactionStreamSegmentId.", expected.getStreamSegmentId(), actual.getStreamSegmentId());
        Assert.assertEquals(message + " Unexpected TransactionStreamSegmentName.", expected.getStreamSegmentName(), actual.getStreamSegmentName());
        Assert.assertEquals(message + " Unexpected ParentStreamSegmentId.", expected.getParentStreamSegmentId(), actual.getParentStreamSegmentId());
    }

    private void assertSame(String message, MetadataCheckpointOperation expected, MetadataCheckpointOperation actual) {
        Assert.assertEquals(message + " Lengths mismatch.", expected.getContents().getLength(), actual.getContents().getLength());
        for (int j = 0; j < expected.getContents().getLength(); j++) {
            if (expected.getContents().get(j) != actual.getContents().get(j)) {
                Assert.fail(String.format("%s Contents differ at index %d.", message, j));
            }
        }
    }
}
