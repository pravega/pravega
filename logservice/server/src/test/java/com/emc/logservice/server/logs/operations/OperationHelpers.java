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

package com.emc.logservice.server.logs.operations;

import org.junit.Assert;

/**
 * Helper methods for Log Operation testing.
 */
public class OperationHelpers {

    /**
     * Checks if the given operations are the same.
     *
     * @param expected
     * @param actual
     */
    public static void assertEquals(Operation expected, Operation actual) {
        assertEquals("", expected, actual);
    }

    /**
     * Checks if the given operations are the same.
     *
     * @param message
     * @param expected
     * @param actual
     */
    public static void assertEquals(String message, Operation expected, Operation actual) {
        Assert.assertEquals(message + " Unexpected Java class.", expected.getClass(), actual.getClass());
        Assert.assertEquals(" Unexpected Sequence Number", expected.getSequenceNumber(), actual.getSequenceNumber());

        if (expected instanceof StorageOperation) {
            assertEquals(message, (StorageOperation) expected, (StorageOperation) actual);
        } else if (expected instanceof MetadataOperation) {
            assertEquals(message, (MetadataOperation) expected, (MetadataOperation) actual);
        } else {
            Assert.fail(message + " No comparison implemented for operation " + expected);
        }
    }

    /**
     * Checks if the given StorageOperations are the same.
     *
     * @param message
     * @param expected
     * @param actual
     */
    public static void assertEquals(String message, StorageOperation expected, StorageOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentId.", expected.getStreamSegmentId(), actual.getStreamSegmentId());
        if (expected instanceof StreamSegmentSealOperation) {
            assertEquals(message, (StreamSegmentSealOperation) expected, (StreamSegmentSealOperation) actual);
        } else if (expected instanceof StreamSegmentAppendOperation) {
            assertEquals(message, (StreamSegmentAppendOperation) expected, (StreamSegmentAppendOperation) actual);
        } else if (expected instanceof MergeBatchOperation) {
            assertEquals(message, (MergeBatchOperation) expected, (MergeBatchOperation) actual);
        } else {
            Assert.fail(message + " No comparison implemented for operation " + expected);
        }
    }

    /**
     * Checks if the given StreamSegmentSealOperations are the same.
     *
     * @param message
     * @param expected
     * @param actual
     */
    public static void assertEquals(String message, StreamSegmentSealOperation expected, StreamSegmentSealOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentLength.", expected.getStreamSegmentLength(), actual.getStreamSegmentLength());
    }

    /**
     * Checks if the given StreamSegmentAppendOperations are the same.
     *
     * @param message
     * @param expected
     * @param actual
     */
    public static void assertEquals(String message, StreamSegmentAppendOperation expected, StreamSegmentAppendOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentOffset.", expected.getStreamSegmentOffset(), actual.getStreamSegmentOffset());
        Assert.assertEquals(message + " Unexpected AppendContext.ClientId", expected.getAppendContext().getClientId(), actual.getAppendContext().getClientId());
<<<<<<< HEAD
        Assert.assertEquals(message + " Unexpected AppendContext.ClientOffset", expected.getAppendContext().getEventNumber(), actual.getAppendContext().getEventNumber());
=======
        Assert.assertEquals(message + " Unexpected AppendContext.EventNumber", expected.getAppendContext().getEventNumber(), actual.getAppendContext().getEventNumber());
>>>>>>> 07d786c90fe5df25e57dcb9367cd44de190e89fe
        Assert.assertArrayEquals(message + " Unexpected Data. ", expected.getData(), actual.getData());
    }

    /**
     * Checks if the given MergeBatchOperations are the same.
     *
     * @param message
     * @param expected
     * @param actual
     */
    public static void assertEquals(String message, MergeBatchOperation expected, MergeBatchOperation actual) {
        Assert.assertEquals(message + " Unexpected BatchStreamSegmentId.", expected.getBatchStreamSegmentId(), actual.getBatchStreamSegmentId());
        Assert.assertEquals(message + " Unexpected BatchStreamSegmentLength.", expected.getBatchStreamSegmentLength(), actual.getBatchStreamSegmentLength());
        Assert.assertEquals(message + " Unexpected TargetStreamSegmentOffset.", expected.getTargetStreamSegmentOffset(), actual.getTargetStreamSegmentOffset());
    }

    /**
     * Checks if the given MetadataOperations are the same.
     *
     * @param message
     * @param expected
     * @param actual
     */
    public static void assertEquals(String message, MetadataOperation expected, MetadataOperation actual) {
        if (expected instanceof MetadataCheckpointOperation) {
            assertEquals(message, (MetadataCheckpointOperation) expected, (MetadataCheckpointOperation) actual);
        } else if (expected instanceof StreamSegmentMapOperation) {
            assertEquals(message, (StreamSegmentMapOperation) expected, (StreamSegmentMapOperation) actual);
        } else if (expected instanceof BatchMapOperation) {
            assertEquals(message, (BatchMapOperation) expected, (BatchMapOperation) actual);
        } else {
            Assert.fail(message + " No comparison implemented for operation " + expected);
        }
    }

    /**
     * Checks if the given StreamSegmentMapOperations are the same.
     *
     * @param message
     * @param expected
     * @param actual
     */
    public static void assertEquals(String message, StreamSegmentMapOperation expected, StreamSegmentMapOperation actual) {
        Assert.assertEquals(message + " Unexpected StreamSegmentId.", expected.getStreamSegmentId(), actual.getStreamSegmentId());
        Assert.assertEquals(message + " Unexpected StreamSegmentLength.", expected.getLength(), actual.getLength());
        Assert.assertEquals(message + " Unexpected StreamSegmentName.", expected.getStreamSegmentName(), actual.getStreamSegmentName());
    }

    /**
     * Checks if the given BatchMapOperations are the same.
     *
     * @param message
     * @param expected
     * @param actual
     */
    public static void assertEquals(String message, BatchMapOperation expected, BatchMapOperation actual) {
        Assert.assertEquals(message + " Unexpected BatchStreamSegmentId.", expected.getStreamSegmentId(), actual.getStreamSegmentId());
        Assert.assertEquals(message + " Unexpected BatchStreamSegmentName.", expected.getStreamSegmentName(), actual.getStreamSegmentName());
        Assert.assertEquals(message + " Unexpected ParentStreamSegmentId.", expected.getParentStreamSegmentId(), actual.getParentStreamSegmentId());
    }

    /**
     * Checks if the given MetadataCheckpointOperations are the same.
     *
     * @param message
     * @param expected
     * @param actual
     */
    public static void assertEquals(String message, MetadataCheckpointOperation expected, MetadataCheckpointOperation actual) {
        Assert.assertEquals(message + " Lengths mismatch.", expected.getContents().getLength(), actual.getContents().getLength());
        for (int j = 0; j < expected.getContents().getLength(); j++) {
            if (expected.getContents().get(j) != actual.getContents().get(j)) {
                Assert.fail(String.format("%s Contents differ at index %d.", message, j));
            }
        }
    }
}
