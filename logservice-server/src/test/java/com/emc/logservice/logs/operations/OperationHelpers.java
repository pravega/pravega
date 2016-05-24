package com.emc.logservice.logs.operations;

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
        Assert.assertEquals("Unexpected Java class.", expected.getClass(), actual.getClass());
        Assert.assertEquals("Unexpected Sequence Number", expected.getSequenceNumber(), actual.getSequenceNumber());

        if (expected instanceof StorageOperation) {
            assertEquals((StorageOperation) expected, (StorageOperation) actual);
        }
        else if (expected instanceof MetadataOperation) {
            assertEquals((MetadataOperation) expected, (MetadataOperation) actual);
        }
        else {
            Assert.fail("No comparison implemented for operation " + expected);
        }
    }

    /**
     * Checks if the given StorageOperations are the same.
     *
     * @param expected
     * @param actual
     */
    public static void assertEquals(StorageOperation expected, StorageOperation actual) {
        Assert.assertEquals("Unexpected StreamSegmentId.", expected.getStreamSegmentId(), actual.getStreamSegmentId());
        if (expected instanceof StreamSegmentSealOperation) {
            assertEquals((StreamSegmentSealOperation) expected, (StreamSegmentSealOperation) actual);
        }
        else if (expected instanceof StreamSegmentAppendOperation) {
            assertEquals((StreamSegmentAppendOperation) expected, (StreamSegmentAppendOperation) actual);
        }
        else if (expected instanceof MergeBatchOperation) {
            assertEquals((MergeBatchOperation) expected, (MergeBatchOperation) actual);
        }
        else {
            Assert.fail("No comparison implemented for operation " + expected);
        }
    }

    /**
     * Checks if the given StreamSegmentSealOperations are the same.
     *
     * @param expected
     * @param actual
     */
    public static void assertEquals(StreamSegmentSealOperation expected, StreamSegmentSealOperation actual) {
        Assert.assertEquals("Unexpected StreamSegmentLength.", expected.getStreamSegmentLength(), actual.getStreamSegmentLength());
    }

    /**
     * Checks if the given StreamSegmentAppendOperations are the same.
     *
     * @param expected
     * @param actual
     */
    public static void assertEquals(StreamSegmentAppendOperation expected, StreamSegmentAppendOperation actual) {
        Assert.assertEquals("Unexpected StreamSegmentOffset.", expected.getStreamSegmentOffset(), actual.getStreamSegmentOffset());
        Assert.assertArrayEquals("Unexpected Data. ", expected.getData(), actual.getData());
    }

    /**
     * Checks if the given MergeBatchOperations are the same.
     *
     * @param expected
     * @param actual
     */
    public static void assertEquals(MergeBatchOperation expected, MergeBatchOperation actual) {
        Assert.assertEquals("Unexpected BatchStreamSegmentId.", expected.getBatchStreamSegmentId(), actual.getBatchStreamSegmentId());
        Assert.assertEquals("Unexpected BatchStreamSegmentLength.", expected.getBatchStreamSegmentLength(), actual.getBatchStreamSegmentLength());
        Assert.assertEquals("Unexpected TargetStreamSegmentOffset.", expected.getTargetStreamSegmentOffset(), actual.getTargetStreamSegmentOffset());
    }

    /**
     * Checks if the given MetadataOperations are the same.
     *
     * @param expected
     * @param actual
     */
    public static void assertEquals(MetadataOperation expected, MetadataOperation actual) {
        if (expected instanceof MetadataPersistedOperation) {
            // nothing special here
            return;
        }
        else if (expected instanceof StreamSegmentMapOperation) {
            assertEquals((StreamSegmentMapOperation) expected, (StreamSegmentMapOperation) actual);
        }
        else if (expected instanceof BatchMapOperation) {
            assertEquals((BatchMapOperation) expected, (BatchMapOperation) actual);
        }
        else {
            Assert.fail("No comparison implemented for operation " + expected);
        }
    }

    /**
     * Checks if the given StreamSegmentMapOperations are the same.
     *
     * @param expected
     * @param actual
     */
    public static void assertEquals(StreamSegmentMapOperation expected, StreamSegmentMapOperation actual) {
        Assert.assertEquals("Unexpected StreamSegmentId.", expected.getStreamSegmentId(), actual.getStreamSegmentId());
        Assert.assertEquals("Unexpected StreamSegmentLength.", expected.getStreamSegmentLength(), actual.getStreamSegmentLength());
        Assert.assertEquals("Unexpected StreamSegmentName.", expected.getStreamSegmentName(), actual.getStreamSegmentName());
    }

    /**
     * Checks if the given BatchMapOperations are the same.
     *
     * @param expected
     * @param actual
     */
    public static void assertEquals(BatchMapOperation expected, BatchMapOperation actual) {
        Assert.assertEquals("Unexpected BatchStreamSegmentId.", expected.getBatchStreamSegmentId(), actual.getBatchStreamSegmentId());
        Assert.assertEquals("Unexpected BatchStreamSegmentName.", expected.getBatchStreamSegmentName(), actual.getBatchStreamSegmentName());
        Assert.assertEquals("Unexpected ParentStreamSegmentId.", expected.getParentStreamSegmentId(), actual.getParentStreamSegmentId());
    }
}
