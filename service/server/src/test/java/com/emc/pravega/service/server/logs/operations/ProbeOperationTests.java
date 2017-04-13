/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.shared.testcommon.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the ProbeOperation class.
 */
public class ProbeOperationTests {

    /**
     * Tests the ability of the ProbeOperation to reject all serialization/deserialization requests.
     */
    @Test
    public void testSerialization() {
        ProbeOperation op = new ProbeOperation();
        Assert.assertFalse("Unexpected value from canSerialize().", op.canSerialize());
        op.setSequenceNumber(1);
        AssertExtensions.assertThrows(
                "serialize() did not fail with the expected exception.",
                () -> op.serialize(new DataOutputStream(new ByteArrayOutputStream())),
                ex -> ex instanceof UnsupportedOperationException);

        // Even though there is no deserialization constructor, we need to ensure that the deserializeContent method
        // does not work.
        AssertExtensions.assertThrows(
                "deserializeContent() did not fail with the expected exception.",
                () -> op.deserializeContent(new DataInputStream(new ByteArrayInputStream(new byte[100]))),
                ex -> ex instanceof UnsupportedOperationException);
    }
}
