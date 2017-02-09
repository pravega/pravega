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

import com.emc.pravega.testcommon.AssertExtensions;
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
