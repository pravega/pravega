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

package com.emc.pravega.common.io;

import com.emc.pravega.common.io.FixedByteArrayOutputStream;
import com.emc.pravega.testcommon.AssertExtensions;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Unit tests for FixedByteArrayOutputStream class.
 */
public class FixedByteArrayOutputStreamTests {
    /**
     * Tests that FixedByteArrayOutputStream works as advertised.
     *
     * @throws IOException
     */
    @Test
    public void testWrite() throws IOException {
        final int arraySize = 100;
        final int startOffset = 10;
        final int length = 50;
        final int secondHalfFillValue = 123;

        byte[] buffer = new byte[arraySize];
        Arrays.fill(buffer, (byte) 0);

        try (FixedByteArrayOutputStream os = new FixedByteArrayOutputStream(buffer, startOffset, length)) {
            // First half: write 1 by 1
            for (int i = 0; i < length / 2; i++) {
                os.write(i);
            }

            // Second half: write an entire array.
            byte[] secondHalf = new byte[length / 2];
            Arrays.fill(secondHalf, (byte) secondHalfFillValue);
            os.write(secondHalf);

            AssertExtensions.assertThrows(
                    "write(byte) allowed writing beyond the end of the stream.",
                    () -> os.write(255),
                    ex -> ex instanceof IOException);

            AssertExtensions.assertThrows(
                    "write(byte[]) allowed writing beyond the end of the stream.",
                    () -> os.write(new byte[10]),
                    ex -> ex instanceof IOException);
        }

        for (int i = 0; i < buffer.length; i++) {
            if (i < startOffset || i >= startOffset + length) {
                Assert.assertEquals("write() wrote data outside of its allocated bounds.", 0, buffer[i]);
            } else {
                int streamOffset = i - startOffset;
                if (streamOffset < length / 2) {
                    Assert.assertEquals("Unexpected value for stream index " + streamOffset, streamOffset, buffer[i]);
                } else {
                    Assert.assertEquals("Unexpected value for stream index " + streamOffset, secondHalfFillValue, buffer[i]);
                }
            }
        }
    }
}
