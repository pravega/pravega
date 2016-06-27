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

package com.emc.logservice.common;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Unit tests for StreamHelpers class.
 */
public class StreamHelpersTests {
    /**
     * Tests the readAll method.
     *
     * @throws IOException
     */
    @Test
    public void testReadAll() throws IOException {
        final int itemCount = 100;
        final byte[] buffer = new byte[itemCount];
        final int readStartOffset = 5;
        final int readLength = itemCount - readStartOffset - 5;
        for (int i = 0; i < itemCount; i++) {
            buffer[i] = (byte) i;
        }

        TestInputStream is = new TestInputStream(buffer);

        byte[] readResult = new byte[itemCount + 10];
        int readBytes = StreamHelpers.readAll(is, readResult, readStartOffset, readLength);
        Assert.assertEquals("Unexpected number of bytes read.", readLength, readBytes);
        for (int i = 0; i < readResult.length; i++) {
            if (i < readStartOffset || i >= readStartOffset + readLength) {
                Assert.assertEquals("readAll wrote data at wrong offsset " + i, 0, readResult[i]);
            } else {
                int originalOffset = i - readStartOffset;
                Assert.assertEquals("unexpected value at target index " + i, buffer[originalOffset], readResult[i]);
            }
        }
    }

    private class TestInputStream extends InputStream {
        private final byte[] buffer;
        private int pos;
        private boolean pause;

        public TestInputStream(byte[] buffer) {
            this.buffer = buffer;
            this.pos = 0;
            this.pause = false;
        }

        @Override
        public int read() throws IOException {
            if (pause) {
                pause = false;
                return -1;
            } else if (pos >= buffer.length) {
                return -1;
            } else {
                pause = true;
                return buffer[pos++];
            }
        }
    }
}
