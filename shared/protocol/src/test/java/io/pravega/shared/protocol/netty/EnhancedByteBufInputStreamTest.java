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
package io.pravega.shared.protocol.netty;

import io.netty.buffer.Unpooled;
import io.pravega.test.common.AssertExtensions;
import java.util.Random;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class EnhancedByteBufInputStreamTest {
    /**
     * Tests the {@link EnhancedByteBufInputStream#readFully(int)} method.
     */
    @Test
    public void testReadFully() throws Exception {
        val rnd = new Random(0);
        val data = new byte[1000];
        rnd.nextBytes(data);
        val buf = Unpooled.wrappedBuffer(data);

        @Cleanup
        val stream = new EnhancedByteBufInputStream(buf);
        AssertExtensions.assertThrows(
                "Negative length.",
                () -> stream.readFully(-1),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "Overflow length.",
                () -> stream.readFully(data.length + 1),
                ex -> ex instanceof IllegalArgumentException);

        int readLength = 0;
        int index = 0;
        while (index < data.length) {
            if (index + readLength > data.length) {
                // Verify overflow (again).
                final int readLengthCopy = readLength;
                AssertExtensions.assertThrows(
                        "Overflow length for index " + index + " and length " + readLength,
                        () -> stream.readFully(readLengthCopy),
                        ex -> ex instanceof IllegalArgumentException);
            }

            // Verify readFully returns what we expect.
            readLength = Math.min(readLength, data.length - index);
            @Cleanup("release")
            val readBuf = stream.readFully(readLength).retain();
            Assert.assertSame(data, readBuf.array());
            Assert.assertEquals(index, readBuf.arrayOffset());
            Assert.assertEquals(readLength, readBuf.readableBytes());

            index += readLength;
            readLength++;
        }

        // Make sure that retain/release also applied to the underlying buffer.
        Assert.assertEquals(1, buf.refCnt());

        // Verify released buffer.
        stream.reset();
        val read2 = stream.readFully(10).retain();
        buf.release();
        Assert.assertEquals(1, read2.refCnt());
        read2.release();
        Assert.assertEquals(0, buf.refCnt());
    }
}
