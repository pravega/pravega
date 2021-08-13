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
package io.pravega.client.connection.impl;

import io.netty.buffer.ByteBuf;
import io.pravega.test.common.AssertExtensions;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IoBufferTest {

    private static class TestInputStream extends InputStream {
        int numAvailable = 0;
        boolean atEof = false;

        @Override
        public int available() throws IOException {
            return numAvailable;
        }
        
        @Override
        public int read() throws IOException {
            if (atEof) {
                return -1;
            }
            return 0;
        }
    }
    
    @Test
    public void testShort() throws IOException {
        TestInputStream in = new TestInputStream();
        IoBuffer buffer = new IoBuffer();
        in.numAvailable = 10;
        ByteBuf buf = buffer.getBuffOfSize(in, 4);
        assertEquals(buf.readableBytes(), 4);
        in.numAvailable = 6;
        buf = buffer.getBuffOfSize(in, 4);
        assertEquals(buf.readableBytes(), 4);
        in.numAvailable = 2;
        in.atEof = true;
        AssertExtensions.assertThrows(EOFException.class, () -> buffer.getBuffOfSize(in, 4));
    }
    
    @Test
    public void testMedium() throws IOException {
        TestInputStream in = new TestInputStream();
        IoBuffer buffer = new IoBuffer();
        in.numAvailable = 10;
        ByteBuf buf = buffer.getBuffOfSize(in, 4);
        assertEquals(buf.readableBytes(), 4);
        in.numAvailable = 6;
        buf = buffer.getBuffOfSize(in, 10);
        assertEquals(buf.readableBytes(), 10);
        in.numAvailable = 2;
        in.atEof = true;
        AssertExtensions.assertThrows(EOFException.class, () -> buffer.getBuffOfSize(in, 4));
    }
    
    @Test
    public void testLong() throws IOException {
        TestInputStream in = new TestInputStream();
        IoBuffer buffer = new IoBuffer();
        in.numAvailable = 10;
        ByteBuf buf = buffer.getBuffOfSize(in, 24);
        assertEquals(buf.readableBytes(), 24);
        in.numAvailable = 10;
        buf = buffer.getBuffOfSize(in, 24);
        assertEquals(buf.readableBytes(), 24);
        in.numAvailable = 0;
        in.atEof = true;
        AssertExtensions.assertThrows(EOFException.class, () -> buffer.getBuffOfSize(in, 4));  
    }
 
    @Test
    public void testFromBuffer() throws IOException {
        TestInputStream in = new TestInputStream();
        IoBuffer buffer = new IoBuffer();
        in.numAvailable = 10;
        ByteBuf buf1 = buffer.getBuffOfSize(in, 2);
        ByteBuf buf2 = buffer.getBuffOfSize(in, 2);
        ByteBuf buf3 = buffer.getBuffOfSize(in, 2);
        ByteBuf buf4 = buffer.getBuffOfSize(in, 2);
        ByteBuf buf5 = buffer.getBuffOfSize(in, 2);
        assertEquals(buf1.readableBytes(), 2);
        assertEquals(buf2.readableBytes(), 2);
        assertEquals(buf3.readableBytes(), 2);
        assertEquals(buf4.readableBytes(), 2);
        assertEquals(buf5.readableBytes(), 2);
        buf1.skipBytes(2);
        assertEquals(buf1.readableBytes(), 0);
        assertEquals(buf2.readableBytes(), 2);
        assertEquals(buf3.readableBytes(), 2);
        assertEquals(buf4.readableBytes(), 2);
        assertEquals(buf5.readableBytes(), 2);
        buf5.skipBytes(2);
        assertEquals(buf1.readableBytes(), 0);
        assertEquals(buf2.readableBytes(), 2);
        assertEquals(buf3.readableBytes(), 2);
        assertEquals(buf4.readableBytes(), 2);
        assertEquals(buf5.readableBytes(), 0);
        in.numAvailable = 0;
        in.atEof = true;
        AssertExtensions.assertThrows(EOFException.class, () -> buffer.getBuffOfSize(in, 4));  
    }
    
}
