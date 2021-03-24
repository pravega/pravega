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
package io.pravega.client.tables;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.tables.impl.IteratorStateImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link IteratorState} class.
 */
public class IteratorStateTests {
    @Test
    public void testEmpty() {
        Assert.assertTrue(IteratorStateImpl.EMPTY.isEmpty());
        Assert.assertEquals(0, IteratorStateImpl.EMPTY.toBytes().remaining());
        Assert.assertSame(IteratorStateImpl.EMPTY, IteratorState.fromBytes(null));
        Assert.assertSame(IteratorStateImpl.EMPTY, IteratorStateImpl.fromBytes((ByteBuf) null));
    }

    @Test
    public void testFromBytes() {
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[123]);
        IteratorState s = IteratorStateImpl.fromBytes(buf);
        Assert.assertEquals(buf, Unpooled.wrappedBuffer(s.toBytes()));
        Assert.assertEquals(s.toBytes(), IteratorState.fromBytes(s.toBytes()).toBytes());
        Assert.assertEquals(s.toBytes(), IteratorStateImpl.fromBytes(Unpooled.wrappedBuffer(s.toBytes())).toBytes());
    }

    @Test
    public void testCopyOf() {
        Assert.assertSame(IteratorStateImpl.EMPTY, IteratorStateImpl.copyOf(IteratorStateImpl.EMPTY));
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[123]);
        IteratorState s = IteratorStateImpl.fromBytes(buf);
        IteratorState s2 = IteratorStateImpl.copyOf(s);
        Assert.assertEquals(s.toBytes(), s2.toBytes());

        // This way we verify that the two are really pointing to different buffers.
        buf.release();
        Assert.assertEquals(s2.toBytes(), IteratorState.fromBytes(s2.toBytes()).toBytes());
    }
}
