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
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link HashTableIteratorItem} class.
 */
public class HashTableIteratorItemTests {
    @Test
    public void testEmpty() {
        Assert.assertTrue(HashTableIteratorItem.State.EMPTY.isEmpty());
        Assert.assertEquals(0, HashTableIteratorItem.State.EMPTY.toBytes().remaining());
        Assert.assertSame(HashTableIteratorItem.State.EMPTY, HashTableIteratorItem.State.fromBytes((ByteBuffer) null));
        Assert.assertSame(HashTableIteratorItem.State.EMPTY, HashTableIteratorItem.State.fromBytes((ByteBuf) null));
    }

    @Test
    public void testFromBytes() {
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[123]);
        HashTableIteratorItem.State s = HashTableIteratorItem.State.fromBytes(buf);
        Assert.assertEquals(buf, Unpooled.wrappedBuffer(s.toBytes()));
        Assert.assertEquals(s.toBytes(), HashTableIteratorItem.State.fromBytes(s.toBytes()).toBytes());
        Assert.assertEquals(s.toBytes(), HashTableIteratorItem.State.fromBytes(Unpooled.wrappedBuffer(s.toBytes())).toBytes());
    }

    @Test
    public void testCopyOf() {
        Assert.assertSame(HashTableIteratorItem.State.EMPTY, HashTableIteratorItem.State.copyOf(HashTableIteratorItem.State.EMPTY));
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[123]);
        HashTableIteratorItem.State s = HashTableIteratorItem.State.fromBytes(buf);
        HashTableIteratorItem.State s2 = HashTableIteratorItem.State.copyOf(s);
        Assert.assertEquals(s.toBytes(), s2.toBytes());

        // This way we verify that the two are really pointing to different buffers.
        buf.release();
        Assert.assertEquals(s2.toBytes(), HashTableIteratorItem.State.fromBytes(s2.toBytes()).toBytes());
    }
}
