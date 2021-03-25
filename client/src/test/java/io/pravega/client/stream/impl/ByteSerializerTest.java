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
package io.pravega.client.stream.impl;

import io.pravega.common.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ByteSerializerTest {

    @Test
    public void testByteArraySerializer() {
        ByteArraySerializer arraySerializer = new ByteArraySerializer();
        byte[] array = new byte[] { 1, 2, 3 };
        ByteBuffer serialized = arraySerializer.serialize(array);
        assertTrue(Arrays.equals(array, arraySerializer.deserialize(serialized)));
        byte[] emptyArray = new byte[] { };
        serialized = arraySerializer.serialize(emptyArray);
        assertTrue(Arrays.equals(emptyArray, arraySerializer.deserialize(serialized)));
    }

    @Test
    public void testByteBufferSerializer() {
        ByteBufferSerializer serializer = new ByteBufferSerializer();
        ByteBuffer buff = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
        ByteBuffer serialized = serializer.serialize(buff);
        assertEquals(buff, serializer.deserialize(serialized));

        serialized.position(serialized.position() + 1);
        assertEquals(2, serialized.remaining());
        assertEquals(3, buff.remaining());

        ByteBuffer subBuffer = serializer.serialize(serialized);
        assertEquals(2, serializer.deserialize(subBuffer).capacity());

        ByteBuffer empty = ByteBufferUtils.EMPTY;
        serialized = serializer.serialize(empty);
        assertEquals(empty, serializer.deserialize(serialized));
    }
}
