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

import com.google.common.base.Strings;
import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UTF8StringSerializerTest {

    @Test
    public void testString() {
        UTF8StringSerializer serializer = new UTF8StringSerializer();
        String one = "this is a test string";
        String result = serializer.deserialize(serializer.serialize(one));
        assertEquals(one, result);
    }

    @Test
    public void testEmptyString() {
        UTF8StringSerializer serializer = new UTF8StringSerializer();
        String one = "";
        String result = serializer.deserialize(serializer.serialize(one));
        assertEquals(one, result);
    }

    @Test
    public void testLargerThan1MBString() {
        UTF8StringSerializer serializer = new UTF8StringSerializer();
        String one = Strings.repeat("0123456789012345", 65537);
        String result = serializer.deserialize(serializer.serialize(one));
        assertEquals(one, result);
    }

    @Test
    public void testByteArrayWithOffset() {
        UTF8StringSerializer serializer = new UTF8StringSerializer();
        byte[] part1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        String part2Str = "this is a test string";
        ByteBuffer part2 = serializer.serialize(part2Str);
        int part2Length = part2.remaining();
        byte[] part3 = new byte[] { 10, 11, 12 };
        // Create a new buffer with parts 1, 2, and 3..
        ByteBuffer buf = ByteBuffer.allocate(part1.length + part2Length + part3.length);
        buf.put(part1);
        buf.put(part2);
        buf.put(part3);
        // Set buffer's position and length so that it points to part 2 only.
        buf.position(part1.length);
        buf.limit(part1.length + part2Length);
        String result = serializer.deserialize(buf);
        assertEquals(part2Str, result);
    }

}
