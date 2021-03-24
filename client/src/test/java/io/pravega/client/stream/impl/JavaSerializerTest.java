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

import io.pravega.test.common.AssertExtensions;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JavaSerializerTest {

    @Data
    private static class Foo implements Serializable {
        private static final long serialVersionUID = 1L;
        final int x;
    }

    @Test
    public void testObject() {
        JavaSerializer<Foo> serializer = new JavaSerializer<>();
        Foo one = new Foo(1);
        Foo result = serializer.deserialize(serializer.serialize(one));
        assertEquals(one, result);
    }

    @Test
    public void testSelf() {
        JavaSerializer<JavaSerializer<String>> serializer = new JavaSerializer<>();
        JavaSerializer<String> one = new JavaSerializer<String>();
        JavaSerializer<String> result = serializer.deserialize(serializer.serialize(one));
        assertEquals(one, result);
    }
    
    @Test
    public void testMap() {
        JavaSerializer<HashMap<Integer, Integer>> serializer = new JavaSerializer<>();
        HashMap<Integer, Integer> in = new HashMap<>();
        in.put(1, 1);
        in.put(2, 2);
        in.put(3, 3);
        HashMap<Integer, Integer> out = serializer.deserialize(serializer.serialize(in));
        assertEquals(in, out);
    }

    @Test
    public void testByteArray() {
        JavaSerializer<byte[]> serializer = new JavaSerializer<>();
        byte[] in = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        byte[] out = serializer.deserialize(serializer.serialize(in));
        AssertExtensions.assertArrayEquals("testByteArray failed", in, 0, out, 0, in.length);
    }

    @Test
    @SuppressWarnings({"rawtypes", "CollectionAddedToSelf"})
    public void testRefCycle() {
        JavaSerializer<HashMap<Integer, Map>> serializer = new JavaSerializer<>();
        HashMap<Integer, Map> in = new HashMap<>();
        in.put(1, in);
        in.put(2, in);
        in.put(3, in);
        HashMap<Integer, Map> out = serializer.deserialize(serializer.serialize(in));
        assertEquals(in.keySet(), out.keySet());
    }

}
