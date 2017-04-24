/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import io.pravega.testcommon.AssertExtensions;

import lombok.Data;

public class JavaSerializerTest {

    @Data
    private static class Foo implements Serializable {
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
