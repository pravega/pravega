package io.pravega.client.stream.impl;

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
        
        serialized.position(serialized.position()+1);
        assertEquals(2, serialized.remaining());
        assertEquals(3, buff.remaining());
        
        ByteBuffer subBuffer = serializer.serialize(serialized);
        assertEquals(2, serializer.deserialize(subBuffer).capacity());
       
        
        ByteBuffer empty = ByteBuffer.allocate(0);
        serialized = serializer.serialize(empty);
        assertEquals(empty, serializer.deserialize(serialized));
    }
    
}
