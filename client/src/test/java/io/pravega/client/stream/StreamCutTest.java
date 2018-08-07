/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.StreamCutImpl;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import lombok.Cleanup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StreamCutTest {

    @Test
    public void testStreamCutSerialization() throws Exception {
        StreamCut sc = new StreamCutImpl(Stream.of("scope", "stream"),
                ImmutableMap.of(new Segment("scope", "stream", 0), 10L,
                        new Segment("scope", "stream", 1), 20L));

        byte[] buf = serialize(sc);
        String base64 = sc.asText();
        assertEquals(sc, deSerializeStreamCut(buf));
        assertEquals(sc, StreamCut.fromBytes(sc.toBytes()));
        assertEquals(sc, StreamCut.from(base64));
    }

    @Test
    public void testUnboundedStreamCutSerialization() throws Exception {
        StreamCut sc = StreamCut.UNBOUNDED;
        final byte[] buf = serialize(sc);
        String base64 = sc.asText();
        assertEquals(sc, deSerializeStreamCut(buf));
        assertNull(deSerializeStreamCut(buf).asImpl());
        assertEquals(sc, StreamCut.fromBytes(sc.toBytes()));
        assertEquals(sc, StreamCut.from(base64));
    }

    private byte[] serialize(StreamCut sc) throws IOException {
        @Cleanup
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @Cleanup
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(sc);
        return baos.toByteArray();
    }

    private StreamCut deSerializeStreamCut(final byte[] buf) throws Exception {
        @Cleanup
        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        @Cleanup
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (StreamCut) ois.readObject();
    }
}
