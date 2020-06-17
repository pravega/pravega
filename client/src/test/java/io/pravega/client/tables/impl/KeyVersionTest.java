/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import lombok.Cleanup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KeyVersionTest {

    @Test
    public void testKeyVersionSerialization() throws Exception {
        KeyVersion kv = new KeyVersionImpl(5L);
        assertEquals(kv, KeyVersion.fromBytes(kv.toBytes()));
        byte[] buf = serialize(kv);
        assertEquals(kv, deSerializeKeyVersion(buf));
    }

    @Test
    public void testNotExistsKeySerialization() throws Exception {
        KeyVersion kv = KeyVersion.NOT_EXISTS;
        assertEquals(kv, KeyVersion.fromBytes(kv.toBytes()));
        byte[] buf = serialize(kv);
        assertEquals(kv, deSerializeKeyVersion(buf));
    }

    @Test
    public void testNoVersionKeySerialization() throws Exception {
        KeyVersion kv = KeyVersion.NO_VERSION;
        assertEquals(kv, KeyVersion.fromBytes(kv.toBytes()));
        byte[] buf = serialize(kv);
        assertEquals(kv, deSerializeKeyVersion(buf));
    }

    private byte[] serialize(KeyVersion sc) throws IOException {
        @Cleanup
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @Cleanup
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(sc);
        return baos.toByteArray();
    }

    private KeyVersion deSerializeKeyVersion(final byte[] buf) throws Exception {
        @Cleanup
        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        @Cleanup
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (KeyVersion) ois.readObject();
    }
}
