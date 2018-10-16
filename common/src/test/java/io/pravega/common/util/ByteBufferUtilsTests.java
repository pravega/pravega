/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteBufferUtilsTests {

    @Test
    public void testCopy() {
        ByteBuffer a = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 });
        ByteBuffer b = ByteBuffer.wrap(new byte[] { 10, 11 });
        int num = ByteBufferUtils.copy(a, b);
        assertEquals(2, num);
        assertEquals(1, b.get());
        assertEquals(2, b.get());
        assertEquals(2, a.position());
        
        a = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 });
        b = ByteBuffer.wrap(new byte[] { 10, 11 });
        num = ByteBufferUtils.copy(b, a);
        assertEquals(2, num);
        assertEquals(0, a.position());
        assertEquals(2, a.limit());
        assertEquals(2, b.position());
        assertEquals(10, a.get());
        assertEquals(11, a.get());
    }

}
