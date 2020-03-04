/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link IteratorState} class.
 */
public class IteratorStateTests {
    @Test
    public void testEmpty() {
        Assert.assertTrue(IteratorState.EMPTY.isEmpty());
        Assert.assertEquals(0, IteratorState.EMPTY.getToken().readableBytes());
        Assert.assertSame(IteratorState.EMPTY, IteratorState.fromBytes(null));
    }

    @Test
    public void testFromBytes() {
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[123]);
        Assert.assertSame(buf, IteratorState.fromBytes(buf).getToken());
    }
}
