/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;


import io.pravega.client.netty.impl.Flow;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FlowTest {

    @Test
    public void testNextSequenceNumber() {
        Flow id = Flow.create();
        assertEquals(id.getFlowId(), (int) (id.asLong() >> 32));
        assertEquals("SequenceNumber should be 0 for " + id, 0, (int) id.asLong());
        assertEquals("SequenceNumber should be incremented", (int) id.asLong() + 1, (int) id.getNextSequenceNumber());
        assertEquals(id, Flow.from(id.asLong()));
    }

    @Test
    public void testSequenceNumberOverflow() {
        Flow id = new Flow(Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(0,  (int) id.getNextSequenceNumber());
    }
}