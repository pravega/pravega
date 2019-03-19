/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RequestIdTest {

    @Test
    public void testNextSequenceNumber() {
        RequestId id = new RequestId();
        assertEquals(id.getSessionId(), (int) (id.asLong() >> 32));
        assertEquals("SequenceNumber should be 0 for " + id, 0, (int) id.asLong());
        assertEquals("SequenceNumber should be incremented", (int) id.asLong() + 1, (int) id.getNextSequenceNumber());
    }

    @Test
    public void testSequenceNumberOverflow() {
        RequestId id = new RequestId(Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(0,  (int) id.getNextSequenceNumber());
    }
}