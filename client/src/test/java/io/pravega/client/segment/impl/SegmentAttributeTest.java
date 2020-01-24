/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import java.util.HashSet;
import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SegmentAttributeTest {

    @Test
    public void testNoCollisions() {
        HashSet<UUID> ids = new HashSet<>();
        for (SegmentAttribute a : SegmentAttribute.values()) {
            ids.add(a.getValue());
        }
        assertEquals(ids.size(), SegmentAttribute.values().length);
    }
    
}
