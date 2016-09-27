package com.emc.pravega.stream.impl;

import static org.junit.Assert.*;

import org.junit.Test;

public class EventRouterTest {

    @Test
    public void testLongBits() {
        assertEquals(1.0, EventRouterImpl.longToDoubleFraction(-1), 0.0000001);
        assertEquals(0.0, EventRouterImpl.longToDoubleFraction(0), 0.0000001);
        assertEquals(0.5, EventRouterImpl.longToDoubleFraction(Long.reverse(1)), 0.0000001);
        assertEquals(0.25, EventRouterImpl.longToDoubleFraction(Long.reverse(2)), 0.0000001);
        assertEquals(0.75, EventRouterImpl.longToDoubleFraction(Long.reverse(3)), 0.0000001);
        assertEquals(0.125, EventRouterImpl.longToDoubleFraction(Long.reverse(4)), 0.0000001);
        assertEquals(0.625, EventRouterImpl.longToDoubleFraction(Long.reverse(5)), 0.0000001);
        assertEquals(0.375, EventRouterImpl.longToDoubleFraction(Long.reverse(6)), 0.0000001);
        assertEquals(0.875, EventRouterImpl.longToDoubleFraction(Long.reverse(7)), 0.0000001);
    }
    
}
