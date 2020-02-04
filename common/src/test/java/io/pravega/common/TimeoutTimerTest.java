/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common;

import java.time.Duration;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeoutTimerTest {

    @Test
    public void testTimeouts() {
        TimeoutTimer timer = new TimeoutTimer(Duration.ofMillis(0));
        assertFalse(timer.hasRemaining());
        timer.reset(Duration.ofMillis(10000));
        assertTrue(timer.hasRemaining());
        timer.zero();
        assertFalse(timer.hasRemaining());
    }
    
    
    @Test
    public void testResetToZero() {
        TimeoutTimer timer = new TimeoutTimer(Duration.ofMillis(10000));
        assertTrue(timer.hasRemaining());
        timer.reset(Duration.ofMillis(0));
        assertFalse(timer.hasRemaining());
        timer.zero();
        assertFalse(timer.hasRemaining());
    }
    
}
