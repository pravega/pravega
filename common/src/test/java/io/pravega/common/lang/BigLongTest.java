/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.lang;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BigLongTest {
    @Test
    public void bigLongTest() {
        BigLong counter = new BigLong(0, 1L);
        BigLong counter2 = new BigLong(0, 1L);
        BigLong counter3 = new BigLong(0, 2L);
        BigLong counter4 = new BigLong(1, 1L);
        BigLong counter5 = new BigLong(1, Long.MAX_VALUE - 1);

        // comparison
        assertTrue(counter.compareTo(counter2) == 0);
        assertTrue(counter.compareTo(counter3) < 0);
        assertTrue(counter.compareTo(counter4) < 0);
        assertTrue(counter4.compareTo(counter3) > 0);

        // tobytes and frombytes
        assertEquals(counter, BigLong.fromBytes(counter.toBytes()));

        // add
        BigLong added = BigLong.add(counter, 100);
        assertEquals(0, added.getMsb());
        assertEquals(101, added.getLsb());

        // add 1 to check we have max allowed lsb covered
        BigLong added2 = BigLong.add(counter5, 1);
        assertEquals(1, added2.getMsb());
        assertEquals(Long.MAX_VALUE, added2.getLsb());

        // add hundred to verify if that the range is covered.
        BigLong added3 = BigLong.add(counter5, 100);
        assertEquals(2, added3.getMsb());
        assertEquals(99, added3.getLsb());
    }

    @Test
    public void atomicBigLongTest() {
        AtomicBigLong atomicCounter = new AtomicBigLong();
        assertEquals(BigLong.ZERO, atomicCounter.get());

        AtomicBigLong atomicCounter2 = new AtomicBigLong(0, 1L);
        assertEquals(new BigLong(0, 1L), atomicCounter2.get());

        BigLong counter = atomicCounter.incrementAndGet();
        assertEquals(counter, atomicCounter.get());
        assertEquals(new BigLong(0, 1L), counter);
    }
}
