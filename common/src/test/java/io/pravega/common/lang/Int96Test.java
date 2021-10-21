/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.lang;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Int96Test {
    @Test
    public void bigLongTest() {
        Int96 counter = new Int96(0, 1L);
        Int96 counter2 = new Int96(0, 1L);
        Int96 counter3 = new Int96(0, 2L);
        Int96 counter4 = new Int96(1, 1L);
        Int96 counter5 = new Int96(1, Long.MAX_VALUE - 1);

        // comparison
        assertTrue(counter.compareTo(counter2) == 0);
        assertTrue(counter.compareTo(counter3) < 0);
        assertTrue(counter.compareTo(counter4) < 0);
        assertTrue(counter4.compareTo(counter3) > 0);

        // tobytes and frombytes
        assertEquals(counter, Int96.fromBytes(counter.toBytes()));

        // add
        Int96 added = counter.add(100);
        assertEquals(0, added.getMsb());
        assertEquals(101, added.getLsb());

        // add 1 to check we have max allowed lsb covered
        Int96 added2 = counter5.add(1);
        assertEquals(1, added2.getMsb());
        assertEquals(Long.MAX_VALUE, added2.getLsb());

        // add hundred to verify if that the range is covered.
        Int96 added3 = counter5.add(100);
        assertEquals(2, added3.getMsb());
        assertEquals(99, added3.getLsb());
    }

    @Test
    public void atomicBigLongTest() {
        AtomicInt96 atomicCounter = new AtomicInt96();
        assertEquals(Int96.ZERO, atomicCounter.get());

        AtomicInt96 atomicCounter2 = new AtomicInt96(0, 1L);
        assertEquals(new Int96(0, 1L), atomicCounter2.get());

        Int96 counter = atomicCounter.incrementAndGet();
        assertEquals(counter, atomicCounter.get());
        assertEquals(new Int96(0, 1L), counter);
    }
}
