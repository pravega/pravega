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

package io.pravega.common.concurrent;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ScheduledQueueTest {

    @Data
    private static class NoDelay implements Scheduled {
        final int id;
        
        @Override
        public long getScheduledTimeNanos() {
            return 0;
        }

        @Override
        public boolean isDelayed() {
            return false;
        }
    }
    
    @Data
    private static class Delay implements Scheduled {
        final long time;
        
        @Override
        public long getScheduledTimeNanos() {
            return time;
        }

        @Override
        public boolean isDelayed() {
            return true;
        }
    }
    
    @Test(timeout = 5000)
    public void testNonDelayed() {
        ScheduledQueue<NoDelay> queue = new ScheduledQueue<NoDelay>();
        queue.add(new NoDelay(1));
        queue.add(new NoDelay(2));
        queue.add(new NoDelay(3));
        assertEquals(1, queue.poll().id);
        assertEquals(2, queue.poll().id);
        assertEquals(3, queue.poll().id);
        assertEquals(null, queue.poll());
    }
    
    @Test(timeout = 5000)
    public void testDelayed() {
        ScheduledQueue<Delay> queue = new ScheduledQueue<Delay>();
        queue.add(new Delay(1));
        queue.add(new Delay(3));
        queue.add(new Delay(2));
        assertEquals(1, queue.poll().time);
        assertEquals(2, queue.poll().time);
        assertEquals(3, queue.poll().time);
        assertEquals(null, queue.poll());
    }
    
    @Test(timeout = 5000)
    public void testPoll() throws InterruptedException {
        ScheduledQueue<Scheduled> queue = new ScheduledQueue<Scheduled>();
        assertNull(queue.poll(5, TimeUnit.SECONDS));
        queue.add(new Delay(Long.MAX_VALUE));
        assertNull(queue.poll(0, TimeUnit.SECONDS));
        queue.add(new Delay(1));
        queue.add(new Delay(3));
        queue.add(new Delay(2));
        queue.add(new NoDelay(4));
        assertEquals(0, queue.poll(5, TimeUnit.SECONDS).getScheduledTimeNanos());
        assertEquals(1, queue.take().getScheduledTimeNanos());
        assertEquals(2, queue.poll(5, TimeUnit.SECONDS).getScheduledTimeNanos());
        assertEquals(3, queue.poll(5, TimeUnit.SECONDS).getScheduledTimeNanos());
        assertEquals(null, queue.poll(0, TimeUnit.SECONDS));
        assertEquals(1, queue.size());
    }
    
    @Test(timeout = 5000)
    public void testPeek() {
        ScheduledQueue<Scheduled> queue = new ScheduledQueue<Scheduled>();
        assertNull(queue.peek());
        Delay delay = new Delay(1);
        queue.add(delay);
        assertEquals(delay, queue.peek());
        NoDelay noDelay = new NoDelay(1);
        queue.add(noDelay);
        assertEquals(noDelay, queue.peek());
        queue.poll();
        assertEquals(delay, queue.peek());
        queue.poll();
        assertNull(queue.peek());
    }
    
    @Test(timeout = 5000)
    public void testSize() {
        ScheduledQueue<Scheduled> queue = new ScheduledQueue<Scheduled>();
        assertEquals(0, queue.size());
        queue.add(new NoDelay(1));
        assertEquals(1, queue.size());
        NoDelay nd2 = new NoDelay(2);
        queue.add(nd2);
        assertEquals(2, queue.size());
        queue.add(new NoDelay(3));
        assertEquals(3, queue.size());
        queue.add(new Delay(1));
        assertEquals(4, queue.size());
        Delay d3 = new Delay(3);
        queue.add(d3);
        assertEquals(5, queue.size());
        queue.add(new Delay(2));
        assertEquals(6, queue.size());
        assertNotNull(queue.poll());
        assertEquals(5, queue.size());
        assertTrue(queue.remove(nd2));
        assertEquals(4, queue.size());
        assertTrue(queue.remove(d3));
        assertEquals(3, queue.size());
        queue.drainDelayed();
        assertEquals(1, queue.size());
        queue.clear();
        assertEquals(0, queue.size());
    }
    
    @Test(timeout = 5000)
    public void testToArray() {
        ScheduledQueue<Scheduled> queue = new ScheduledQueue<Scheduled>();
        queue.add(new Delay(Long.MAX_VALUE));
        queue.add(new Delay(1));
        queue.add(new Delay(3));
        queue.add(new Delay(2));
        queue.add(new NoDelay(4));
        Object[] objects = queue.toArray();
        assertEquals(5, objects.length);
        assertEquals(new NoDelay(4), objects[0]);
        assertEquals(new Delay(1), objects[1]);
        assertEquals(new Delay(2), objects[2]);
        assertEquals(new Delay(3), objects[3]);
        assertEquals(new Delay(Long.MAX_VALUE), objects[4]);
    }
    
    @Test(timeout = 5000)
    public void testDrainTo() {
        ScheduledQueue<Scheduled> queue = new ScheduledQueue<Scheduled>();
        queue.add(new Delay(Long.MAX_VALUE));
        queue.add(new Delay(1));
        queue.add(new Delay(3));
        queue.add(new Delay(2));
        queue.add(new NoDelay(4));
        ArrayList<Scheduled> result = new ArrayList<>();
        queue.drainTo(result, 2);
        assertEquals(2, result.size());
        assertEquals(new NoDelay(4), result.get(0));
        assertEquals(new Delay(1), result.get(1));
        queue.drainTo(result);
        assertEquals(5, result.size());
        assertEquals(new NoDelay(4), result.get(0));
        assertEquals(new Delay(1), result.get(1));
        assertEquals(new Delay(2), result.get(2));
        assertEquals(new Delay(3), result.get(3));
        assertEquals(new Delay(Long.MAX_VALUE), result.get(4));
    }
}
