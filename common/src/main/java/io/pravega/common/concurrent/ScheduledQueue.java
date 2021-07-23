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

import com.google.common.collect.Iterators;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import lombok.Data;
import lombok.val;

/**
 * Provides a unbounded blocking queue which {@link Scheduled} items can be added to.
 * Items which are scheduled will not be returned from {@link #poll()} or {@link #take()} until their scheduled time.
 * 
 * This class is similar to DelayQueue but it allows a delay to be optional. This allows adding and polling non-delayed tasks in O(1).
 * It also it lock-free, and may offer higher throughput under contention. 
 */
public class ScheduledQueue<E extends Scheduled> extends AbstractQueue<E> implements BlockingQueue<E> {
   
    private final AtomicLong itemsAdded = new AtomicLong(0);
    private final AtomicLong itemsRemoved = new AtomicLong(0);
    private final ConcurrentSkipListMap<FireTime, E> delayedTasks = new ConcurrentSkipListMap<FireTime, E>();
    private final ConcurrentLinkedQueue<E> readyTasks = new ConcurrentLinkedQueue<E>();
    private final Semaphore blocker = new Semaphore(1);
    
    @Data
    private static final class FireTime implements Comparable<FireTime> {
        private final long timeNanos;
        private final long sequenceNumber;

        @Override
        public int compareTo(FireTime other) {
            if (this.timeNanos < other.timeNanos) {
                return -1;
            } else if (this.timeNanos > other.timeNanos) {
                return 1;
            } else {
                return Long.compare(this.sequenceNumber, other.sequenceNumber);
            }
        }
    }

    /**
     * Retrieves and removes the head of this queue, waiting if necessary
     * until an element becomes available.
     *
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public E take() throws InterruptedException {
        return poll(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }


    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        @SuppressWarnings("unused")
        boolean ignored = blocker.tryAcquire();
        E result = readyTasks.poll();
        if (result != null) {
            this.itemsRemoved.incrementAndGet();
            return result;
        }
        long startTime = System.nanoTime();
        long now = startTime;
        while (now - startTime <= timeout) {
            val delayed = delayedTasks.firstEntry();
            if (delayed != null && delayed.getKey().getTimeNanos() <= now) {
                if (delayedTasks.remove(delayed.getKey(), delayed.getValue())) {
                    val next = delayedTasks.firstEntry();
                    if (next != null && next.getKey().timeNanos <= now) {
                        //In case there are more tasks that are ready wake up other callers.
                        blocker.release();
                    }
                    this.itemsRemoved.incrementAndGet();
                    return delayed.getValue();
                } else {
                    continue; // Some other thread grabbed the task.
                }
            } else {
                //If readyTasks is non-empty then blocker must have permits.
                ignored = blocker.tryAcquire(sleepTimeout(timeout, startTime, now, delayed), TimeUnit.NANOSECONDS);
                result = readyTasks.poll();
                if (result != null) {
                    this.itemsRemoved.incrementAndGet();
                    return result;
                }
            }
            now = System.nanoTime();
        }
        return null;
    }
    
    @Override
    public E poll() {
        @SuppressWarnings("unused")
        boolean ignored = blocker.tryAcquire();
        E result = readyTasks.poll();
        if (result != null) {
            this.itemsRemoved.incrementAndGet();
            return result;
        }
        while (true) {
            val delayed = delayedTasks.firstEntry();
            if (delayed == null || delayed.getKey().getTimeNanos() > System.nanoTime()) {
                return null;
            } 
            if (delayedTasks.remove(delayed.getKey(), delayed.getValue())) {
                this.itemsRemoved.incrementAndGet();
                return delayed.getValue();
            }
        }
    }

    private long sleepTimeout(long timeout, long startTime, long now, Entry<FireTime, E> delayed) {
        long sleepTimeout = timeout - (now - startTime);
        if (delayed != null) {
            sleepTimeout = Math.min(sleepTimeout, delayed.getKey().getTimeNanos() - now);
        }
        return sleepTimeout;
    }
    
    /**
     * Inserts the specified element into this delay queue.
     *
     * @param e the element to add
     * @return {@code true} (as specified by {@link Collection#add})
     * @throws NullPointerException if the specified element is null
     */
    @Override
    public boolean add(E e) {
        return offer(e);
    }

    /**
     * Inserts the specified element into this delay queue.
     *
     * @param e the element to add
     * @return {@code true}
     * @throws NullPointerException if the specified element is null
     */
    @Override
    public boolean offer(E e) {
        long seq = itemsAdded.incrementAndGet();
        if (!e.isDelayed()) {
            readyTasks.add(e);
        } else {
            delayedTasks.put(new FireTime(e.getScheduledTimeNanos(), seq), e);
        }
        // This is done unconditionally even if delayed because it could be the
        // new shortest delay in which case some thread should wake up to
        // re-schedule its sleep.
        blocker.release();
        return true;
    }

    /**
     * Inserts the specified element into this delay queue. As the queue is
     * unbounded this method will never block.
     *
     * @param e the element to add
     * @param timeout This parameter is ignored as the method never blocks
     * @param unit This parameter is ignored as the method never blocks
     * @return {@code true}
     * @throws NullPointerException {@inheritDoc}
     */
    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) {
        return offer(e);
    }
    
    /**
     * Inserts the specified element into this delay queue. As the queue is
     * unbounded this method will never block.
     *
     * @param e the element to add
     * @throws NullPointerException {@inheritDoc}
     */
    @Override
    public void put(E e) {
        offer(e);
    }

    /**
     * Retrieves, but does not remove, the head of this queue, or
     * returns {@code null} if this queue is empty. 
     * 
     * Unlike {@code poll}, this method returns the elements who's scheduled time has not yet arrived.
     * 
     * (Items added concurrently may or may not be observed)
     *
     * @return the head of this queue, or {@code null} if this queue is empty
     */
    @Override
    public E peek() {
        val ready = readyTasks.peek();
        if (ready == null) {            
            val result = delayedTasks.firstEntry();
            if (result == null) {
                return null;
            } else {            
                return result.getValue();
            }
        } else {
            return ready;
        }
    }

    /**
     * Returns the size of this collection. 
     * The value is only guaranteed to be accurate if there are not concurrent operations being performed.
     * If there are, it may reflect the operation or not.
     * This call is O(1).
     */
    @Override
    public int size() {
        return (int) Long.min(itemsAdded.get() - itemsRemoved.get(), Integer.MAX_VALUE);
    }

    /**
     * Always returns {@code Integer.MAX_VALUE} because
     * a {@code ScheduledQueue} is not capacity constrained.
     *
     * @return {@code Integer.MAX_VALUE}
     */
    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    /**
     * Returns an array containing all of the elements in this queue.
     * (Items added concurrently may or may not be included)
     *
     * @param a the array into which the elements of the queue are to
     *          be stored, if it is big enough; otherwise, a new array of the
     *          same runtime type is allocated for this purpose
     * @return an array containing all of the elements in this queue
     */
    @Override
    public <T> T[] toArray(@Nonnull T[] a) {
        ArrayList<E> result = new ArrayList<E>();
        for (E val : readyTasks) {
            result.add(val);
        }
        for (E val : delayedTasks.values()) {
            result.add(val);
        }
        return result.toArray(a);
    }

    /**
     * Removes a single instance of the specified element from this
     * queue, if it is present, whether or not it has expired.
     */
    @Override
    public boolean remove(Object o) {
        if (readyTasks.remove(o)) {
            this.itemsRemoved.incrementAndGet();
            return true;
        } else {
            if (delayedTasks.values().remove(o)) {
                this.itemsRemoved.incrementAndGet();
                return true;
            }
            return false;
        }
    }

    /**
     * Returns an iterator over all the items in the queue. 
     *
     * <p>The returned iterator is
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * @return an iterator over the elements in this queue
     */
    @Override
    public Iterator<E> iterator() {
        return Iterators.unmodifiableIterator(Iterators.concat(readyTasks.iterator(), delayedTasks.values().iterator()));
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        int itemCount = 0;
        blocker.drainPermits();
        while (itemCount < maxElements) {
            E item = readyTasks.poll();
            if (item == null) {
                break;
            }
            c.add(item);
            itemCount++;
        }
        while (itemCount < maxElements) {
            Entry<FireTime, E> item = delayedTasks.pollFirstEntry();
            if (item == null) {
                break;
            }
            c.add(item.getValue());
            itemCount++;
        }
        itemsRemoved.addAndGet(itemCount);
        blocker.release(); //In case there are items remaining
        return itemCount;
    }

    /**
     * Clears all delayed tasks from the queue but leaves those which can be polled immediately. 
     */
    public List<E> drainDelayed() {
        ArrayList<E> result = new ArrayList<>();
        Entry<FireTime, E> item = delayedTasks.pollFirstEntry();
        while (item != null) {
            result.add(item.getValue());
            itemsRemoved.incrementAndGet();
            item = delayedTasks.pollFirstEntry();
        }
        return result;
    }
    
}
