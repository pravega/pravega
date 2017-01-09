/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.autoscaling.util;

import lombok.Synchronized;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Rolling window utility. It stores objects of type T over the rolling window. Whenever an object expires,
 * it is evicted from the store.
 * When new elements are added, they are
 * <p>
 * Elements added to rolling window have their own timestamp when they were created.
 * They are allowed into the rolling window if this time is within the current rolling window and evicted once
 * they cease to be in the rolling window.
 * It is important to note that Rolling window does not assume that all elements added in order of their timestamp.
 * However, rolling window needs to ensure time based eviction for the time of creation of the elements
 * and not time of addition into the queue.
 *
 * @param <T> Type of value stored in rolling window
 */
public class RollingWindow<T> {
    private static final long INITIAL_DELAY = 1;
    private static final long PERIOD = 1;

    /**
     * Duration of rolling window.
     */
    private final Duration duration;

    /**
     * Executor used for scheduling evictions. Every time an entry is added into the rolling window, an eviction for that
     * entry is scheduled after its expiry from rolling window.
     */
    private final ScheduledExecutorService executor;

    /**
     * SkiplistMap contains treemap which stores values sorted by the key.
     * Multiple values (typically from different segments) could have same timestamp(key).
     * So we maintain a list of values for any timeStamp.
     */
    private final ConcurrentSkipListMap<Long, List<T>> rollingWindow;

    public RollingWindow(Duration duration, ScheduledExecutorService executor) {
        this.duration = duration;
        this.executor = executor;
        rollingWindow = new ConcurrentSkipListMap<>();

        /**
         * Periodic timer to ensure that we clean up memory for streams that do not have any activity resulting
         * in evictions. Periodically evicting is garbage collection.
         */
        this.executor.scheduleAtFixedRate(this::evict, INITIAL_DELAY, PERIOD, TimeUnit.HOURS);
    }

    /**
     * Rolling window returns a list of elements sorted by order in which they were added.
     *
     * @return flatmap and return list of values sorted in ascending order of timestamps.
     */
    public List<T> getElements() {
        return rollingWindow.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }

    /**
     * Method to add element e which was created at timestamp into the rolling window.
     * Entries are added to the rolling window if the timestamp lies within rolling window.
     * Rolling window starts from current time and goes back to curenttime - rollind-window-duration
     * When an element is inserted, its eviction is scheduled for its expiry.
     *
     * @param timestamp Time when element was created.
     * @param e         Element to be instered.
     */
    public void addElement(final long timestamp, final T e) {
        if (timestamp > System.currentTimeMillis() - duration.toMillis()) {
            this.executor.schedule(this::evict, timestamp + duration.toMillis() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

            List<T> list = rollingWindow.get(timestamp);
            if (list == null) {
                list = new ArrayList<>();
            }

            list.add(e);
            rollingWindow.put(timestamp, list);
        }
    }

    /**
     * Method to evict keys that are expired.
     */
    @Synchronized
    private void evict() {
        // evict all keys older than the duration of rolling window's size.
        rollingWindow.keySet().removeAll(rollingWindow.keySet().stream()
                .filter(x -> System.currentTimeMillis() - x > duration.toMillis()).collect(Collectors.toList()));
    }
}
