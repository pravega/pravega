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

/**
 * An item scheduled for a point in time in the future. 
 * This is used by {@link ThreadPoolScheduledExecutorService}
 */
interface Scheduled {
    
    /**
     * Returns the time in nanos (as defined by {@link System#nanoTime()} that this item is
     * scheduled for.
     */
    long getScheduledTimeNanos();
    
    /**
     * Returns true if the {@link #getScheduledTimeNanos()} is now or was previously in the future.
     */
    boolean isDelayed();
    
}