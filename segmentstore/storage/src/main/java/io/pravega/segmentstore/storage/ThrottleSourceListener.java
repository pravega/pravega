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
package io.pravega.segmentstore.storage;

/**
 * Defines a listener that will be notified every time the state of the a Throttling Source changed.
 */
public interface ThrottleSourceListener {
    /**
     * Notifies this {@link ThrottleSourceListener} the state of the Throttling Source has changed.
     */
    void notifyThrottleSourceChanged();

    /**
     * Gets a value indicating whether this {@link ThrottleSourceListener} is closed and should be unregistered.
     *
     * @return True if need to be unregistered (no further notifications will be sent), false otherwise.
     */
    boolean isClosed();
}
