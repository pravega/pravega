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
package io.pravega.client.stream.notifications;

/**
 * This represents an observable notification.
 * @param <T> The type of event that is to be observed.
 */
public interface Observable<T> {
    /**
     * Register listener for notification type T. Multiple listeners can be added for the same type.
     * @param listener This is the listener which will be invoked incase of an Notification.
     *
     */
    void registerListener(final Listener<T> listener);

    /**
     * Remove a listener.
     * @param listener the listener which needs to be removed.
     */
    void unregisterListener(final Listener<T> listener);

    /**
     * Remove all listeners for a given type.
     */
    void unregisterAllListeners();

    /**
     * Get the notification type.
     * @return Notification type.
     */
    String getType();
}
