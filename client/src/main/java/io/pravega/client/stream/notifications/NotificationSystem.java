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

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import javax.annotation.concurrent.GuardedBy;
import lombok.Data;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NotificationSystem {
    @GuardedBy("$lock")
    private final Multimap<String, ListenerWithExecutor<Notification>> map = ArrayListMultimap.create();

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Synchronized
    public <T extends Notification> void addListeners(final String type,
                                                      final Listener<T> listener,
                                                      final ScheduledExecutorService executor) {
        if (!isListenerPresent(listener)) {
            map.put(type, new ListenerWithExecutor(listener, executor));
        }
    }

    /**
     * This method will ensure the notification is intimated to the listeners of the same type.
     *
     * @param notification Notification to be notified.
     * @param <T>   Type of notification.
     */
    @Synchronized
    public <T extends Notification> void notify(final T notification) {
        String type = notification.getClass().getSimpleName();
        map.get(type).forEach(l -> {
            log.info("Executing listener of type: {} for notification: {}", type, notification);
            ExecutorServiceHelpers.execute(() -> l.getListener().onNotification(notification),
                    throwable -> log.error("Exception while executing listener for notification: {}", notification),
                    () -> log.info("Completed execution of notify for notification :{}", notification),
                    l.getExecutor());
        });
    }

    /**
     * Remove Listener of a given notification type.
     *
     * @param <T>      Type of notification.
     * @param type     Type of notification listener.
     * @param listener Listener to be removed.
     */
    @Synchronized
    public <T extends Notification> void removeListener(final String type, final Listener<T> listener) {
        map.get(type).removeIf(e -> e.getListener().equals(listener));
    }

    /**
     * Remove all listeners of a notification type.
     *
     * @param type Type of notification listener.
     */
    @Synchronized
    public void removeListeners(final String type) {
        map.removeAll(type);
    }

    /**
     * Check if a Listener is present for a given notification type.
     *
     * @param type Type of notification listener.
     * @return true if Listener is present.
     */
    @Synchronized
    public boolean isListenerPresent(final String type) {
        return !map.get(type).isEmpty();
    }

    private <T extends Notification> boolean isListenerPresent(final Listener<T> listener) {
        return map.values().stream().anyMatch(le -> le.getListener().equals(listener));
    }

    @Data
    private class ListenerWithExecutor<T> {
        private final Listener<T> listener;
        private final ScheduledExecutorService executor;
    }
}
