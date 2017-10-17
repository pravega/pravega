/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import io.pravega.client.stream.notifications.events.Event;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import javax.annotation.concurrent.GuardedBy;
import lombok.Data;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NotificationSystem {
    @GuardedBy("$lock")
    private final Multimap<String, ListenerWithExecutor<Event>> map = ArrayListMultimap.create();

    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends Event> void addListeners(final String type,
                                               final Listener<T> listener,
                                               final ScheduledExecutorService executor) {
        if (!isListenerPresent(listener)) {
            map.put(type, new ListenerWithExecutor(listener, executor));
        }
    }

    /**
     * This method will ensure the event is notified to the listeners of the same type.
     *
     * @param event Event to be notified.
     * @param <T>   Type of Event.
     */
    @Synchronized
    public <T extends Event> void notify(final T event) {
        String type = event.getClass().getSimpleName();
        map.get(type).forEach(l -> {
            log.info("Executing listener of type: {} for event: {}", type, event);
            ExecutorServiceHelpers.execute(() -> l.getListener().onEvent(event),
                    throwable -> log.error("Exception while executing listener for event: {}", event),
                    () -> log.info("Completed execution of notify for event :{}", event),
                    l.getExecutor());
        });
    }

    /**
     * Remove Listener of a given event type.
     *
     * @param <T>      Type of event.
     * @param type     Type of event listener.
     * @param listener Listener to be removed.
     */
    @Synchronized
    public <T extends Event> void removeListener(final String type, final Listener<T> listener) {
        map.get(type).removeIf(e -> e.getListener().equals(listener));
    }

    /**
     * Remove all listeners of an event type.
     *
     * @param type Type of event listener.
     */
    @Synchronized
    public void removeListeners(final String type) {
        map.removeAll(type);
    }

    /**
     * Check if a Listener is present for a given event type.
     *
     * @param type Type of event listener.
     * @return true if Listener is present.
     */
    @Synchronized
    public boolean isListenerPresent(final String type) {
        return !map.get(type).isEmpty();
    }

    private <T extends Event> boolean isListenerPresent(final Listener<T> listener) {
        return map.values().stream().anyMatch(le -> le.getListener().equals(listener));
    }

    @Data
    private class ListenerWithExecutor<T> {
        private final Listener<T> listener;
        private final ScheduledExecutorService executor;
    }
}
