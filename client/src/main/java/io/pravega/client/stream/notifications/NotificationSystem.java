/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import lombok.Data;

public enum NotificationSystem {
    INSTANCE;

    private final List<ListenerWithType<Event>> listeners = new CopyOnWriteArrayList<>();

    public <T extends Event> void addListeners(final Class<T> type, final Listener<T> listener) {
        listeners.add(new ListenerWithType(listener, type));
    }

    public void removeListeners(final Listener listener) {
        listeners.remove(listener);
    }

    public <T extends Event> void notify(final T event) {
        listeners.stream()
                 .filter(listener -> listener.getType().equals(event.getClass()))
                 .forEach(listenerWithType -> {
                     listenerWithType.getListener().onEvent(event);
                 });
    }

    @Data
    private class ListenerWithType<T> {
        private final Listener<T> listener;
        private final Class<T> type;
    }
}
