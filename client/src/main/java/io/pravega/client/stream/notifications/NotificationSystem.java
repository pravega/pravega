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

public class NotificationSystem {

    private final List<ListenerWithType<Event>> listeners = new CopyOnWriteArrayList<>();
    private final NotifierFactory factory = new NotifierFactory(this);

    @SuppressWarnings("unchecked")
    public <T extends Event> void addListeners(final Class<T> type, final Listener<T> listener) {
        listeners.add(new ListenerWithType(type, listener));
    }
   
    /**
     * This method will ensure the event is notified to the listeners of the same type.
     * @param event Event to be notified.
     * @param <T> All class which extends Event.
     */
    public <T extends Event> void notify(final T event) {
        listeners.stream()
                 .filter(listener -> listener.getType().equals(event.getClass()))
                 .forEach(l -> l.getListener().onEvent(event));
    }

    public <T extends Event> void removeListeners(final Listener<T> listener) {
        listeners.removeIf(e -> e.getListener().equals(listener));
    }

    public <T extends Event> void removeListeners(Class<T> type) {
        listeners.removeIf(e -> e.getType().equals(type));
    }

    public NotifierFactory getNotifierFactory() {
        return factory;
    }

    @Data
    private class ListenerWithType<T> {
        private final Class<T> type;
        private final Listener<T> listener;
    }
}
