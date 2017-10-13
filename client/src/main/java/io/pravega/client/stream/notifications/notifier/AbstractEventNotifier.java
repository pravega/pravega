/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications.notifier;

import java.util.concurrent.ScheduledExecutorService;

import io.pravega.client.stream.notifications.events.Event;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.Observable;

/**
 * AbstractEventNotifier which is used by all types of Event Notifiers.
 * @param <T>
 */
public abstract class AbstractEventNotifier<T extends Event> implements Observable<T> {

    protected final NotificationSystem notifySystem;

    AbstractEventNotifier(final NotificationSystem notifySystem) {
        this.notifySystem = notifySystem;
    }

    @Override
    public void unregisterListener(final Listener<T> listener) {
        this.notifySystem.removeListener(getType(), listener);
    }

    @Override
    public void unregisterListeners() {
        this.notifySystem.removeListeners(getType());
    }

    @Override
    public void registerListener(final Listener<T> listener, final ScheduledExecutorService executor) {
        this.notifySystem.addListeners(getType(), listener, executor);
    }
}
