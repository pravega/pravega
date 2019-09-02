/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications.notifier;

import java.util.concurrent.ScheduledExecutorService;

import io.pravega.client.stream.notifications.Notification;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.Observable;

/**
 * AbstractNotifier which is used by all types of Notifiers.
 * @param <T> notification subtype
 */
public abstract class AbstractNotifier<T extends Notification> implements Observable<T> {

    protected final NotificationSystem notifySystem;
    protected final ScheduledExecutorService executor;

    protected AbstractNotifier(final NotificationSystem notifySystem, final ScheduledExecutorService executor) {
        this.notifySystem = notifySystem;
        this.executor = executor;
    }

    @Override
    public void unregisterListener(final Listener<T> listener) {
        this.notifySystem.removeListener(getType(), listener);
    }

    @Override
    public void unregisterAllListeners() {
        this.notifySystem.removeListeners(getType());
    }

    @Override
    public void registerListener(final Listener<T> listener) {
        this.notifySystem.addListeners(getType(), listener, this.executor);
    }
}
