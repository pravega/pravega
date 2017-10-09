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

import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.Observable;
import io.pravega.client.stream.notifications.events.ScaleEvent;

public class ScaleEventNotifier implements Observable<ScaleEvent> {

    public final NotificationSystem system;

    public ScaleEventNotifier(final NotificationSystem system) {
        this.system = system;
    }

    @Override
    public void addListener(final Listener<ScaleEvent> listener, final ScheduledExecutorService executor) {
        system.addListeners(getType(), listener, executor);
    }

    @Override
    public void removeListener(final Listener<ScaleEvent> listener) {
        system.removeListeners(listener);
    }

    @Override
    public void removeListener() {
        this.system.removeListeners(getType());
    }

    @Override
    public Class<ScaleEvent> getType() {
        return ScaleEvent.class;
    }
}
