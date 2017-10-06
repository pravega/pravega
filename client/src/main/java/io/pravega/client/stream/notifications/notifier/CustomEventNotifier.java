/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications.notifier;

import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.Observable;
import io.pravega.client.stream.notifications.events.CustomEvent;

public class CustomEventNotifier implements Observable<CustomEvent> {

    private final NotificationSystem system = NotificationSystem.INSTANCE;

    @Override
    public void addListener(Listener<CustomEvent> listener) {
        this.system.addListeners(getType(), listener);
    }

    @Override
    public void removeListener(Listener<CustomEvent> listener) {
        this.system.removeListeners(listener);
    }

    @Override
    public void removeListener() {
        this.system.removeListeners(getType());
    }

    @Override
    public Class<CustomEvent> getType() {
        return CustomEvent.class;
    }
}
