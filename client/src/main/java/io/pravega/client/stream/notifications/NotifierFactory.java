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

import java.util.function.Supplier;

import io.pravega.client.stream.notifications.events.ScaleEvent;
import io.pravega.client.stream.notifications.notifier.CustomEventNotifier;
import io.pravega.client.stream.notifications.notifier.ScaleEventNotifier;
import lombok.Synchronized;

/**
 * Factory used to create different types of notifiers.
 */
public class NotifierFactory {

    private final CustomEventNotifier customEventNotifier;
    private final NotificationSystem system;
    private ScaleEventNotifier scaleEventNotifier;

    NotifierFactory(final NotificationSystem notificationSystem) {
        this.system = notificationSystem;
        this.customEventNotifier = new CustomEventNotifier(this.system);
    }

    @Synchronized
    public ScaleEventNotifier getScaleNotifier(final Supplier<ScaleEvent> scaleEventSupplier) {
        if (scaleEventNotifier == null) {
            scaleEventNotifier = new ScaleEventNotifier(this.system, scaleEventSupplier);
        }
        return scaleEventNotifier;
    }

    public CustomEventNotifier getCustomNotifier() {
        return customEventNotifier;
    }

    // multiple such notifiers can be added.
}
