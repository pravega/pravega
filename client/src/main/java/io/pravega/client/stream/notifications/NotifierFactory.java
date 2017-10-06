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

import io.pravega.client.stream.notifications.notifier.CustomEventNotifier;
import io.pravega.client.stream.notifications.notifier.ScaleEventNotifier;

public class NotifierFactory {

    private final NotificationSystem system;

    public NotifierFactory(NotificationSystem notificationSystem) {
        this.system = notificationSystem;
    }

    public ScaleEventNotifier getScaleNotifier() {
        return new ScaleEventNotifier(this.system);
    }

    public CustomEventNotifier getCustomNotifier() {
        return new CustomEventNotifier(this.system);
    }

    // multiple such notifiers can be added.
}
