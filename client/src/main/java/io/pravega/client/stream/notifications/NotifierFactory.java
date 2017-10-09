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

    private final ScaleEventNotifier scaleEventNotifier;
    private final CustomEventNotifier customEventNotifier;

    NotifierFactory(final NotificationSystem notificationSystem) {
        this.scaleEventNotifier = new ScaleEventNotifier(notificationSystem);
        this.customEventNotifier = new CustomEventNotifier(notificationSystem);
    }

    public ScaleEventNotifier getScaleNotifier() {
        return scaleEventNotifier;
    }

    public CustomEventNotifier getCustomNotifier() {
        return customEventNotifier;
    }

    // multiple such notifiers can be added.
}
