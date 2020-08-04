/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

import java.util.concurrent.ScheduledExecutorService;

import io.pravega.client.stream.notifications.notifier.AbstractNotifier;

public class CustomNotifier extends AbstractNotifier<CustomNotification> {

    public CustomNotifier(final NotificationSystem system, final ScheduledExecutorService executor) {
        super(system, executor);
    }

    @Override
    public String getType() {
        return CustomNotification.class.getSimpleName();
    }
}
