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

import io.pravega.client.stream.notifications.notifier.AbstractEventNotifier;

public class CustomEventNotifier extends AbstractEventNotifier<CustomEvent> {

    public CustomEventNotifier(final NotificationSystem system) {
        super(system);
    }

    @Override
    public String getType() {
        return CustomEvent.class.getSimpleName();
    }
}
