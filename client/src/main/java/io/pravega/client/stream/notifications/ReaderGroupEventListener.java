/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

import java.util.concurrent.ScheduledExecutorService;

import io.pravega.client.stream.notifications.events.ScaleEvent;

/**
 * ReaderGroup event listener interface. This has the list of events supported by ReaderGroup.
 */
public interface ReaderGroupEventListener {

    /**
     * Get a scale event notifier for a given readergroup. A scale event notifier is triggered when there are more
     * number of segments when compared to the number of readers or vice versa. Listeners can be registered which will
     * notify in case of a scale event.
     *
     * @param executor executor on which the listeners run.
     * @return Observable of type ScaleEvent.
     *
     */
    Observable<ScaleEvent> getScaleEventNotifier(final ScheduledExecutorService executor);
}
