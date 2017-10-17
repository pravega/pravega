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

import io.pravega.client.stream.notifications.events.SegmentEvent;

/**
 * ReaderGroup event listener interface. This has the list of events supported by ReaderGroup.
 */
public interface ReaderGroupEventListener {

    /**
     * Get a segment event notifier for a given reader group.
     * <br>
     * A segment event notifier is triggered when the total number of segments managed by the ReaderGroup changes.
     * During a scale operation segments can be split into multiple or merge into some other segment causing the total
     * number of segments to change. The total number of segments can also change when configuration of the
     * reader group is changed, for example modify the configuration of a reader group to add/remove a stream.
     * <P>
     * Note:
     * <br>
     * * In case of a seal stream operation the segments are sealed and the segment has no successors. In this case
     * the notifier is not triggered.
     *
     * @param executor executor on which the listeners run.
     * @return Observable of type SegmentEvent.
     *
     */
    Observable<SegmentEvent> getSegmentEventNotifier(final ScheduledExecutorService executor);
}
