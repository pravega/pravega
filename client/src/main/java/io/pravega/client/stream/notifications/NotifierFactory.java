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
import java.util.function.Supplier;

import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.notifier.EndOfDataEventNotifier;
import io.pravega.client.stream.notifications.notifier.SegmentEventNotifier;
import lombok.Synchronized;

/**
 * Factory used to create different types of notifiers.
 * To add a new notifier add a method which returns a new Notifier object which internally implements
 * {@link io.pravega.client.stream.notifications.Observable}
 */
public class NotifierFactory {

    private final NotificationSystem system;
    private SegmentEventNotifier segmentEventNotifier;
    private EndOfDataEventNotifier endOfDataEventNotifier;

    public NotifierFactory(final NotificationSystem notificationSystem) {
        this.system = notificationSystem;
    }

    @Synchronized
    public SegmentEventNotifier getSegmentNotifier(
            final Supplier<StateSynchronizer<ReaderGroupState>> stateSyncronizerSupplier,
            final ScheduledExecutorService executor) {
        if (segmentEventNotifier == null) {
            segmentEventNotifier = new SegmentEventNotifier(this.system, stateSyncronizerSupplier, executor);
        }
        return segmentEventNotifier;
    }

    @Synchronized
    public EndOfDataEventNotifier getEndOfDataNotifier(
            final Supplier<StateSynchronizer<ReaderGroupState>> stateSyncronizerSupplier,
            final ScheduledExecutorService executor) {
        if (endOfDataEventNotifier == null) {
            endOfDataEventNotifier = new EndOfDataEventNotifier(this.system, stateSyncronizerSupplier, executor);
        }
        return endOfDataEventNotifier;
    }

    // multiple such notifiers can be added.
}
