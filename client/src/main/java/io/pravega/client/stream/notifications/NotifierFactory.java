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
import io.pravega.client.stream.notifications.notifier.EndOfDataNotifier;
import io.pravega.client.stream.notifications.notifier.SegmentNotifier;
import lombok.Synchronized;

/**
 * Factory used to create different types of notifiers.
 * To add a new notifier add a method which returns a new Notifier object which internally implements
 * {@link io.pravega.client.stream.notifications.Observable}
 */
public class NotifierFactory {

    private final NotificationSystem system;
    private SegmentNotifier segmentNotifier;
    private EndOfDataNotifier endOfDataNotifier;

    public NotifierFactory(final NotificationSystem notificationSystem) {
        this.system = notificationSystem;
    }

    @Synchronized
    public SegmentNotifier getSegmentNotifier(
            final Supplier<StateSynchronizer<ReaderGroupState>> stateSyncronizerSupplier,
            final ScheduledExecutorService executor) {
        if (segmentNotifier == null) {
            segmentNotifier = new SegmentNotifier(this.system, stateSyncronizerSupplier, executor);
        }
        return segmentNotifier;
    }

    @Synchronized
    public EndOfDataNotifier getEndOfDataNotifier(
            final Supplier<StateSynchronizer<ReaderGroupState>> stateSyncronizerSupplier,
            final ScheduledExecutorService executor) {
        if (endOfDataNotifier == null) {
            endOfDataNotifier = new EndOfDataNotifier(this.system, stateSyncronizerSupplier, executor);
        }
        return endOfDataNotifier;
    }

    // multiple such notifiers can be added.
}
