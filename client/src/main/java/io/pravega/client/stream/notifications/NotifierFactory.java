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
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;

/**
 * Factory used to create different types of notifiers.
 * To add a new notifier add a method which returns a new Notifier object which internally implements
 * {@link io.pravega.client.stream.notifications.Observable}
 */
public class NotifierFactory {

    private final NotificationSystem system;
    private final Supplier<StateSynchronizer<ReaderGroupState>> stateSynchronizerSupplier;
    @GuardedBy("$lock")
    private StateSynchronizer<ReaderGroupState> synchronizer;
    @GuardedBy("$lock")
    private SegmentNotifier segmentNotifier;
    @GuardedBy("$lock")
    private EndOfDataNotifier endOfDataNotifier;

    public NotifierFactory(final NotificationSystem notificationSystem,
                           final Supplier<StateSynchronizer<ReaderGroupState>> stateSynchronizerSupplier) {
        this.system = notificationSystem;
        this.stateSynchronizerSupplier = stateSynchronizerSupplier;
    }

    @Synchronized
    public SegmentNotifier getSegmentNotifier(final ScheduledExecutorService executor) {
        createSynchronizer();
        if (segmentNotifier == null) {
            segmentNotifier = new SegmentNotifier(this.system, this.synchronizer, executor);
        }
        return segmentNotifier;
    }

    @Synchronized
    public EndOfDataNotifier getEndOfDataNotifier(final ScheduledExecutorService executor) {
        createSynchronizer();
        if (endOfDataNotifier == null) {
            endOfDataNotifier = new EndOfDataNotifier(this.system, this.synchronizer, executor);
        }
        return endOfDataNotifier;
    }

    /*
        Method to lazily create a synchronizer object. It
            - creates a synchronizer object only when a notifier is invoked.
            - ensures a common synchronizer object can be used across notifiers.
     */
    private void createSynchronizer() {
        if (synchronizer == null) {
            synchronizer = this.stateSynchronizerSupplier.get();
        }
    }

    // multiple such notifiers can be added.
}
