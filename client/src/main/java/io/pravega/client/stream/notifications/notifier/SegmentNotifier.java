/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications.notifier;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.SegmentNotification;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentNotifier extends AbstractPollingNotifier<SegmentNotification> {
    private static final int UPDATE_INTERVAL_SECONDS = Integer.parseInt(
            System.getProperty("pravega.client.segmentNotification.poll.interval.seconds", String.valueOf(120)));
    @GuardedBy("$lock")
    private int numberOfSegments = 0;

    public SegmentNotifier(final NotificationSystem notifySystem,
                           final Supplier<StateSynchronizer<ReaderGroupState>> synchronizerSupplier,
                           final ScheduledExecutorService executor) {
        super(notifySystem, executor, synchronizerSupplier);
    }

    @Override
    @Synchronized
    public void registerListener(final Listener<SegmentNotification> listener) {
        notifySystem.addListeners(getType(), listener, this.executor);
        //periodically fetch the segment count.
        startPolling(this::checkAndTriggerSegmentNotification, UPDATE_INTERVAL_SECONDS);
    }

    @Override
    public String getType() {
        return SegmentNotification.class.getSimpleName();
    }

    private void checkAndTriggerSegmentNotification() {
        this.synchronizer.fetchUpdates();
        ReaderGroupState state = this.synchronizer.getState();
        int newNumberOfSegments = state.getNumberOfSegments();
        checkState(newNumberOfSegments > 0, "Number of segments cannot be zero");

        if (this.numberOfSegments == 0) { //initialize the number of segments.
            this.numberOfSegments = newNumberOfSegments;
        } else if (this.numberOfSegments != newNumberOfSegments) { // SegmentNotification has happened.
            this.numberOfSegments = newNumberOfSegments;
            SegmentNotification notification = SegmentNotification.builder().numOfSegments(state.getNumberOfSegments())
                                                           .numOfReaders(state.getOnlineReaders().size())
                                                           .build();
            notifySystem.notify(notification);
        }
    }
}
