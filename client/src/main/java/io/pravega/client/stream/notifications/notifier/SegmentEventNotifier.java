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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.events.SegmentEvent;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentEventNotifier extends AbstractEventNotifier<SegmentEvent> {

    private static final int UPDATE_INTERVAL_SECONDS = Integer.parseInt(
            System.getProperty("pravega.client.segmentEvent.poll.interval.seconds", String.valueOf(120)));
    private final Supplier<StateSynchronizer<ReaderGroupState>> synchronizerSupplier;
    private final AtomicBoolean pollingStarted = new AtomicBoolean();
    @GuardedBy("$lock")
    private ScheduledFuture<?> future;
    @GuardedBy("$lock")
    private StateSynchronizer<ReaderGroupState> synchronizer;
    @GuardedBy("$lock")
    private int numberOfSegments = 0;

    public SegmentEventNotifier(final NotificationSystem notifySystem,
                                final Supplier<StateSynchronizer<ReaderGroupState>> synchronizerSupplier,
                                final ScheduledExecutorService executor) {
        super(notifySystem, executor);
        this.synchronizerSupplier = synchronizerSupplier;
    }

    @Override
    @Synchronized
    public void registerListener(final Listener<SegmentEvent> listener) {
        notifySystem.addListeners(getType(), listener, this.executor);
        //periodically fetch the segment count.
        if (!pollingStarted.getAndSet(true)) { //schedule the  only once
            synchronizer = synchronizerSupplier.get();
            future = executor.scheduleAtFixedRate(this::checkAndTriggerSegmentNotification, 0,
                    UPDATE_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Override
    @Synchronized
    public void unregisterListener(final Listener<SegmentEvent> listener) {
        notifySystem.removeListener(getType(), listener);
        if (!notifySystem.isListenerPresent(getType())) {
            cancelScheduledTask();
            synchronizer.close();
        }
    }

    @Override
    @Synchronized
    public void unregisterAllListeners() {
        this.notifySystem.removeListeners(getType());
        cancelScheduledTask();
        synchronizer.close();
    }

    @Override
    public String getType() {
        return SegmentEvent.class.getSimpleName();
    }

    private void checkAndTriggerSegmentNotification() {
        this.synchronizer.fetchUpdates();
        ReaderGroupState state = this.synchronizer.getState();
        int newNumberOfSegments = state.getNumberOfSegments();
        checkState(newNumberOfSegments > 0, "Number of segments cannot be zero");

        if (this.numberOfSegments == 0) { //initialize the number of segments.
            this.numberOfSegments = newNumberOfSegments;
        } else if (this.numberOfSegments != newNumberOfSegments) { // SegmentEvent has happened.
            this.numberOfSegments = newNumberOfSegments;
            SegmentEvent event = SegmentEvent.builder().numOfSegments(state.getNumberOfSegments())
                                             .numOfReaders(state.getOnlineReaders().size())
                                             .build();
            notifySystem.notify(event);
        }
    }

    @GuardedBy("$lock")
    private void cancelScheduledTask() {
        log.debug("Cancel the scheduled task to check for SegmentEvent");
        if (future != null) {
            future.cancel(true);
        }
        pollingStarted.set(false);
    }
}
